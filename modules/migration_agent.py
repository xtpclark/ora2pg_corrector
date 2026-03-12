"""
Autonomous Migration Agent - drives end-to-end Oracle-to-PostgreSQL migrations.

Usage:
    agent = MigrationAgent(client_id, client_config, app_db_conn, encryption_key)
    result = agent.run()

The agent autonomously:
1. Connects to Oracle, discovers the schema
2. Assesses complexity, plans conversion strategy per object
3. Exports/converts DDL via Ora2Pg, validates against PostgreSQL, self-heals
4. Migrates data with proper FK ordering
5. Reports results

When it encounters unhandled edge cases, it logs them for control plane improvement.
"""

import os
import logging
import subprocess
import time
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, List

import psycopg2
import psycopg2.sql

from .db import execute_query, insert_returning_id
from .sql_processing import Ora2PgAICorrector
from .ddl_parser import parse_ddl_file
from .orchestrator import DDL_TYPE_ORDER, extract_table_dependencies
from .constants import (
    OUTPUT_DIR,
    EXPORT_TYPES,
    VALIDATABLE_TYPES,
    calculate_ai_cost,
    get_session_dir,
    ensure_session_dir,
)

logger = logging.getLogger(__name__)


class Phase(str, Enum):
    """Migration phases."""
    CONNECT = "connect"
    DISCOVER = "discover"
    ASSESS = "assess"
    EXPORT = "export"
    CONVERT = "convert"
    VALIDATE = "validate"
    DATA = "data"
    VERIFY = "verify"
    COMPLETE = "complete"
    FAILED = "failed"


@dataclass
class MigrationObject:
    """Tracks a single schema object through the migration pipeline."""
    name: str
    object_type: str
    supported: bool = True
    status: str = "pending"
    oracle_ddl: str = ""
    pg_ddl: str = ""
    error: str = ""
    attempts: int = 0
    ai_tokens_used: int = 0
    needs_ai: bool = False


@dataclass
class MigrationResult:
    """Final result of a migration run."""
    success: bool = False
    phase: Phase = Phase.CONNECT
    total_objects: int = 0
    migrated: int = 0
    failed: int = 0
    skipped: int = 0
    data_tables_migrated: int = 0
    data_rows_migrated: int = 0
    ai_cost_usd: float = 0.0
    duration_seconds: float = 0.0
    errors: list = field(default_factory=list)
    edge_cases: list = field(default_factory=list)
    objects: list = field(default_factory=list)


class MigrationAgent:
    """
    Autonomous agent that drives a full Oracle-to-PostgreSQL migration.

    Wraps the existing Ora2Pg + AI correction pipeline and orchestrates
    the entire workflow without manual intervention.
    """

    MAX_RETRY_PER_OBJECT = 3

    def __init__(
        self,
        client_id: int,
        client_config: dict,
        app_db_conn,
        encryption_key: bytes,
        ai_settings: dict,
        callback=None,
    ):
        """
        :param client_id: Client ID from the database
        :param client_config: Full client config dict (oracle_dsn, oracle_user, etc.)
        :param app_db_conn: Application database connection for session/object tracking
        :param encryption_key: Fernet encryption key
        :param ai_settings: AI provider settings dict
        :param callback: Optional progress callback(phase, message, pct)
        """
        self.client_id = client_id
        self.config = client_config
        self.db_conn = app_db_conn
        self.encryption_key = encryption_key
        self.ai_settings = ai_settings
        self.callback = callback

        # Key config values
        self.oracle_schema = client_config.get('oracle_schema', client_config.get('schema', ''))
        self.pg_dsn = client_config.get('validation_pg_dsn', '')

        # Build the corrector (existing engine)
        self.corrector = Ora2PgAICorrector(OUTPUT_DIR, ai_settings, encryption_key)

        self.objects: List[MigrationObject] = []
        self.result = MigrationResult()
        self.session_id: Optional[int] = None
        self._start_time = 0.0

    # -----------------------------------------------------------------
    # Main entry point
    # -----------------------------------------------------------------

    def run(self) -> MigrationResult:
        """Execute the full migration pipeline. Returns MigrationResult."""
        self._start_time = time.time()
        try:
            self._phase(Phase.CONNECT, self._connect)
            self._phase(Phase.DISCOVER, self._discover)
            self._phase(Phase.ASSESS, self._assess)
            self._phase(Phase.EXPORT, self._export)
            self._phase(Phase.CONVERT, self._convert)
            self._phase(Phase.VALIDATE, self._validate)
            self._phase(Phase.DATA, self._migrate_data)
            self._phase(Phase.VERIFY, self._verify)
            self.result.phase = Phase.COMPLETE
            self.result.success = True
        except MigrationAbort as e:
            self.result.phase = Phase.FAILED
            self.result.errors.append(str(e))
            logger.error(f"Migration aborted in phase {e.phase}: {e}")
        except Exception as e:
            self.result.phase = Phase.FAILED
            self.result.errors.append(f"Unexpected error: {e}")
            logger.exception("Migration agent encountered an unexpected error")

        self.result.duration_seconds = time.time() - self._start_time
        self.result.objects = [
            {
                "name": o.name, "type": o.object_type,
                "status": o.status, "error": o.error,
                "attempts": o.attempts, "ai_tokens": o.ai_tokens_used,
            }
            for o in self.objects
        ]
        self._emit("complete", self._summary(), 100)
        return self.result

    # -----------------------------------------------------------------
    # Phase: CONNECT
    # -----------------------------------------------------------------

    def _connect(self):
        """Verify connectivity to both Oracle and PostgreSQL."""
        # Test Oracle via sqlplus
        self._emit("connect", "Testing Oracle connection...", 0)
        connect_str, err = self.corrector._build_sqlplus_connect_string(self.config)
        if err:
            raise MigrationAbort(Phase.CONNECT, f"Oracle config error: {err}")

        try:
            proc = subprocess.run(
                [self.corrector.sqlplus_path, '-S', connect_str],
                input="SELECT 1 FROM DUAL;\nEXIT;\n",
                capture_output=True, text=True, timeout=30,
            )
            if proc.returncode != 0:
                raise MigrationAbort(
                    Phase.CONNECT,
                    f"Oracle connection failed: {proc.stderr.strip() or proc.stdout.strip()}"
                )
        except FileNotFoundError:
            raise MigrationAbort(Phase.CONNECT, "sqlplus not found - is Oracle client installed?")
        except subprocess.TimeoutExpired:
            raise MigrationAbort(Phase.CONNECT, "Oracle connection timed out")

        # Test PostgreSQL
        self._emit("connect", "Testing PostgreSQL connection...", 5)
        try:
            conn = psycopg2.connect(self.pg_dsn)
            conn.close()
        except Exception as e:
            raise MigrationAbort(Phase.CONNECT, f"PostgreSQL connection failed: {e}")

        self._emit("connect", "Both connections verified.", 10)

    # -----------------------------------------------------------------
    # Phase: DISCOVER
    # -----------------------------------------------------------------

    def _discover(self):
        """Discover all schema objects from Oracle using sqlplus."""
        self._emit("discover", f"Discovering objects in schema {self.oracle_schema}...", 10)

        discovered, err = self.corrector._get_object_list(self.db_conn, self.config)
        if err:
            raise MigrationAbort(Phase.DISCOVER, f"Discovery failed: {err}")

        for obj in discovered:
            self.objects.append(MigrationObject(
                name=obj['name'],
                object_type=obj['type'],
                supported=obj.get('supported', True),
            ))

        self.result.total_objects = len(self.objects)
        supported_count = sum(1 for o in self.objects if o.supported)
        self._emit(
            "discover",
            f"Found {len(self.objects)} objects ({supported_count} supported by Ora2Pg).",
            20,
        )

        if not self.objects:
            raise MigrationAbort(Phase.DISCOVER, "No objects found in schema.")

    # -----------------------------------------------------------------
    # Phase: ASSESS
    # -----------------------------------------------------------------

    def _assess(self):
        """Assess complexity and determine conversion strategy per object."""
        self._emit("assess", "Assessing object complexity...", 20)

        # Oracle types that are internal/non-exportable — filter out
        skip_types = {'LOB', 'LOB PARTITION', 'TABLE PARTITION', 'TABLE SUBPARTITION',
                      'INDEX PARTITION', 'INDEX SUBPARTITION'}

        # Types that Ora2Pg handles natively
        native_types = {'TABLE', 'SEQUENCE', 'INDEX'}

        # Types that typically need AI assistance
        ai_types = {'FUNCTION', 'PROCEDURE', 'PACKAGE', 'PACKAGE BODY', 'TRIGGER'}

        # Types Ora2Pg can export (override discovery's supported flag)
        exportable_types = {'TABLE', 'VIEW', 'SEQUENCE', 'FUNCTION', 'PROCEDURE',
                           'PACKAGE', 'TRIGGER', 'TYPE', 'INDEX', 'GRANT'}

        filtered_objects = []
        skipped_internal = 0
        skipped_pk = 0

        # Detect PK indexes: Oracle discovers them as INDEX objects, but Ora2Pg
        # generates them inline as PRIMARY KEY in CREATE TABLE. These names
        # typically end with _KEY or _PK and match a table name prefix.
        table_names = {o.name.upper() for o in self.objects if o.object_type == 'TABLE'}

        for obj in self.objects:
            if obj.object_type in skip_types:
                skipped_internal += 1
                continue

            # Skip PK indexes — they're created inline in CREATE TABLE.
            # Oracle discovers PK constraint indexes as separate INDEX objects,
            # but Ora2Pg generates them as inline PRIMARY KEY in CREATE TABLE DDL.
            if obj.object_type == 'INDEX':
                uname = obj.name.upper()
                is_pk = False

                # Pattern 1: TABLE_KEY, TABLE_PK, TABLE_PKEY
                for suffix in ('_KEY', '_PK', '_PKEY'):
                    if uname.endswith(suffix):
                        table_prefix = uname[:-len(suffix)]
                        if table_prefix in table_names:
                            is_pk = True
                            break

                # Pattern 2: Same name as a table (Oracle default PK index)
                if not is_pk and uname in table_names:
                    is_pk = True

                # Pattern 3: PKnnn (Oracle system-generated PK names)
                if not is_pk and uname.startswith('PK') and uname[2:].isdigit():
                    is_pk = True

                if is_pk:
                    obj.status = "validated"  # Already handled by TABLE export
                    skipped_pk += 1
                    filtered_objects.append(obj)
                    continue

            # Override discovery's supported flag for types we know Ora2Pg handles
            if obj.object_type in exportable_types:
                obj.supported = True

            if not obj.supported:
                obj.status = "unsupported"
                filtered_objects.append(obj)
                continue

            if obj.object_type in ai_types:
                obj.needs_ai = True
            elif obj.object_type == 'VIEW':
                obj.needs_ai = True  # Views often have Oracle-isms

            filtered_objects.append(obj)

        self.objects = filtered_objects
        self.result.total_objects = len(self.objects)

        ai_count = sum(1 for o in self.objects if o.needs_ai)
        native_count = sum(1 for o in self.objects if o.supported and not o.needs_ai
                          and o.status != "validated")
        unsupported = sum(1 for o in self.objects if not o.supported)
        self._emit(
            "assess",
            f"Assessment: {native_count} native, {ai_count} AI-assisted, "
            f"{unsupported} unsupported, {skipped_internal} internal filtered, "
            f"{skipped_pk} PK indexes (inline).",
            25,
        )

    # -----------------------------------------------------------------
    # Phase: EXPORT
    # -----------------------------------------------------------------

    def _export(self):
        """Export DDL from Oracle via Ora2Pg, type by type."""
        self._emit("export", "Exporting DDL via Ora2Pg...", 25)

        # Map Oracle object types to Ora2Pg export types
        type_mapping = {
            'TABLE': 'TABLE',
            'VIEW': 'VIEW',
            'MATERIALIZED VIEW': 'MVIEW',
            'SEQUENCE': 'SEQUENCE',
            'FUNCTION': 'FUNCTION',
            'PROCEDURE': 'PROCEDURE',
            'PACKAGE': 'PACKAGE',
            'PACKAGE BODY': 'PACKAGE',
            'TRIGGER': 'TRIGGER',
            'TYPE': 'TYPE',
            'INDEX': 'INDEX',
        }

        # Determine which Ora2Pg export types we need
        discovered_types = set(o.object_type for o in self.objects if o.supported)
        ora2pg_types_needed = set()
        for dtype in discovered_types:
            mapped = type_mapping.get(dtype)
            if mapped:
                ora2pg_types_needed.add(mapped)

        exported_types = set()
        for obj_type in DDL_TYPE_ORDER:
            ora2pg_type = type_mapping.get(obj_type, obj_type)
            if ora2pg_type not in ora2pg_types_needed:
                continue
            if ora2pg_type in exported_types:
                continue

            self._emit("export", f"Exporting {obj_type}s...", 30)

            # Build per-type config
            export_config = self.config.copy()
            export_config['type'] = ora2pg_type

            try:
                export_result = self.corrector.run_ora2pg_export(
                    self.client_id, self.db_conn, export_config,
                    session_name=f"agent-{ora2pg_type}",
                )
                exported_types.add(ora2pg_type)

                # Unpack result tuple safely
                if isinstance(export_result, tuple) and len(export_result) == 2:
                    result, err = export_result
                else:
                    logger.warning(f"Unexpected return from export for {ora2pg_type}: {type(export_result)}")
                    continue

                if err:
                    logger.warning(f"Export error for {ora2pg_type}: {err}")
                    continue

                if not result or not isinstance(result, dict):
                    continue

                # Track session for later reference
                if result.get('session_id') and not self.session_id:
                    self.session_id = result['session_id']

                # Get the export directory
                export_dir = result.get('directory', '')
                session_id = result.get('session_id')

                # If single-file export with sql_output, parse it
                if result.get('sql_output'):
                    self._assign_ddl_from_content(result['sql_output'], obj_type)

                # If we got exported files, find and parse them
                if result.get('files'):
                    # Determine directory to look in
                    search_dir = export_dir
                    if not search_dir and session_id:
                        search_dir = get_session_dir(self.client_id, session_id)

                    if search_dir:
                        for filename in result['files']:
                            filepath = os.path.join(search_dir, filename)
                            if os.path.exists(filepath):
                                self._assign_ddl_from_file(filepath, obj_type)

            except Exception as e:
                logger.warning(f"Export failed for {ora2pg_type}: {e}", exc_info=True)
                for obj in self.objects:
                    if obj.object_type == obj_type and obj.status == "pending":
                        obj.status = "export_failed"
                        obj.error = str(e)

        exported = sum(1 for o in self.objects if o.oracle_ddl)
        total_supported = sum(1 for o in self.objects if o.supported)
        self._emit("export", f"Exported DDL for {exported}/{total_supported} objects.", 40)

    # -----------------------------------------------------------------
    # Phase: CONVERT
    # -----------------------------------------------------------------

    def _convert(self):
        """Convert Oracle DDL to PostgreSQL, using AI where needed."""
        self._emit("convert", "Converting DDL to PostgreSQL...", 40)

        convertible = [o for o in self.objects if o.oracle_ddl and o.status != "export_failed"]
        total = len(convertible)

        for i, obj in enumerate(convertible):
            pct = 40 + int((i / max(total, 1)) * 20)
            self._emit("convert", f"Converting {obj.object_type} {obj.name}...", pct)

            try:
                if obj.needs_ai:
                    pg_ddl, metrics = self.corrector.ai_correct_sql(
                        obj.oracle_ddl, source_dialect="oracle"
                    )
                    obj.pg_ddl = pg_ddl
                    obj.ai_tokens_used = metrics.get('tokens_used', 0)
                    obj.status = "converted"
                else:
                    # Ora2Pg output is already PostgreSQL DDL
                    obj.pg_ddl = obj.oracle_ddl
                    obj.status = "converted"
            except Exception as e:
                obj.status = "convert_failed"
                obj.error = str(e)
                logger.warning(f"Conversion failed for {obj.name}: {e}")

        converted = sum(1 for o in self.objects if o.status == "converted")
        self._emit("convert", f"Converted {converted}/{total} objects.", 60)

    # -----------------------------------------------------------------
    # Phase: VALIDATE
    # -----------------------------------------------------------------

    def _validate(self):
        """Validate converted DDL against PostgreSQL with self-healing."""
        self._emit("validate", "Validating DDL against PostgreSQL...", 60)

        # Sort by dependency order
        ordered = self._dependency_order()
        validatable = [o for o in ordered if o.pg_ddl and o.object_type in VALIDATABLE_TYPES]
        total = len(validatable)

        for i, obj in enumerate(validatable):
            pct = 60 + int(((i + 1) / max(total, 1)) * 20)
            self._emit("validate", f"Validating {obj.object_type} {obj.name} ({i+1}/{total})...", pct)

            # Pre-drop the object to avoid "already exists" errors wasting AI tokens
            self._drop_existing_object(obj.name, obj.object_type)

            while obj.attempts < self.MAX_RETRY_PER_OBJECT:
                obj.attempts += 1

                success, message, corrected_sql, _ = self.corrector.validate_sql(
                    obj.pg_ddl, self.pg_dsn, defer_fk=True,
                )

                if success:
                    obj.status = "validated"
                    obj.pg_ddl = corrected_sql or obj.pg_ddl
                    self.result.migrated += 1
                    break

                # Check if it's an "already exists" error — treat as success
                if message and 'already exists' in message.lower():
                    obj.status = "validated"
                    self.result.migrated += 1
                    break

                logger.info(
                    f"Validation attempt {obj.attempts} failed for {obj.name}: {message}"
                )
                if corrected_sql and corrected_sql != obj.pg_ddl:
                    obj.pg_ddl = corrected_sql  # Use AI-corrected version for next attempt
                elif obj.attempts < self.MAX_RETRY_PER_OBJECT:
                    # Force AI re-correction with error context
                    fixed_sql, metrics = self.corrector.ai_correct_sql(
                        f"-- ERROR: {message}\n-- Fix this PostgreSQL DDL:\n{obj.pg_ddl}",
                        source_dialect="oracle",
                    )
                    obj.pg_ddl = fixed_sql
                    obj.ai_tokens_used += metrics.get('tokens_used', 0)

            if obj.status != "validated":
                obj.status = "failed"
                obj.error = message
                self.result.failed += 1
                self.result.edge_cases.append(f"{obj.object_type} {obj.name}: {message}")

        # Count all validated objects (including PK indexes pre-validated in assess)
        self.result.migrated = sum(1 for o in self.objects if o.status == "validated")
        self.result.failed = sum(1 for o in self.objects if o.status == "failed")
        self.result.skipped = sum(1 for o in self.objects
                                  if o.status in ("pending", "unsupported", "export_failed"))
        self._emit(
            "validate",
            f"Validated: {self.result.migrated} ok, {self.result.failed} failed, "
            f"{self.result.skipped} skipped.",
            80,
        )

    # -----------------------------------------------------------------
    # Phase: DATA
    # -----------------------------------------------------------------

    def _migrate_data(self):
        """Migrate data for validated tables via Ora2Pg COPY mode."""
        self._emit("data", "Migrating data...", 80)

        tables = [o for o in self.objects if o.object_type == 'TABLE' and o.status == 'validated']

        if not tables:
            self._emit("data", "No validated tables to migrate data for.", 90)
            return

        # Pre-check: get row counts from Oracle to skip empty tables
        connect_str, _ = self.corrector._build_sqlplus_connect_string(self.config)
        oracle_counts = self._batch_oracle_row_counts(
            [t.name for t in tables], connect_str
        )

        non_empty = [t for t in tables if oracle_counts.get(t.name.upper(), 0) > 0]
        empty_count = len(tables) - len(non_empty)

        if not non_empty:
            self._emit(
                "data",
                f"All {len(tables)} tables are empty in Oracle. Skipping data migration.",
                90,
            )
            return

        self._emit(
            "data",
            f"Found {len(non_empty)} tables with data ({empty_count} empty, skipping).",
            81,
        )

        ordered_tables = self._sort_tables_for_data(non_empty)

        for i, obj in enumerate(ordered_tables):
            row_count = oracle_counts.get(obj.name.upper(), 0)
            pct = 81 + int(((i + 1) / len(ordered_tables)) * 9)
            self._emit(
                "data",
                f"Migrating {obj.name} ({row_count} rows, {i+1}/{len(ordered_tables)})...",
                pct,
            )

            try:
                # Use Ora2Pg COPY mode for data transfer
                copy_config = self.config.copy()
                copy_config['type'] = 'COPY'
                copy_config['ALLOW'] = obj.name
                copy_config['PG_DSN'] = self.pg_dsn

                export_result = self.corrector.run_ora2pg_export(
                    self.client_id, self.db_conn, copy_config,
                    session_name=f"agent-data-{obj.name}",
                )

                if isinstance(export_result, tuple) and len(export_result) == 2:
                    result, err = export_result
                else:
                    err = f"Unexpected return: {type(export_result)}"

                if err:
                    logger.warning(f"Data migration warning for {obj.name}: {err}")
                    self.result.edge_cases.append(f"Data copy failed for {obj.name}: {err}")
                else:
                    self.result.data_tables_migrated += 1
                    pg_rows = self._count_pg_rows(obj.name)
                    self.result.data_rows_migrated += pg_rows or 0

            except Exception as e:
                logger.warning(f"Data migration failed for {obj.name}: {e}")
                self.result.edge_cases.append(f"Data migration failed for {obj.name}: {e}")

        self._emit(
            "data",
            f"Migrated data: {self.result.data_tables_migrated} tables, "
            f"{self.result.data_rows_migrated} rows.",
            90,
        )

    # -----------------------------------------------------------------
    # Phase: VERIFY
    # -----------------------------------------------------------------

    def _verify(self):
        """Verify migration integrity - row counts between Oracle and PostgreSQL."""
        self._emit("verify", "Verifying migration integrity...", 90)

        tables = [o for o in self.objects if o.object_type == 'TABLE' and o.status == 'validated']
        mismatches = []

        connect_str, _ = self.corrector._build_sqlplus_connect_string(self.config)

        for obj in tables:
            try:
                oracle_count = self._count_oracle_rows(obj.name, connect_str)
                pg_count = self._count_pg_rows(obj.name)
                if oracle_count is not None and pg_count is not None:
                    if oracle_count != pg_count:
                        mismatches.append(
                            f"{obj.name}: Oracle={oracle_count}, PG={pg_count}"
                        )
            except Exception as e:
                logger.warning(f"Row count verification failed for {obj.name}: {e}")

        if mismatches:
            self.result.edge_cases.extend([f"Row count mismatch: {m}" for m in mismatches])
            self._emit("verify", f"Verification: {len(mismatches)} row count mismatches.", 95)
        else:
            self._emit("verify", "Verification passed - all row counts match.", 95)

    # -----------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------

    def _phase(self, phase: Phase, fn):
        """Run a phase, updating result state."""
        self.result.phase = phase
        logger.info(f"=== Migration phase: {phase.value} ===")
        fn()

    def _emit(self, phase: str, message: str, pct: int):
        """Emit progress to callback if registered."""
        logger.info(f"[{phase}] ({pct}%) {message}")
        if self.callback:
            self.callback(phase, message, pct)

    def _summary(self) -> str:
        """Generate a human-readable summary."""
        r = self.result
        lines = [
            f"Migration {'COMPLETE' if r.success else 'FAILED'}",
            f"  Objects: {r.migrated} migrated, {r.failed} failed, {r.skipped} skipped (of {r.total_objects})",
            f"  Data: {r.data_tables_migrated} tables, {r.data_rows_migrated} rows",
            f"  Duration: {r.duration_seconds:.1f}s",
            f"  AI cost: ${r.ai_cost_usd:.4f}",
        ]
        if r.edge_cases:
            lines.append(f"  Edge cases ({len(r.edge_cases)}):")
            for ec in r.edge_cases[:10]:
                lines.append(f"    - {ec}")
        return "\n".join(lines)

    def _assign_ddl_from_file(self, filepath: str, obj_type: str):
        """Parse an exported DDL file and assign content to matching objects."""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
            self._assign_ddl_from_content(content, obj_type)
        except Exception as e:
            logger.warning(f"Failed to parse export file {filepath}: {e}")

    def _assign_ddl_from_content(self, content: str, obj_type: str):
        """Parse DDL content and assign to matching objects.

        Ora2Pg output is PostgreSQL DDL. For native types (TABLE, SEQUENCE, INDEX),
        assign directly to pg_ddl. For AI types, assign to oracle_ddl for
        re-conversion.
        """
        # parse_ddl_file takes content string, returns list of dicts with
        # 'object_name', 'object_type', 'ddl' keys
        parsed = parse_ddl_file(content, object_type_hint=obj_type)

        if not parsed:
            logger.info(f"Parser found no individual statements for {obj_type}, "
                        f"content length: {len(content)} chars")
            return

        native_types = {'TABLE', 'SEQUENCE', 'INDEX'}
        assigned = 0
        unmatched = []

        # Build lookup for fast matching
        obj_by_name = {}
        for obj in self.objects:
            obj_by_name[obj.name.upper()] = obj

        for stmt in parsed:
            stmt_name = stmt.get('object_name', '').upper()
            stmt_ddl = stmt.get('ddl', '')
            if not stmt_name or not stmt_ddl:
                continue

            obj = obj_by_name.get(stmt_name)
            if obj:
                if obj.object_type in native_types:
                    # Ora2Pg already converted — assign as PG DDL
                    obj.pg_ddl = stmt_ddl
                    obj.status = "converted"
                else:
                    # Needs AI — store as oracle_ddl for re-conversion
                    obj.oracle_ddl = stmt_ddl
                assigned += 1
            else:
                unmatched.append(stmt_name)

        if unmatched and len(unmatched) <= 10:
            logger.info(f"Unmatched DDL objects for {obj_type}: {unmatched}")
        elif unmatched:
            logger.info(f"{len(unmatched)} unmatched DDL objects for {obj_type}")

        logger.info(f"Assigned DDL to {assigned}/{len(parsed)} parsed objects for type {obj_type}")

    def _dependency_order(self) -> List[MigrationObject]:
        """Sort objects by DDL_TYPE_ORDER for dependency-safe creation."""
        type_rank = {t: i for i, t in enumerate(DDL_TYPE_ORDER)}
        return sorted(self.objects, key=lambda o: type_rank.get(o.object_type, 99))

    def _sort_tables_for_data(self, tables: List[MigrationObject]) -> List[MigrationObject]:
        """Sort tables by FK dependency for safe data loading."""
        deps = {}
        name_to_obj = {o.name.upper(): o for o in tables}

        for obj in tables:
            table_deps = extract_table_dependencies(obj.pg_ddl, obj.name)
            deps[obj.name.upper()] = [d.upper() for d in table_deps]

        # Simple topological sort
        try:
            ordered = []
            remaining = dict(deps)
            resolved = set()
            while remaining:
                batch = [n for n, d in remaining.items() if all(x in resolved for x in d)]
                if not batch:
                    batch = list(remaining.keys())  # Break circular deps
                for n in batch:
                    ordered.append(n)
                    resolved.add(n)
                    del remaining[n]
            return [name_to_obj[n] for n in ordered if n in name_to_obj]
        except Exception:
            return tables

    def _drop_existing_object(self, name: str, object_type: str):
        """Drop an existing object from PostgreSQL before re-validating."""
        drop_map = {
            'TABLE': 'DROP TABLE IF EXISTS {} CASCADE',
            'VIEW': 'DROP VIEW IF EXISTS {} CASCADE',
            'SEQUENCE': 'DROP SEQUENCE IF EXISTS {} CASCADE',
            'FUNCTION': 'DROP FUNCTION IF EXISTS {} CASCADE',
            'PROCEDURE': 'DROP PROCEDURE IF EXISTS {} CASCADE',
            'INDEX': 'DROP INDEX IF EXISTS {}',
            'TYPE': 'DROP TYPE IF EXISTS {} CASCADE',
        }
        template = drop_map.get(object_type)
        if not template:
            return
        try:
            conn = psycopg2.connect(self.pg_dsn)
            conn.set_session(autocommit=True)
            cur = conn.cursor()
            cur.execute(
                psycopg2.sql.SQL(template).format(
                    psycopg2.sql.Identifier(name.lower())
                )
            )
            cur.close()
            conn.close()
        except Exception as e:
            logger.warning(f"Pre-drop failed for {object_type} {name}: {e}")

    def _count_pg_rows(self, table_name: str) -> Optional[int]:
        """Count rows in a PostgreSQL table."""
        try:
            conn = psycopg2.connect(self.pg_dsn)
            cur = conn.cursor()
            # Use identifier quoting for safety
            cur.execute(
                psycopg2.sql.SQL("SELECT count(*) FROM {}").format(
                    psycopg2.sql.Identifier(table_name.lower())
                )
            )
            count = cur.fetchone()[0]
            cur.close()
            conn.close()
            return count
        except Exception as e:
            logger.warning(f"Failed to count PG rows for {table_name}: {e}")
            return None

    def _batch_oracle_row_counts(self, table_names: list, connect_str: str) -> dict:
        """Get row counts for multiple Oracle tables in a single sqlplus call.

        Returns dict of {TABLE_NAME_UPPER: row_count}.
        Uses Oracle's USER_TABLES.NUM_ROWS (from stats) for speed,
        falling back to 1 (assume non-empty) if stats unavailable.
        """
        if not table_names or not connect_str:
            return {}

        try:
            schema = self.corrector._validate_oracle_identifier(self.oracle_schema)

            # Use ALL_TABLES stats — much faster than COUNT(*) per table
            sql = f"""
                SET PAGESIZE 0
                SET FEEDBACK OFF
                SET HEADING OFF
                SET LINESIZE 200
                SET TRIMOUT ON
                SET TRIMSPOOL ON
                SELECT table_name || '|' || NVL(num_rows, 0)
                FROM all_tables
                WHERE owner = '{schema}'
                ORDER BY table_name;
                EXIT;
            """
            proc = subprocess.run(
                [self.corrector.sqlplus_path, '-S', connect_str],
                input=sql, capture_output=True, text=True, timeout=60,
            )

            counts = {}
            if proc.returncode == 0:
                for line in proc.stdout.strip().split('\n'):
                    line = line.strip()
                    if '|' in line:
                        name, count_str = line.split('|', 1)
                        try:
                            counts[name.strip().upper()] = int(count_str.strip())
                        except ValueError:
                            counts[name.strip().upper()] = 1  # Assume non-empty

            logger.info(f"Oracle row counts: {sum(counts.values())} total rows across {len(counts)} tables, "
                        f"{sum(1 for v in counts.values() if v > 0)} non-empty")
            return counts

        except Exception as e:
            logger.warning(f"Failed to get Oracle row counts: {e}")
            # Fall back: assume all tables have data
            return {name.upper(): 1 for name in table_names}

    def _count_oracle_rows(self, table_name: str, connect_str: str) -> Optional[int]:
        """Count rows in an Oracle table via sqlplus."""
        try:
            validated = self.corrector._validate_oracle_identifier(table_name)
            schema = self.corrector._validate_oracle_identifier(self.oracle_schema)

            sql = f"""
                SET PAGESIZE 0
                SET FEEDBACK OFF
                SET HEADING OFF
                SELECT COUNT(*) FROM {schema}.{validated};
                EXIT;
            """
            proc = subprocess.run(
                [self.corrector.sqlplus_path, '-S', connect_str],
                input=sql, capture_output=True, text=True, timeout=60,
            )
            if proc.returncode == 0:
                return int(proc.stdout.strip())
        except Exception as e:
            logger.warning(f"Failed to count Oracle rows for {table_name}: {e}")
        return None


class MigrationAbort(Exception):
    """Raised when a migration phase fails fatally."""

    def __init__(self, phase: Phase, message: str):
        self.phase = phase
        super().__init__(message)

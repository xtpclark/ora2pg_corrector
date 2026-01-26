"""
Orchestrator module for one-click DDL migration workflow.

This module coordinates the full migration pipeline:
1. Discover objects from Oracle
2. Export DDL via Ora2Pg
3. AI-convert to PostgreSQL
4. Validate with self-healing
5. Track status and results
"""

import os
import re
import logging
from datetime import datetime
from .db import get_db, execute_query, get_client_config, extract_ai_settings, ENCRYPTION_KEY
from .sql_processing import Ora2PgAICorrector
from .ddl_parser import parse_ddl_file, count_objects_by_type
from .constants import OUTPUT_DIR, calculate_ai_cost

logger = logging.getLogger(__name__)

# DDL object type ordering for dependency resolution
# Objects should be created in this order to satisfy foreign key and reference dependencies
DDL_TYPE_ORDER = [
    'TYPE',           # Custom types first
    'SEQUENCE',       # Sequences before tables that use them
    'TABLE',          # Tables before views/functions that reference them
    'INDEX',          # Indexes after tables
    'VIEW',           # Views after tables they reference
    'MATERIALIZED VIEW',
    'FUNCTION',       # Functions before procedures/triggers that call them
    'PROCEDURE',
    'TRIGGER',        # Triggers after tables and functions
    'PACKAGE',        # Packages last (complex dependencies)
]

# Reverse order for rollback (drops)
ROLLBACK_TYPE_ORDER = list(reversed(DDL_TYPE_ORDER))


def extract_table_dependencies(ddl_content, table_name):
    """
    Extract table names that this DDL depends on (via REFERENCES clauses).

    Parses FK constraints like:
      - REFERENCES employees(employee_id)
      - REFERENCES hr.employees(employee_id)
      - FOREIGN KEY (col) REFERENCES other_table

    :param str ddl_content: The DDL content to parse
    :param str table_name: The name of the table being created (to avoid self-refs)
    :return: Set of table names this DDL depends on
    :rtype: set
    """
    dependencies = set()

    # Pattern to match REFERENCES table_name (handles schema.table and just table)
    # Matches: REFERENCES table_name, REFERENCES schema.table_name
    ref_pattern = r'REFERENCES\s+(?:[\w]+\.)?(\w+)\s*\('

    for match in re.finditer(ref_pattern, ddl_content, re.IGNORECASE):
        ref_table = match.group(1).upper()
        # Don't add self-references
        if ref_table != table_name.upper():
            dependencies.add(ref_table)

    return dependencies


def topological_sort_files(files, get_content_func):
    """
    Sort TABLE files by FK dependency order, keeping other file types in original order.

    Only TABLE files (individual .sql files, not output_*.sql) are sorted by FK dependencies.
    Views, procedures, etc. are kept in their original order and placed AFTER all tables.

    Handles circular dependencies by breaking cycles (common in schemas like HR where
    EMPLOYEES.department_id -> DEPARTMENTS and DEPARTMENTS.manager_id -> EMPLOYEES).

    :param list files: List of file records with 'file_id' and 'filename'
    :param callable get_content_func: Function(file_id) -> (content, error, export_dir)
    :return: Files sorted in dependency order (tables first, then others)
    :rtype: list
    """
    # Separate TABLE files from other files (views, procedures, etc.)
    # TABLE files are individual files like EMPLOYEES.sql, not output_view.sql
    table_files = []
    other_files = []

    for f in files:
        filename = f['filename']
        # Individual table files don't start with 'output_' and don't contain 'view', 'procedure', etc.
        if filename.lower().startswith('output_') or '_output_' in filename.lower():
            other_files.append(f)
        else:
            table_files.append(f)

    # If no table files to sort, return original order
    if not table_files:
        return files

    # Build dependency graph for TABLE files only
    table_to_file = {}
    file_dependencies = {}  # file_id -> set of dependent table names

    for file_record in table_files:
        file_id = file_record['file_id']
        filename = file_record['filename']

        # Extract table name from filename (e.g., "EMPLOYEES.sql" -> "EMPLOYEES")
        table_name = os.path.splitext(filename)[0].upper()

        table_to_file[table_name] = file_record

        # Get content and extract dependencies
        content, error, _ = get_content_func(file_id)
        if content and not error:
            deps = extract_table_dependencies(content, table_name)
            file_dependencies[file_id] = deps
            if deps:
                logger.debug(f"[Dependency] {table_name} depends on: {deps}")
        else:
            file_dependencies[file_id] = set()

    # Kahn's algorithm for topological sort with cycle breaking
    # Calculate in-degree (number of unresolved dependencies within this file set)
    in_degree = {}
    for file_record in table_files:
        file_id = file_record['file_id']
        deps = file_dependencies.get(file_id, set())
        # Only count dependencies on tables IN this migration
        count = sum(1 for dep in deps if dep in table_to_file)
        in_degree[file_id] = count

    # Start with files that have no dependencies (in_degree == 0)
    queue = [f for f in table_files if in_degree[f['file_id']] == 0]
    sorted_tables = []
    processed_tables = set()

    while queue or len(sorted_tables) < len(table_files):
        if queue:
            # Process file with no pending dependencies
            current = queue.pop(0)
        else:
            # Cycle detected! Break it by picking the file with lowest in-degree
            remaining = [f for f in table_files if f['file_id'] not in processed_tables]
            if not remaining:
                break
            # Sort by in_degree, then by filename for deterministic ordering
            remaining.sort(key=lambda f: (in_degree[f['file_id']], f['filename']))
            current = remaining[0]
            logger.warning(f"[Dependency] Breaking cycle: processing {current['filename']} "
                          f"(in_degree={in_degree[current['file_id']]})")

        sorted_tables.append(current)
        processed_tables.add(current['file_id'])

        # Get table name for this file
        current_filename = current['filename']
        current_table = os.path.splitext(current_filename)[0].upper()

        # Reduce in-degree for files that depend on this one
        for file_record in table_files:
            file_id = file_record['file_id']
            if file_id in processed_tables:
                continue
            deps = file_dependencies.get(file_id, set())
            if current_table in deps:
                in_degree[file_id] -= 1
                if in_degree[file_id] == 0 and file_id not in processed_tables:
                    queue.append(file_record)

    # Log the table order
    table_order = [os.path.splitext(f['filename'])[0] for f in sorted_tables]
    logger.info(f"[Dependency] Table order: {' -> '.join(table_order)}")

    # Return sorted tables first, then other files (views, procedures) in original order
    return sorted_tables + other_files


class MigrationOrchestrator:
    """
    Orchestrates the complete DDL migration workflow.
    """

    def __init__(self, client_id):
        """
        Initialize the orchestrator for a specific client.

        :param int client_id: The client ID to run migration for
        """
        self.client_id = client_id
        self.conn = None
        self.config = None
        self.corrector = None
        self.session_id = None
        self.session_name = None
        self.results = {
            'status': 'pending',
            'phase': None,
            'total_objects': 0,
            'processed_objects': 0,
            'successful': 0,
            'failed': 0,
            'errors': [],
            'files': []
        }

    def _initialize(self):
        """Load config and create corrector instance."""
        self.conn = get_db()
        self.config = get_client_config(self.client_id, self.conn)

        self.corrector = Ora2PgAICorrector(
            output_dir=OUTPUT_DIR,
            ai_settings=extract_ai_settings(self.config),
            encryption_key=ENCRYPTION_KEY
        )

    def _update_session_status(self, status):
        """Update the workflow status of the current session."""
        if not self.session_id:
            return

        query = 'UPDATE migration_sessions SET workflow_status = ? WHERE session_id = ?'
        execute_query(self.conn, query, (status, self.session_id))
        self.conn.commit()

    def _update_session_progress(self, phase=None, processed=None, total=None, current_file=None):
        """
        Update migration progress in the database.

        This method stores progress in the database to fix the multi-worker race condition
        where in-memory state wasn't shared across gunicorn workers.

        :param str phase: Current phase (discovery, export, validating, fk_constraints, completed)
        :param int processed: Number of files processed so far
        :param int total: Total number of files to process
        :param str current_file: Name of the file currently being processed
        """
        if not self.session_id:
            return

        # Build dynamic UPDATE query with only the fields that are being updated
        updates = []
        params = []

        if phase is not None:
            updates.append('current_phase = ?')
            params.append(phase)
        if processed is not None:
            updates.append('processed_count = ?')
            params.append(processed)
        if total is not None:
            updates.append('total_count = ?')
            params.append(total)
        if current_file is not None:
            updates.append('current_file = ?')
            params.append(current_file)

        if not updates:
            return

        params.append(self.session_id)
        query = f"UPDATE migration_sessions SET {', '.join(updates)} WHERE session_id = ?"
        execute_query(self.conn, query, tuple(params))
        self.conn.commit()

    def _update_file_status(self, file_id, status, corrected_content=None, error_message=None,
                            input_tokens=0, output_tokens=0, ai_attempts=0):
        """Update the status and content of a migration file, including token metrics."""
        # Build dynamic update query based on provided parameters
        updates = ['status = ?', 'last_modified = CURRENT_TIMESTAMP']
        params = [status]

        if corrected_content is not None:
            updates.append('corrected_content = ?')
            params.append(corrected_content)
        if error_message is not None:
            updates.append('error_message = ?')
            params.append(error_message)
        if input_tokens > 0:
            updates.append('input_tokens = ?')
            params.append(input_tokens)
        if output_tokens > 0:
            updates.append('output_tokens = ?')
            params.append(output_tokens)
        if ai_attempts > 0:
            updates.append('ai_attempts = ?')
            params.append(ai_attempts)

        params.append(file_id)
        query = f"UPDATE migration_files SET {', '.join(updates)} WHERE file_id = ?"
        execute_query(self.conn, query, tuple(params))
        self.conn.commit()

    def _get_file_content(self, file_id):
        """Retrieve the content of an exported file and its directory."""
        query = '''SELECT mf.filename, ms.export_directory
                   FROM migration_files mf
                   JOIN migration_sessions ms ON mf.session_id = ms.session_id
                   WHERE mf.file_id = ?'''
        cursor = execute_query(self.conn, query, (file_id,))
        file_info = cursor.fetchone()

        if not file_info:
            return None, "File not found", None

        export_dir = file_info['export_directory']
        file_path = os.path.join(export_dir, file_info['filename'])
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read(), None, export_dir
        except Exception as e:
            return None, str(e), None

    def _register_objects_from_file(self, file_id, session_id, content):
        """
        Parse DDL file and register individual objects in migration_objects table.

        :param int file_id: The migration_files.file_id
        :param int session_id: The migration_sessions.session_id
        :param str content: The DDL file content
        :return: Number of objects registered
        """
        try:
            objects = parse_ddl_file(content)

            for obj in objects:
                query = '''INSERT INTO migration_objects
                           (session_id, file_id, object_name, object_type, status,
                            original_ddl, line_start, line_end)
                           VALUES (?, ?, ?, ?, 'pending', ?, ?, ?)'''
                execute_query(self.conn, query, (
                    session_id,
                    file_id,
                    obj['object_name'],
                    obj['object_type'],
                    obj['ddl'],
                    obj['line_start'],
                    obj['line_end']
                ))

            self.conn.commit()
            logger.info(f"[Client {self.client_id}] Registered {len(objects)} objects from file {file_id}")
            return len(objects)

        except Exception as e:
            logger.error(f"[Client {self.client_id}] Failed to register objects: {e}")
            return 0

    def _update_object_status(self, session_id, object_name, status, error_message=None, corrected_ddl=None):
        """
        Update status for a specific object.

        :param int session_id: Session ID
        :param str object_name: Name of the object
        :param str status: New status (pending, validated, failed, skipped)
        :param str error_message: Optional error message
        :param str corrected_ddl: Optional AI-corrected DDL
        """
        if corrected_ddl:
            query = '''UPDATE migration_objects
                       SET status = ?, error_message = ?, corrected_ddl = ?,
                           ai_corrected = 1, validated_at = CURRENT_TIMESTAMP
                       WHERE session_id = ? AND object_name = ?'''
            execute_query(self.conn, query, (status, error_message, corrected_ddl, session_id, object_name))
        elif error_message:
            query = '''UPDATE migration_objects
                       SET status = ?, error_message = ?, validated_at = CURRENT_TIMESTAMP
                       WHERE session_id = ? AND object_name = ?'''
            execute_query(self.conn, query, (status, error_message, session_id, object_name))
        else:
            query = '''UPDATE migration_objects
                       SET status = ?, validated_at = CURRENT_TIMESTAMP
                       WHERE session_id = ? AND object_name = ?'''
            execute_query(self.conn, query, (status, session_id, object_name))
        self.conn.commit()

    def _update_objects_in_file(self, file_id, status, error_message=None, ai_corrected=False):
        """
        Update status for all objects belonging to a file.

        :param int file_id: The file ID
        :param str status: New status
        :param str error_message: Optional error message (for failures)
        :param bool ai_corrected: Whether AI correction was applied
        """
        if error_message:
            query = '''UPDATE migration_objects
                       SET status = ?, error_message = ?, validated_at = CURRENT_TIMESTAMP
                       WHERE file_id = ?'''
            execute_query(self.conn, query, (status, error_message, file_id))
        elif ai_corrected:
            query = '''UPDATE migration_objects
                       SET status = ?, ai_corrected = 1, validated_at = CURRENT_TIMESTAMP
                       WHERE file_id = ?'''
            execute_query(self.conn, query, (status, file_id))
        else:
            query = '''UPDATE migration_objects
                       SET status = ?, validated_at = CURRENT_TIMESTAMP
                       WHERE file_id = ?'''
            execute_query(self.conn, query, (status, file_id))
        self.conn.commit()

    def discover_objects(self):
        """
        Phase 1: Discover all objects from Oracle schema.

        :return: List of discovered objects grouped by type
        :rtype: dict
        """
        self.results['phase'] = 'discovery'
        # Note: session_id not yet available, so _update_session_* calls would no-op
        # Progress will be updated once session is created during export

        logger.info(f"[Client {self.client_id}] Starting object discovery")

        objects, error = self.corrector._get_object_list(self.conn, self.config)

        if error:
            self.results['status'] = 'failed'
            self.results['errors'].append(f"Discovery failed: {error}")
            return None

        # Filter to supported objects only and group by type
        supported_objects = [obj for obj in objects if obj['supported']]
        objects_by_type = {}

        for obj in supported_objects:
            obj_type = obj['type']
            if obj_type not in objects_by_type:
                objects_by_type[obj_type] = []
            objects_by_type[obj_type].append(obj['name'])

        self.results['total_objects'] = len(supported_objects)
        logger.info(f"[Client {self.client_id}] Discovered {len(supported_objects)} supported objects")

        return objects_by_type

    def export_ddl(self, objects_by_type, auto_create_ddl=True):
        """
        Phase 2: Export DDL for all objects using Ora2Pg.

        :param dict objects_by_type: Objects grouped by type from discovery
        :param bool auto_create_ddl: Whether auto-create DDL is enabled. When False,
                                     forces file_per_table for TABLEs to enable
                                     dependency-ordered validation.
        :return: List of exported file records
        :rtype: list
        """
        self.results['phase'] = 'export'
        # Note: session_id not yet available until first export completes
        # Status/progress updates happen after session is created in the export loop

        logger.info(f"[Client {self.client_id}] Starting DDL export")

        all_files = []

        # Sort types by dependency order
        sorted_types = sorted(
            objects_by_type.keys(),
            key=lambda t: DDL_TYPE_ORDER.index(t) if t in DDL_TYPE_ORDER else 999
        )

        for type_idx, obj_type in enumerate(sorted_types):
            obj_names = objects_by_type[obj_type]
            logger.info(f"[Client {self.client_id}] Exporting {len(obj_names)} {obj_type}(s)")
            self._update_session_progress(processed=type_idx, current_file=f"Exporting {obj_type}...")

            # Configure for this type
            export_config = self.config.copy()
            export_config['type'] = obj_type
            export_config['ALLOW'] = ','.join(obj_names)

            # Force file_per_table for TABLEs when auto_create_ddl is OFF
            # This enables dependency-ordered validation (tables created in FK order)
            # BUT respect user's explicit FILE_PER_TABLE=0 setting for faster exports
            # Note: file_per_table is a boolean after config loading (see BOOLEAN_CONFIG_KEYS)
            user_file_per_table = self.config.get('file_per_table', True)
            if obj_type == 'TABLE' and not auto_create_ddl and user_file_per_table:
                export_config['file_per_table'] = True
                logger.info(f"[Client {self.client_id}] Forcing file_per_table for dependency-ordered validation")
            elif obj_type == 'TABLE' and not user_file_per_table:
                export_config['file_per_table'] = False
                logger.info(f"[Client {self.client_id}] Using FILE_PER_TABLE=0 per user setting (single file export)")

            # Run export - first export creates session, subsequent exports add to it
            result, error = self.corrector.run_ora2pg_export(
                self.client_id, self.conn, export_config,
                session_name=self.session_name if not self.session_id else None,
                existing_session_id=self.session_id  # Add to existing session if we have one
            )

            if error:
                self.results['errors'].append(f"Export failed for {obj_type}: {error}")
                logger.error(f"[Client {self.client_id}] Export failed for {obj_type}: {error}")
                continue

            # Track the session from the first successful export
            # Note: Progress updates happen inside run_ora2pg_export() during the table loop
            if not self.session_id and result.get('session_id'):
                self.session_id = result['session_id']

            # Collect file info and register individual objects
            if result.get('files'):
                session_id = result.get('session_id')
                exported_filenames = result.get('files')  # Only files from THIS export

                # Fetch file IDs only for files created by THIS export (not all session files)
                placeholders = ','.join(['?' for _ in exported_filenames])
                query = f'''SELECT file_id, filename FROM migration_files
                           WHERE session_id = ? AND filename IN ({placeholders})
                           ORDER BY file_id'''
                cursor = execute_query(self.conn, query, (session_id, *exported_filenames))
                files = [dict(row) for row in cursor.fetchall()]

                # Parse each file and register individual objects
                total_objects = 0
                for file_info in files:
                    content, error, _ = self._get_file_content(file_info['file_id'])
                    if content and not error:
                        num_objects = self._register_objects_from_file(
                            file_info['file_id'], session_id, content
                        )
                        total_objects += num_objects

                # Update total_objects in results to reflect actual parsed objects
                self.results['total_objects'] = self.results.get('total_objects', 0) + total_objects

                all_files.extend(files)

        # If we exported multiple object types, update session to show "DDL" instead of just the first type
        if len(sorted_types) > 1 and self.session_id:
            execute_query(self.conn,
                'UPDATE migration_sessions SET export_type = ? WHERE session_id = ?',
                ('DDL', self.session_id))
            self.conn.commit()
            logger.info(f"[Client {self.client_id}] Updated session {self.session_id} export_type to 'DDL' (multi-type export)")

        logger.info(f"[Client {self.client_id}] Exported {len(all_files)} files with {self.results.get('total_objects', 0)} objects")
        return all_files

    def convert_and_validate(self, files, validation_options=None):
        """
        Phase 3 & 4: Validate each exported file, using AI only when needed.

        OPTIMIZED WORKFLOW (Option C):
        1. Strip psql metacommands from Ora2Pg output
        2. Validate directly against PostgreSQL
        3. If validation fails, AI fixes based on error message (self-healing)
        4. Only uses AI tokens when actually needed

        :param list files: List of file records to process
        :param dict validation_options: Options like clean_slate, auto_create_ddl
        :return: Results summary
        :rtype: dict
        """
        self.results['phase'] = 'validating'
        self._update_session_status('validating')
        self._update_session_progress(phase='validating', processed=0, total=len(files))

        if validation_options is None:
            validation_options = {
                'clean_slate': False,
                'auto_create_ddl': True
            }

        validation_dsn = self.config.get('validation_pg_dsn')
        if not validation_dsn:
            logger.warning(f"[Client {self.client_id}] No validation DSN configured, skipping validation")

        # Sort files by dependency order (tables with FKs come after tables they reference)
        # This ensures that when we validate DEPARTMENTS, EMPLOYEES already exists
        logger.info(f"[Client {self.client_id}] Sorting {len(files)} files by dependency order...")
        sorted_files = topological_sort_files(files, self._get_file_content)

        # Log the order for debugging
        file_order = [f['filename'] for f in sorted_files]
        logger.info(f"[Client {self.client_id}] Validation order: {', '.join(file_order)}")

        # Collect deferred FK constraints to execute after all tables exist
        all_deferred_fks = []

        # Track total AI token usage for the session
        session_input_tokens = 0
        session_output_tokens = 0

        for file_idx, file_record in enumerate(sorted_files):
            file_id = file_record['file_id']
            filename = file_record['filename']
            self.results['processed_objects'] += 1

            # Update progress in database for cross-worker visibility
            self._update_session_progress(processed=file_idx, current_file=filename)

            logger.info(f"[Client {self.client_id}] Processing file {filename} ({self.results['processed_objects']}/{len(files)})")

            # Get file content and export directory
            content, error, export_dir = self._get_file_content(file_id)
            if error:
                self._update_file_status(file_id, 'failed', error_message=error)
                self.results['failed'] += 1
                self.results['errors'].append(f"{filename}: {error}")
                continue

            # Skip empty files
            if not content or not content.strip():
                self._update_file_status(file_id, 'skipped', error_message='Empty file')
                continue

            # Validation (if DSN configured)
            # validate_sql() already:
            #   - Strips psql metacommands
            #   - Validates against PostgreSQL
            #   - Uses AI to fix errors (self-healing with retries)
            #   - Creates missing dependency DDL if auto_create_ddl=True
            #   - Caches AI-generated DDL for reuse (if cache_context provided)
            if validation_dsn:
                try:
                    # Build cache context for DDL caching
                    cache_context = {
                        'db_conn': self.conn,
                        'client_id': self.client_id,
                        'export_dir': export_dir
                    }

                    # Metrics dict for tracking AI token usage per file
                    file_metrics = {'input_tokens': 0, 'output_tokens': 0, 'ai_attempts': 0}

                    is_valid, message, corrected_sql, deferred_fks = self.corrector.validate_sql(
                        content,
                        validation_dsn,
                        clean_slate=validation_options.get('clean_slate', False),
                        auto_create_ddl=validation_options.get('auto_create_ddl', True),
                        cache_context=cache_context,
                        defer_fk=True,  # Defer FK constraints until all tables exist
                        metrics=file_metrics  # Track AI tokens
                    )

                    # Accumulate session-level token totals
                    session_input_tokens += file_metrics.get('input_tokens', 0)
                    session_output_tokens += file_metrics.get('output_tokens', 0)

                    # Collect deferred FK constraints
                    if deferred_fks:
                        all_deferred_fks.extend([(filename, fk) for fk in deferred_fks])

                    if is_valid:
                        # corrected_sql is only set if AI made changes
                        final_content = corrected_sql if corrected_sql else content
                        self._update_file_status(file_id, 'validated', corrected_content=final_content,
                                                input_tokens=file_metrics.get('input_tokens', 0),
                                                output_tokens=file_metrics.get('output_tokens', 0),
                                                ai_attempts=file_metrics.get('ai_attempts', 0))
                        self.results['successful'] += 1

                        # Update all objects in this file as validated
                        self._update_objects_in_file(file_id, 'validated',
                                                     ai_corrected=bool(corrected_sql))

                        if corrected_sql:
                            logger.info(f"[Client {self.client_id}] Validated {filename} (AI-corrected, {file_metrics.get('ai_attempts', 0)} AI calls)")
                        else:
                            logger.info(f"[Client {self.client_id}] Validated {filename} (no AI needed)")
                    else:
                        self._update_file_status(file_id, 'failed',
                                                corrected_content=corrected_sql,
                                                error_message=message,
                                                input_tokens=file_metrics.get('input_tokens', 0),
                                                output_tokens=file_metrics.get('output_tokens', 0),
                                                ai_attempts=file_metrics.get('ai_attempts', 0))
                        self.results['failed'] += 1
                        self.results['errors'].append(f"{filename}: {message}")

                        # Update all objects in this file as failed
                        self._update_objects_in_file(file_id, 'failed', error_message=message)

                        logger.warning(f"[Client {self.client_id}] Validation failed for {filename}: {message}")

                except Exception as e:
                    self._update_file_status(file_id, 'failed',
                                            error_message=f"Validation error: {str(e)}")
                    self.results['failed'] += 1
                    self.results['errors'].append(f"{filename}: Validation error - {str(e)}")
            else:
                # No validation DSN - just mark as exported (can't validate)
                self._update_file_status(file_id, 'exported')
                self.results['successful'] += 1
                logger.info(f"[Client {self.client_id}] Exported {filename} (no validation DSN)")

            self.results['files'].append({
                'file_id': file_id,
                'filename': filename,
                'status': 'validated' if validation_dsn else 'exported'
            })

        # Phase 2: Execute deferred FK constraints now that all tables exist
        if all_deferred_fks and validation_dsn:
            logger.info(f"[Client {self.client_id}] Executing {len(all_deferred_fks)} deferred FK constraint(s)...")
            self._update_session_progress(phase='fk_constraints', processed=0, total=len(all_deferred_fks), current_file='Applying FK constraints...')
            import psycopg2
            try:
                with psycopg2.connect(validation_dsn) as conn:
                    with conn.cursor() as cursor:
                        conn.set_session(autocommit=True)
                        for fk_idx, (source_file, fk_sql) in enumerate(all_deferred_fks):
                            self._update_session_progress(processed=fk_idx, current_file=f'FK from {source_file}')
                            try:
                                cursor.execute(fk_sql)
                                logger.info(f"[Client {self.client_id}] Applied FK from {source_file}")
                            except psycopg2.Error as e:
                                error_msg = str(e).strip()
                                # If constraint already exists, that's fine
                                if 'already exists' in error_msg:
                                    logger.info(f"[Client {self.client_id}] FK from {source_file} already exists, skipping")
                                else:
                                    logger.error(f"[Client {self.client_id}] FK from {source_file} failed: {error_msg}")
                                    self.results['errors'].append(f"FK constraint from {source_file}: {error_msg}")
            except psycopg2.Error as e:
                logger.error(f"[Client {self.client_id}] Failed to apply deferred FKs: {e}")
                self.results['errors'].append(f"Deferred FK execution failed: {e}")

        # Store session token totals in results for caller access
        self.results['total_input_tokens'] = session_input_tokens
        self.results['total_output_tokens'] = session_output_tokens

        return self.results

    def run_full_migration(self, options=None):
        """
        Execute the complete one-click DDL migration workflow.

        :param dict options: Migration options
            - clean_slate: Drop existing tables before validation (default: False)
            - auto_create_ddl: Auto-create missing tables during validation (default: True)
            - object_types: List of object types to migrate (default: all supported)
            - session_name: Friendly name for the migration session (default: auto-generated)
        :return: Migration results
        :rtype: dict
        """
        if options is None:
            options = {}

        # Store session name for use during export
        self.session_name = options.get('session_name')

        try:
            # Initialize
            self._initialize()
            self.results['status'] = 'running'
            self.results['started_at'] = datetime.now().isoformat()

            logger.info(f"[Client {self.client_id}] Starting full DDL migration")

            # Phase 1: Discovery
            objects_by_type = self.discover_objects()
            if objects_by_type is None:
                self.results['status'] = 'failed'
                self._update_session_status('failed')
                return self.results

            # Filter object types if specified
            if options.get('object_types'):
                allowed_types = set(options['object_types'])
                objects_by_type = {k: v for k, v in objects_by_type.items() if k in allowed_types}

            if not objects_by_type:
                self.results['status'] = 'completed'
                self.results['message'] = 'No objects to migrate'
                return self.results

            # Phase 2: Export
            auto_create_ddl = options.get('auto_create_ddl', True)
            files = self.export_ddl(objects_by_type, auto_create_ddl=auto_create_ddl)
            if not files:
                self.results['status'] = 'completed'
                self.results['message'] = 'No files exported'
                self._update_session_status('completed')
                return self.results

            # Phase 3 & 4: Convert and Validate
            validation_options = {
                'clean_slate': options.get('clean_slate', False),
                'auto_create_ddl': options.get('auto_create_ddl', True)
            }
            self.convert_and_validate(files, validation_options)

            # Final status
            if self.results['failed'] == 0:
                self.results['status'] = 'completed'
                self._update_session_status('completed')
                self._update_session_progress(phase='completed', current_file='Migration complete')
            elif self.results['successful'] > 0:
                self.results['status'] = 'partial'
                self._update_session_status('partial')
                self._update_session_progress(phase='completed', current_file='Migration partial')
            else:
                self.results['status'] = 'failed'
                self._update_session_status('failed')
                self._update_session_progress(phase='failed', current_file='Migration failed')

            # Generate rollback script for successful/partial migrations
            if self.results['successful'] > 0:
                try:
                    # Get validated files with their content
                    validated_files = [f for f in self.results['files'] if f.get('status') == 'validated']
                    if validated_files:
                        # Get export directory from first file
                        _, _, export_dir = self._get_file_content(validated_files[0]['file_id'])
                        if export_dir:
                            rollback_sql = self._generate_rollback_script(validated_files)
                            if rollback_sql:
                                self._save_rollback_script(rollback_sql, export_dir)
                                self.results['rollback_generated'] = True
                except Exception as e:
                    logger.warning(f"Failed to generate rollback script: {e}")

            # Update session with token totals and cost estimate (from convert_and_validate results)
            session_input_tokens = self.results.get('total_input_tokens', 0)
            session_output_tokens = self.results.get('total_output_tokens', 0)
            if session_input_tokens > 0 or session_output_tokens > 0:
                ai_model = self.config.get('ai_model', '')
                estimated_cost = calculate_ai_cost(ai_model, session_input_tokens, session_output_tokens)
                query = '''UPDATE migration_sessions
                           SET total_input_tokens = ?, total_output_tokens = ?,
                               estimated_cost_usd = ?, completed_at = CURRENT_TIMESTAMP
                           WHERE session_id = ?'''
                execute_query(self.conn, query, (session_input_tokens, session_output_tokens,
                                                  estimated_cost, self.session_id))
                self.conn.commit()
                logger.info(f"[Client {self.client_id}] Session tokens: {session_input_tokens} input, {session_output_tokens} output, est. cost: ${estimated_cost:.4f}")

            self.results['completed_at'] = datetime.now().isoformat()
            logger.info(f"[Client {self.client_id}] Migration completed: {self.results['successful']} successful, {self.results['failed']} failed")

            return self.results

        except Exception as e:
            logger.error(f"[Client {self.client_id}] Migration failed with exception: {e}", exc_info=True)
            self.results['status'] = 'failed'
            self.results['errors'].append(f"Unexpected error: {str(e)}")
            if self.session_id:
                self._update_session_status('failed')
            return self.results

    def get_status(self):
        """
        Get the current migration status.

        :return: Current results/status
        :rtype: dict
        """
        # Include session_id in the status
        status = self.results.copy()
        status['session_id'] = self.session_id
        return status

    # =================================================================
    # Rollback Script Generation Methods
    # =================================================================

    def _parse_ddl_objects(self, sql):
        """
        Extract object names and types from CREATE statements in SQL.

        :param str sql: SQL content to parse
        :return: List of (object_type, object_name, drop_statement) tuples
        :rtype: list
        """
        objects = []

        # Patterns for different CREATE statements
        patterns = {
            'TABLE': r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)',
            'VIEW': r'CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+([^\s(]+)',
            'MATERIALIZED VIEW': r'CREATE\s+MATERIALIZED\s+VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s]+)',
            'INDEX': r'CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?([^\s]+)',
            'FUNCTION': r'CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+([^\s(]+)',
            'PROCEDURE': r'CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+([^\s(]+)',
            'SEQUENCE': r'CREATE\s+SEQUENCE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s]+)',
            'TYPE': r'CREATE\s+TYPE\s+([^\s]+)',
            'TRIGGER': r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:CONSTRAINT\s+)?TRIGGER\s+([^\s]+)',
        }

        for obj_type, pattern in patterns.items():
            matches = re.finditer(pattern, sql, re.IGNORECASE | re.MULTILINE)
            for match in matches:
                obj_name = match.group(1).strip('"').strip("'")
                # Clean up schema-qualified names
                obj_name = obj_name.split('.')[-1] if '.' in obj_name else obj_name

                # Generate appropriate DROP statement
                drop_stmt = self._generate_drop_statement(obj_type, obj_name, sql)
                objects.append((obj_type, obj_name, drop_stmt))

        return objects

    def _generate_drop_statement(self, obj_type, obj_name, full_sql=None):
        """
        Generate a DROP statement for an object.

        :param str obj_type: Type of object
        :param str obj_name: Name of object
        :param str full_sql: Full SQL content (for trigger table detection)
        :return: DROP statement
        :rtype: str
        """
        # Special handling for triggers - need to include table name
        if obj_type == 'TRIGGER' and full_sql:
            # Try to find the table name for this trigger
            pattern = rf'CREATE\s+(?:OR\s+REPLACE\s+)?(?:CONSTRAINT\s+)?TRIGGER\s+{re.escape(obj_name)}\s+.*?ON\s+([^\s]+)'
            match = re.search(pattern, full_sql, re.IGNORECASE | re.DOTALL)
            if match:
                table_name = match.group(1).strip('"').strip("'")
                return f'DROP TRIGGER IF EXISTS "{obj_name}" ON "{table_name}" CASCADE;'

        # Standard DROP statements
        drop_templates = {
            'TABLE': f'DROP TABLE IF EXISTS "{obj_name}" CASCADE;',
            'VIEW': f'DROP VIEW IF EXISTS "{obj_name}" CASCADE;',
            'MATERIALIZED VIEW': f'DROP MATERIALIZED VIEW IF EXISTS "{obj_name}" CASCADE;',
            'INDEX': f'DROP INDEX IF EXISTS "{obj_name}" CASCADE;',
            'FUNCTION': f'DROP FUNCTION IF EXISTS "{obj_name}" CASCADE;',
            'PROCEDURE': f'DROP PROCEDURE IF EXISTS "{obj_name}" CASCADE;',
            'SEQUENCE': f'DROP SEQUENCE IF EXISTS "{obj_name}" CASCADE;',
            'TYPE': f'DROP TYPE IF EXISTS "{obj_name}" CASCADE;',
            'TRIGGER': f'DROP TRIGGER IF EXISTS "{obj_name}" CASCADE;',  # Fallback
            'PACKAGE': f'-- Package "{obj_name}" requires manual drop',
        }

        return drop_templates.get(obj_type, f'-- Unknown type: {obj_type} "{obj_name}"')

    def _generate_rollback_script(self, validated_files):
        """
        Generate a rollback script from validated migration files.

        :param list validated_files: List of validated file records
        :return: Rollback SQL script
        :rtype: str
        """
        all_objects = []

        for file_record in validated_files:
            file_id = file_record.get('file_id')
            if not file_id:
                continue

            # Get file content (corrected or original)
            content, error, _ = self._get_file_content(file_id)
            if error or not content:
                continue

            # Also check for corrected_content in the record
            if file_record.get('corrected_content'):
                content = file_record['corrected_content']

            # Parse objects from this file
            objects = self._parse_ddl_objects(content)
            all_objects.extend(objects)

        if not all_objects:
            return None

        # Remove duplicates while preserving order
        seen = set()
        unique_objects = []
        for obj in all_objects:
            key = (obj[0], obj[1])  # (type, name)
            if key not in seen:
                seen.add(key)
                unique_objects.append(obj)

        # Sort by rollback type order
        def sort_key(obj):
            obj_type = obj[0]
            try:
                return ROLLBACK_TYPE_ORDER.index(obj_type)
            except ValueError:
                return len(ROLLBACK_TYPE_ORDER)

        unique_objects.sort(key=sort_key)

        # Get client name for header
        client_name = self.config.get('client_name', f'Client {self.client_id}')

        # Build the rollback script
        lines = [
            '-- =============================================================================',
            '-- ROLLBACK SCRIPT',
            '-- =============================================================================',
            f'-- Session ID: {self.session_id}',
            f'-- Client: {client_name}',
            f'-- Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
            '--',
            '-- WARNING: This script will DROP all objects created by this migration.',
            '-- Review carefully before executing!',
            '--',
            '-- To execute:',
            '--   psql -h hostname -U username -d database -f rollback.sql',
            '-- =============================================================================',
            '',
            'BEGIN;',
            '',
            '-- Drop in reverse dependency order (safest order)',
            '',
        ]

        current_type = None
        type_counter = 1
        for obj_type, obj_name, drop_stmt in unique_objects:
            if obj_type != current_type:
                current_type = obj_type
                lines.append(f'-- {type_counter}. {obj_type}s')
                type_counter += 1
            lines.append(drop_stmt)

        lines.extend([
            '',
            'COMMIT;',
            '',
            '-- =============================================================================',
            '-- End of rollback script',
            '-- =============================================================================',
        ])

        return '\n'.join(lines)

    def _save_rollback_script(self, rollback_sql, export_dir):
        """
        Save rollback script to file and database.

        :param str rollback_sql: The rollback SQL script
        :param str export_dir: Directory to save the file
        """
        if not rollback_sql:
            return

        # Save to file
        rollback_file = os.path.join(export_dir, 'rollback.sql')
        try:
            with open(rollback_file, 'w', encoding='utf-8') as f:
                f.write(rollback_sql)
            logger.info(f"Rollback script saved to: {rollback_file}")
        except Exception as e:
            logger.warning(f"Failed to save rollback script to file: {e}")

        # Save to database
        try:
            query = '''UPDATE migration_sessions
                       SET rollback_script = ?, rollback_generated_at = CURRENT_TIMESTAMP
                       WHERE session_id = ?'''
            execute_query(self.conn, query, (rollback_sql, self.session_id))
            self.conn.commit()
            logger.info(f"Rollback script stored in database for session {self.session_id}")
        except Exception as e:
            logger.warning(f"Failed to save rollback script to database: {e}")


def run_migration(client_id, options=None):
    """
    Convenience function to run a full migration for a client.

    :param int client_id: The client ID
    :param dict options: Migration options
    :return: Migration results
    :rtype: dict
    """
    orchestrator = MigrationOrchestrator(client_id)
    return orchestrator.run_full_migration(options)


class CompleteMigrationOrchestrator:
    """
    Orchestrates a complete end-to-end migration: DDL + Data + FK validation.

    This combines the DDL migration workflow with data export/load into a single
    unified operation. The workflow is:

    1. DDL Phase: Export, AI-correct, and validate schema objects
    2. FK Prep: Add NOT VALID to FK constraints (enables data load without FK checks)
    3. Apply DDL: Execute DDL on target PostgreSQL
    4. Data Phase: Export data via COPY and load into PostgreSQL
    5. FK Validation: Validate all NOT VALID FK constraints
    6. Report: Generate combined migration report
    """

    def __init__(self, client_id):
        """
        Initialize the complete migration orchestrator.

        :param int client_id: The client ID to run migration for
        """
        self.client_id = client_id
        self.conn = None
        self.config = None
        self.ddl_session_id = None
        self.data_session_id = None
        self.results = {
            'status': 'pending',
            'phase': None,
            'ddl_results': None,
            'data_results': None,
            'fk_validation_results': None,
            'errors': [],
            'started_at': None,
            'completed_at': None
        }

    def _initialize(self):
        """Load config and initialize database connection."""
        self.conn = get_db()
        self.config = get_client_config(self.client_id, self.conn)

    def _update_session_progress(self, session_id, phase=None, current_file=None):
        """Update progress in the database for the given session."""
        if not session_id:
            return

        updates = []
        params = []

        if phase is not None:
            updates.append('current_phase = ?')
            params.append(phase)
        if current_file is not None:
            updates.append('current_file = ?')
            params.append(current_file)

        if not updates:
            return

        params.append(session_id)
        query = f"UPDATE migration_sessions SET {', '.join(updates)} WHERE session_id = ?"
        execute_query(self.conn, query, tuple(params))
        self.conn.commit()

    def run_complete_migration(self, options=None):
        """
        Execute the complete end-to-end migration workflow.

        :param dict options: Migration options
            - clean_slate: Drop existing tables before validation (default: False)
            - auto_create_ddl: Auto-create missing tables during validation (default: True)
            - session_name: Friendly name for the migration session
            - tables: List of tables to migrate data for (default: all)
            - on_error: 'stop' or 'continue' (default: 'continue')
            - constraint_mode: 'replica', 'skip', or 'normal' (default: 'replica')
        :return: Migration results
        :rtype: dict
        """
        import psycopg2
        import os
        import re
        from io import StringIO

        if options is None:
            options = {}

        try:
            self._initialize()
            self.results['status'] = 'running'
            self.results['started_at'] = datetime.now().isoformat()

            logger.info(f"[Client {self.client_id}] Starting complete migration (DDL + Data)")

            # =================================================================
            # PHASE 1: DDL Migration
            # =================================================================
            self.results['phase'] = 'ddl_migration'
            logger.info(f"[Client {self.client_id}] Phase 1: DDL Migration")

            ddl_orchestrator = MigrationOrchestrator(self.client_id)
            ddl_options = {
                'clean_slate': options.get('clean_slate', False),
                'auto_create_ddl': options.get('auto_create_ddl', True),
                'session_name': options.get('session_name', 'Complete Migration')
            }
            ddl_results = ddl_orchestrator.run_full_migration(ddl_options)
            self.ddl_session_id = ddl_orchestrator.session_id
            self.results['ddl_results'] = ddl_results

            if ddl_results['status'] == 'failed' and ddl_results['successful'] == 0:
                self.results['status'] = 'failed'
                self.results['errors'].append('DDL migration failed completely')
                return self.results

            logger.info(f"[Client {self.client_id}] DDL phase complete: "
                       f"{ddl_results['successful']} successful, {ddl_results['failed']} failed")

            # =================================================================
            # PHASE 2: Data Export
            # =================================================================
            self.results['phase'] = 'data_export'
            self._update_session_progress(self.ddl_session_id, phase='data_export',
                                         current_file='Exporting data...')
            logger.info(f"[Client {self.client_id}] Phase 2: Data Export")

            # Get list of tables to export
            tables = options.get('tables')
            if not tables:
                # Get all tables from the DDL migration
                cursor = execute_query(self.conn, '''
                    SELECT DISTINCT object_name FROM migration_objects
                    WHERE session_id = ? AND object_type = 'TABLE' AND status = 'validated'
                ''', (self.ddl_session_id,))
                tables = [row['object_name'] for row in cursor.fetchall()]

            if not tables:
                logger.warning(f"[Client {self.client_id}] No tables to export data for")
                self.results['data_results'] = {'message': 'No tables to export'}
            else:
                # Run Ora2Pg COPY export
                corrector = Ora2PgAICorrector(
                    output_dir=OUTPUT_DIR,
                    ai_settings=extract_ai_settings(self.config),
                    encryption_key=ENCRYPTION_KEY
                )

                export_config = self.config.copy()
                export_config['type'] = 'COPY'
                export_config['ALLOW'] = ','.join(tables)

                data_result, data_error = corrector.run_ora2pg_export(
                    self.client_id, self.conn, export_config,
                    session_name=f"Data Export - {options.get('session_name', 'Complete Migration')}"
                )

                if data_error:
                    self.results['errors'].append(f'Data export failed: {data_error}')
                    logger.error(f"[Client {self.client_id}] Data export failed: {data_error}")
                else:
                    self.data_session_id = data_result.get('session_id')
                    self.results['data_results'] = {
                        'session_id': self.data_session_id,
                        'files': data_result.get('files', []),
                        'tables_exported': len(tables)
                    }
                    logger.info(f"[Client {self.client_id}] Data export complete: "
                               f"{len(data_result.get('files', []))} files")

            # =================================================================
            # PHASE 3: Load Data into PostgreSQL
            # =================================================================
            if self.data_session_id:
                self.results['phase'] = 'data_load'
                self._update_session_progress(self.ddl_session_id, phase='data_load',
                                             current_file='Loading data...')
                logger.info(f"[Client {self.client_id}] Phase 3: Data Load")

                pg_dsn = self.config.get('validation_pg_dsn')
                if not pg_dsn:
                    self.results['errors'].append('No PostgreSQL DSN configured for data load')
                else:
                    # Get export directory
                    cursor = execute_query(self.conn, '''
                        SELECT export_directory FROM migration_sessions WHERE session_id = ?
                    ''', (self.data_session_id,))
                    session_row = cursor.fetchone()
                    export_dir = session_row['export_directory'] if session_row else None

                    if export_dir and os.path.exists(export_dir):
                        constraint_mode = options.get('constraint_mode', 'replica')
                        load_results = self._load_data_files(
                            pg_dsn, export_dir, constraint_mode
                        )
                        self.results['data_results']['load_results'] = load_results
                        logger.info(f"[Client {self.client_id}] Data load complete: "
                                   f"{load_results.get('total_rows', 0)} rows loaded")

            # =================================================================
            # PHASE 4: Validate FK Constraints
            # =================================================================
            self.results['phase'] = 'fk_validation'
            self._update_session_progress(self.ddl_session_id, phase='fk_validation',
                                         current_file='Validating FK constraints...')
            logger.info(f"[Client {self.client_id}] Phase 4: FK Validation")

            pg_dsn = self.config.get('validation_pg_dsn')
            if pg_dsn:
                fk_results = self._validate_fk_constraints(pg_dsn)
                self.results['fk_validation_results'] = fk_results
                logger.info(f"[Client {self.client_id}] FK validation complete: "
                           f"{fk_results.get('validated', 0)} validated, "
                           f"{fk_results.get('failed', 0)} failed")

            # =================================================================
            # FINALIZATION
            # =================================================================
            self.results['phase'] = 'completed'
            self._update_session_progress(self.ddl_session_id, phase='completed',
                                         current_file='Complete migration finished')
            self.results['completed_at'] = datetime.now().isoformat()

            # Determine final status
            has_errors = len(self.results['errors']) > 0
            ddl_failed = ddl_results.get('failed', 0) > 0
            fk_failed = self.results.get('fk_validation_results', {}).get('failed', 0) > 0

            if ddl_results['successful'] == 0:
                self.results['status'] = 'failed'
            elif has_errors or ddl_failed or fk_failed:
                self.results['status'] = 'partial'
            else:
                self.results['status'] = 'completed'

            # Update DDL session workflow_status to reflect complete migration final state
            if self.ddl_session_id:
                query = 'UPDATE migration_sessions SET workflow_status = ?, completed_at = CURRENT_TIMESTAMP WHERE session_id = ?'
                execute_query(self.conn, query, (self.results['status'], self.ddl_session_id))
                self.conn.commit()

            logger.info(f"[Client {self.client_id}] Complete migration finished: {self.results['status']}")
            return self.results

        except Exception as e:
            logger.error(f"[Client {self.client_id}] Complete migration failed: {e}", exc_info=True)
            self.results['status'] = 'failed'
            self.results['errors'].append(f"Unexpected error: {str(e)}")
            return self.results

    def _load_data_files(self, pg_dsn, export_dir, constraint_mode):
        """
        Load COPY data files into PostgreSQL.

        :param str pg_dsn: PostgreSQL connection string
        :param str export_dir: Directory containing COPY files
        :param str constraint_mode: 'replica', 'skip', or 'normal'
        :return: Load results
        :rtype: dict
        """
        import psycopg2
        import os
        import re
        from io import StringIO

        results = {
            'loaded_files': 0,
            'total_rows': 0,
            'constraint_mode': constraint_mode,
            'tables': {},
            'errors': []
        }

        # Get data files (table-specific COPY files)
        sql_files = [f for f in os.listdir(export_dir)
                     if f.endswith('.sql') and '_output_' in f.lower()]

        if not sql_files:
            results['message'] = 'No data files found'
            return results

        try:
            with psycopg2.connect(pg_dsn) as pg_conn:
                with pg_conn.cursor() as pg_cursor:
                    # Apply constraint mode
                    if constraint_mode == 'replica':
                        pg_cursor.execute("SET session_replication_role = replica")
                        results['constraint_handling'] = 'FK checks bypassed via replica mode'

                    for filename in sql_files:
                        file_path = os.path.join(export_dir, filename)
                        try:
                            with open(file_path, 'r') as f:
                                sql_content = f.read()

                            if not sql_content.strip():
                                continue

                            # Extract table name
                            table_match = re.match(r'([A-Za-z_][A-Za-z0-9_]*)_output_', filename)
                            table_name = table_match.group(1).lower() if table_match else 'unknown'

                            # Handle COPY FROM STDIN format
                            if 'COPY' in sql_content and 'FROM STDIN' in sql_content:
                                # Execute SET commands first
                                for line in sql_content.split('\n'):
                                    stripped = line.strip().upper()
                                    if stripped.startswith('SET '):
                                        pg_cursor.execute(line)

                                # Find and execute COPY command
                                copy_match = re.search(
                                    r'(COPY\s+\S+\s*\([^)]+\)\s*FROM\s+STDIN[^;]*;?)\s*\n(.*)',
                                    sql_content,
                                    re.IGNORECASE | re.DOTALL
                                )

                                if copy_match:
                                    copy_cmd = copy_match.group(1).rstrip(';')
                                    data_section = copy_match.group(2)
                                    data_section = re.sub(r'\n\\.\s*$', '', data_section)

                                    pg_cursor.copy_expert(
                                        f"{copy_cmd} ",
                                        StringIO(data_section)
                                    )

                                rows_affected = pg_cursor.rowcount if pg_cursor.rowcount > 0 else 0
                            else:
                                # Regular SQL (INSERT statements)
                                pg_cursor.execute(sql_content)
                                rows_affected = pg_cursor.rowcount if pg_cursor.rowcount > 0 else 0

                            results['tables'][table_name] = {
                                'rows': rows_affected,
                                'status': 'success'
                            }
                            results['loaded_files'] += 1
                            results['total_rows'] += rows_affected

                        except psycopg2.Error as e:
                            error_msg = str(e).split('\n')[0]
                            results['tables'][table_name] = {
                                'rows': 0,
                                'status': 'failed',
                                'error': error_msg
                            }
                            results['errors'].append(f'{table_name}: {error_msg}')
                            pg_conn.rollback()

                    # Restore session_replication_role
                    if constraint_mode == 'replica':
                        pg_cursor.execute("SET session_replication_role = DEFAULT")

                    pg_conn.commit()

        except psycopg2.Error as e:
            results['errors'].append(f'Database connection error: {str(e)}')

        return results

    def _validate_fk_constraints(self, pg_dsn):
        """
        Validate all NOT VALID FK constraints.

        :param str pg_dsn: PostgreSQL connection string
        :return: Validation results
        :rtype: dict
        """
        import psycopg2

        results = {
            'validated': 0,
            'failed': 0,
            'constraints': {},
            'errors': []
        }

        try:
            with psycopg2.connect(pg_dsn) as pg_conn:
                with pg_conn.cursor() as pg_cursor:
                    # Find all NOT VALID FK constraints
                    pg_cursor.execute("""
                        SELECT c.conname AS constraint_name,
                               t.relname AS table_name
                        FROM pg_constraint c
                        JOIN pg_class t ON c.conrelid = t.oid
                        JOIN pg_namespace n ON t.relnamespace = n.oid
                        WHERE c.contype = 'f'
                        AND n.nspname = 'public'
                        AND NOT c.convalidated
                    """)
                    constraints = pg_cursor.fetchall()

                    if not constraints:
                        results['message'] = 'No NOT VALID constraints to validate'
                        return results

                    results['total_constraints'] = len(constraints)

                    for constraint_name, table_name in constraints:
                        try:
                            validate_sql = f'ALTER TABLE "{table_name}" VALIDATE CONSTRAINT "{constraint_name}"'
                            pg_cursor.execute(validate_sql)
                            pg_conn.commit()

                            results['constraints'][constraint_name] = {
                                'table': table_name,
                                'status': 'valid'
                            }
                            results['validated'] += 1

                        except psycopg2.Error as e:
                            pg_conn.rollback()
                            error_msg = str(e).split('\n')[0]
                            results['constraints'][constraint_name] = {
                                'table': table_name,
                                'status': 'failed',
                                'error': error_msg
                            }
                            results['failed'] += 1
                            results['errors'].append(f'{constraint_name}: {error_msg}')

        except psycopg2.Error as e:
            results['errors'].append(f'Database connection error: {str(e)}')

        return results

    def get_status(self):
        """
        Get the current migration status.

        :return: Current results/status
        :rtype: dict
        """
        status = self.results.copy()
        status['ddl_session_id'] = self.ddl_session_id
        status['data_session_id'] = self.data_session_id
        return status


def run_complete_migration(client_id, options=None):
    """
    Convenience function to run a complete end-to-end migration.

    :param int client_id: The client ID
    :param dict options: Migration options
    :return: Migration results
    :rtype: dict
    """
    orchestrator = CompleteMigrationOrchestrator(client_id)
    return orchestrator.run_complete_migration(options)

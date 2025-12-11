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
from .constants import OUTPUT_DIR

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

    def _update_file_status(self, file_id, status, corrected_content=None, error_message=None):
        """Update the status and content of a migration file."""
        if corrected_content and error_message:
            query = '''UPDATE migration_files
                       SET status = ?, corrected_content = ?, error_message = ?, last_modified = CURRENT_TIMESTAMP
                       WHERE file_id = ?'''
            execute_query(self.conn, query, (status, corrected_content, error_message, file_id))
        elif corrected_content:
            query = '''UPDATE migration_files
                       SET status = ?, corrected_content = ?, last_modified = CURRENT_TIMESTAMP
                       WHERE file_id = ?'''
            execute_query(self.conn, query, (status, corrected_content, file_id))
        elif error_message:
            query = '''UPDATE migration_files
                       SET status = ?, error_message = ?, last_modified = CURRENT_TIMESTAMP
                       WHERE file_id = ?'''
            execute_query(self.conn, query, (status, error_message, file_id))
        else:
            query = '''UPDATE migration_files
                       SET status = ?, last_modified = CURRENT_TIMESTAMP
                       WHERE file_id = ?'''
            execute_query(self.conn, query, (status, file_id))
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
        self._update_session_status('discovering')

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

    def export_ddl(self, objects_by_type):
        """
        Phase 2: Export DDL for all objects using Ora2Pg.

        :param dict objects_by_type: Objects grouped by type from discovery
        :return: List of exported file records
        :rtype: list
        """
        self.results['phase'] = 'export'
        self._update_session_status('exporting')

        logger.info(f"[Client {self.client_id}] Starting DDL export")

        all_files = []

        # Sort types by dependency order
        sorted_types = sorted(
            objects_by_type.keys(),
            key=lambda t: DDL_TYPE_ORDER.index(t) if t in DDL_TYPE_ORDER else 999
        )

        for obj_type in sorted_types:
            obj_names = objects_by_type[obj_type]
            logger.info(f"[Client {self.client_id}] Exporting {len(obj_names)} {obj_type}(s)")

            # Configure for this type
            export_config = self.config.copy()
            export_config['type'] = obj_type
            export_config['ALLOW'] = ','.join(obj_names)

            # Run export
            result, error = self.corrector.run_ora2pg_export(
                self.client_id, self.conn, export_config
            )

            if error:
                self.results['errors'].append(f"Export failed for {obj_type}: {error}")
                logger.error(f"[Client {self.client_id}] Export failed for {obj_type}: {error}")
                continue

            # Track the session from the first successful export
            if not self.session_id and result.get('session_id'):
                self.session_id = result['session_id']

            # Collect file info and register individual objects
            if result.get('files'):
                session_id = result.get('session_id')
                # Fetch file IDs from database
                query = '''SELECT file_id, filename FROM migration_files
                          WHERE session_id = ? ORDER BY file_id'''
                cursor = execute_query(self.conn, query, (session_id,))
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

        if validation_options is None:
            validation_options = {
                'clean_slate': False,
                'auto_create_ddl': True
            }

        validation_dsn = self.config.get('validation_pg_dsn')
        if not validation_dsn:
            logger.warning(f"[Client {self.client_id}] No validation DSN configured, skipping validation")

        for file_record in files:
            file_id = file_record['file_id']
            filename = file_record['filename']
            self.results['processed_objects'] += 1

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

                    is_valid, message, corrected_sql = self.corrector.validate_sql(
                        content,
                        validation_dsn,
                        clean_slate=validation_options.get('clean_slate', False),
                        auto_create_ddl=validation_options.get('auto_create_ddl', True),
                        cache_context=cache_context
                    )

                    if is_valid:
                        # corrected_sql is only set if AI made changes
                        final_content = corrected_sql if corrected_sql else content
                        self._update_file_status(file_id, 'validated', corrected_content=final_content)
                        self.results['successful'] += 1

                        # Update all objects in this file as validated
                        self._update_objects_in_file(file_id, 'validated',
                                                     ai_corrected=bool(corrected_sql))

                        if corrected_sql:
                            logger.info(f"[Client {self.client_id}] Validated {filename} (AI-corrected)")
                        else:
                            logger.info(f"[Client {self.client_id}] Validated {filename} (no AI needed)")
                    else:
                        self._update_file_status(file_id, 'failed',
                                                corrected_content=corrected_sql,
                                                error_message=message)
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

        return self.results

    def run_full_migration(self, options=None):
        """
        Execute the complete one-click DDL migration workflow.

        :param dict options: Migration options
            - clean_slate: Drop existing tables before validation (default: False)
            - auto_create_ddl: Auto-create missing tables during validation (default: True)
            - object_types: List of object types to migrate (default: all supported)
        :return: Migration results
        :rtype: dict
        """
        if options is None:
            options = {}

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
            files = self.export_ddl(objects_by_type)
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
            elif self.results['successful'] > 0:
                self.results['status'] = 'partial'
                self._update_session_status('partial')
            else:
                self.results['status'] = 'failed'
                self._update_session_status('failed')

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
        return self.results

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

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
import logging
from datetime import datetime
from .db import get_db, execute_query, get_client_config, extract_ai_settings, ENCRYPTION_KEY
from .sql_processing import Ora2PgAICorrector

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
            output_dir='/app/output',
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
        """Retrieve the content of an exported file."""
        query = '''SELECT mf.filename, ms.export_directory
                   FROM migration_files mf
                   JOIN migration_sessions ms ON mf.session_id = ms.session_id
                   WHERE mf.file_id = ?'''
        cursor = execute_query(self.conn, query, (file_id,))
        file_info = cursor.fetchone()

        if not file_info:
            return None, "File not found"

        file_path = os.path.join(file_info['export_directory'], file_info['filename'])
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read(), None
        except Exception as e:
            return None, str(e)

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

            # Collect file info
            if result.get('files'):
                session_id = result.get('session_id')
                # Fetch file IDs from database
                query = '''SELECT file_id, filename FROM migration_files
                          WHERE session_id = ? ORDER BY file_id'''
                cursor = execute_query(self.conn, query, (session_id,))
                files = [dict(row) for row in cursor.fetchall()]
                all_files.extend(files)

        logger.info(f"[Client {self.client_id}] Exported {len(all_files)} files")
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

            # Get file content
            content, error = self._get_file_content(file_id)
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
            if validation_dsn:
                try:
                    is_valid, message, corrected_sql = self.corrector.validate_sql(
                        content,
                        validation_dsn,
                        clean_slate=validation_options.get('clean_slate', False),
                        auto_create_ddl=validation_options.get('auto_create_ddl', True)
                    )

                    if is_valid:
                        # corrected_sql is only set if AI made changes
                        final_content = corrected_sql if corrected_sql else content
                        self._update_file_status(file_id, 'validated', corrected_content=final_content)
                        self.results['successful'] += 1
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

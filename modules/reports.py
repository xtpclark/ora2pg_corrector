"""
Migration Reports Module - AsciiDoc report generation.

Generates detailed migration reports in AsciiDoc format for
documentation and stakeholder communication.
"""

import os
import logging
from datetime import datetime
from .db import execute_query

logger = logging.getLogger(__name__)


class MigrationReportGenerator:
    """
    Generates AsciiDoc migration reports for a client/session.
    """

    def __init__(self, conn, client_id, session_id=None):
        """
        Initialize the report generator.

        :param conn: Database connection
        :param int client_id: Client ID for the report
        :param int session_id: Optional specific session ID (defaults to latest)
        """
        self.conn = conn
        self.client_id = client_id
        self.session_id = session_id
        self.data = {}

    def gather_data(self):
        """
        Query database for all report data.

        :return: Dictionary of report data
        :rtype: dict
        """
        # Get client info
        query = 'SELECT client_name FROM clients WHERE client_id = ?'
        cursor = execute_query(self.conn, query, (self.client_id,))
        client = cursor.fetchone()
        self.data['client_name'] = client['client_name'] if client else f'Client {self.client_id}'

        # Get AI settings from config
        query = '''SELECT config_key, config_value FROM configs
                   WHERE client_id = ? AND config_key IN ('ai_provider', 'ai_model')'''
        cursor = execute_query(self.conn, query, (self.client_id,))
        ai_config = {row['config_key']: row['config_value'] for row in cursor.fetchall()}
        self.data['ai_provider'] = ai_config.get('ai_provider', 'Unknown')
        self.data['ai_model'] = ai_config.get('ai_model', 'Unknown')

        # Find the base TABLE session for this migration run
        if self.session_id:
            # If given a specific session, find its corresponding TABLE session
            # by looking for the closest TABLE session with ID <= given session
            query = '''SELECT session_id, session_name, export_directory, export_type,
                              workflow_status, created_at, rollback_generated_at
                       FROM migration_sessions
                       WHERE client_id = ? AND export_type = 'TABLE' AND session_id <= ?
                       ORDER BY session_id DESC LIMIT 1'''
            cursor = execute_query(self.conn, query, (self.client_id, self.session_id))
        else:
            # Get latest TABLE session
            query = '''SELECT session_id, session_name, export_directory, export_type,
                              workflow_status, created_at, rollback_generated_at
                       FROM migration_sessions
                       WHERE client_id = ? AND export_type = 'TABLE'
                       ORDER BY session_id DESC LIMIT 1'''
            cursor = execute_query(self.conn, query, (self.client_id,))

        main_session = cursor.fetchone()
        if not main_session:
            self.data['sessions'] = []
            self.data['files'] = []
            return self.data

        base_session_id = main_session['session_id']
        self.session_id = base_session_id
        self.data['main_session'] = dict(main_session)
        self.data['export_directory'] = main_session['export_directory']

        # Get all related sessions (TABLE, VIEW, PROCEDURE from same migration run)
        # These are sessions created after the TABLE session until the next TABLE session
        query = '''SELECT session_id, session_name, export_directory, export_type,
                          workflow_status, created_at
                   FROM migration_sessions
                   WHERE client_id = ? AND session_id >= ?
                   AND session_id < COALESCE(
                       (SELECT MIN(session_id) FROM migration_sessions
                        WHERE client_id = ? AND export_type = 'TABLE' AND session_id > ?),
                       999999999
                   )
                   ORDER BY session_id'''
        cursor = execute_query(self.conn, query, (self.client_id, base_session_id, self.client_id, base_session_id))
        self.data['sessions'] = [dict(row) for row in cursor.fetchall()]

        # Get all files across sessions
        session_ids = [s['session_id'] for s in self.data['sessions']]
        if session_ids:
            placeholders = ','.join(['?' for _ in session_ids])
            query = f'''SELECT mf.file_id, mf.session_id, mf.filename, mf.status,
                               mf.error_message, mf.last_modified, ms.export_type
                        FROM migration_files mf
                        JOIN migration_sessions ms ON mf.session_id = ms.session_id
                        WHERE mf.session_id IN ({placeholders})
                        ORDER BY ms.export_type, mf.filename'''
            cursor = execute_query(self.conn, query, tuple(session_ids))
            self.data['files'] = [dict(row) for row in cursor.fetchall()]

            # Get object-level stats from migration_objects table
            query = f'''SELECT object_type, status, COUNT(*) as count
                        FROM migration_objects
                        WHERE session_id IN ({placeholders})
                        GROUP BY object_type, status
                        ORDER BY object_type'''
            cursor = execute_query(self.conn, query, tuple(session_ids))
            object_stats = {}
            for row in cursor.fetchall():
                obj_type = row['object_type']
                if obj_type not in object_stats:
                    object_stats[obj_type] = {'total': 0, 'validated': 0, 'failed': 0, 'pending': 0}
                object_stats[obj_type][row['status']] = row['count']
                object_stats[obj_type]['total'] += row['count']
            self.data['object_stats'] = object_stats
        else:
            self.data['files'] = []
            self.data['object_stats'] = {}

        # Calculate statistics
        self._calculate_stats()

        return self.data

    def _calculate_stats(self):
        """Calculate summary statistics from file data."""
        files = self.data.get('files', [])

        # Overall stats
        self.data['total_files'] = len(files)
        self.data['successful'] = sum(1 for f in files if f['status'] == 'validated')
        self.data['failed'] = sum(1 for f in files if f['status'] == 'failed')
        self.data['skipped'] = sum(1 for f in files if f['status'] == 'skipped')

        if self.data['total_files'] > 0:
            self.data['success_rate'] = round(
                (self.data['successful'] / self.data['total_files']) * 100, 1
            )
        else:
            self.data['success_rate'] = 0

        # Stats by export type
        type_stats = {}
        for f in files:
            exp_type = f.get('export_type', 'UNKNOWN')
            if exp_type not in type_stats:
                type_stats[exp_type] = {'total': 0, 'success': 0, 'failed': 0}
            type_stats[exp_type]['total'] += 1
            if f['status'] == 'validated':
                type_stats[exp_type]['success'] += 1
            elif f['status'] == 'failed':
                type_stats[exp_type]['failed'] += 1

        self.data['type_stats'] = type_stats

        # Calculate duration if we have timestamps
        sessions = self.data.get('sessions', [])
        if sessions:
            # Find earliest created_at
            created_times = [s.get('created_at') for s in sessions if s.get('created_at')]
            if created_times:
                self.data['started_at'] = min(created_times)

    def generate_asciidoc(self):
        """
        Generate AsciiDoc formatted report.

        :return: AsciiDoc report as string
        :rtype: str
        """
        if not self.data:
            self.gather_data()

        lines = []

        # Document header
        lines.append(f"= Migration Report: {self.data.get('client_name', 'Unknown')}")
        lines.append(':toc:')
        lines.append(':icons: font')
        lines.append(':sectnums:')
        lines.append('')

        # Executive Summary
        lines.append('== Executive Summary')
        lines.append('')
        lines.append('[cols="1,2"]')
        lines.append('|===')

        # Status with emoji
        status = self.data.get('main_session', {}).get('workflow_status', 'unknown')
        status_icon = {'completed': 'pass:[&#10004;]', 'partial': 'pass:[&#9888;]', 'failed': 'pass:[&#10008;]'}.get(status, '')
        lines.append(f'|Status |{status_icon} {status.title()}')

        # Success rate
        success_rate = self.data.get('success_rate', 0)
        successful = self.data.get('successful', 0)
        total = self.data.get('total_files', 0)
        lines.append(f'|Success Rate |{success_rate}% ({successful}/{total} files)')

        # Started at
        started_at = self.data.get('started_at', 'Unknown')
        lines.append(f'|Started |{started_at}')

        # Export directory
        export_dir = self.data.get('export_directory', 'Unknown')
        lines.append(f'|Export Directory |`{export_dir}`')

        lines.append('|===')
        lines.append('')

        # Object Summary by Type (from migration_objects table)
        lines.append('== Object Summary')
        lines.append('')

        object_stats = self.data.get('object_stats', {})
        if object_stats:
            # Calculate totals
            total_objects = sum(s['total'] for s in object_stats.values())
            total_validated = sum(s.get('validated', 0) for s in object_stats.values())
            total_failed = sum(s.get('failed', 0) for s in object_stats.values())
            overall_rate = round((total_validated / total_objects) * 100, 1) if total_objects > 0 else 0

            lines.append(f'*Total: {total_validated}/{total_objects} objects validated ({overall_rate}%)*')
            lines.append('')
            lines.append('[cols="1,1,1,1,1"]')
            lines.append('|===')
            lines.append('|Object Type |Total |Validated |Failed |Rate')
            lines.append('')

            for obj_type, stats in sorted(object_stats.items()):
                validated = stats.get('validated', 0)
                failed = stats.get('failed', 0)
                rate = round((validated / stats['total']) * 100, 1) if stats['total'] > 0 else 0
                lines.append(f"|{obj_type} |{stats['total']} |{validated} |{failed} |{rate}%")

            lines.append('|===')
        else:
            # Fall back to file-level stats if no object data
            type_stats = self.data.get('type_stats', {})
            if type_stats:
                lines.append('[cols="1,1,1,1,1"]')
                lines.append('|===')
                lines.append('|File Type |Total |Success |Failed |Rate')
                lines.append('')

                for exp_type, stats in sorted(type_stats.items()):
                    rate = round((stats['success'] / stats['total']) * 100, 1) if stats['total'] > 0 else 0
                    lines.append(f"|{exp_type} |{stats['total']} |{stats['success']} |{stats['failed']} |{rate}%")

                lines.append('|===')
            else:
                lines.append('_No objects were processed._')
        lines.append('')

        # File Details
        lines.append('== File Details')
        lines.append('')

        files = self.data.get('files', [])
        if files:
            lines.append('[cols="3,1,1,3"]')
            lines.append('|===')
            lines.append('|Filename |Type |Status |Error')
            lines.append('')

            for f in files:
                status = f['status']
                status_icon = {'validated': 'pass:[&#10004;]', 'failed': 'pass:[&#10008;]', 'skipped': 'pass:[&#8212;]'}.get(status, '')
                error = f.get('error_message', '-') or '-'
                # Truncate long errors
                if len(error) > 50:
                    error = error[:47] + '...'
                exp_type = f.get('export_type', 'UNKNOWN')
                lines.append(f"|{f['filename']} |{exp_type} |{status_icon} {status} |{error}")

            lines.append('|===')
        else:
            lines.append('_No files were processed._')
        lines.append('')

        # Errors Section
        failed_files = [f for f in files if f['status'] == 'failed' and f.get('error_message')]
        if failed_files:
            lines.append('== Errors')
            lines.append('')

            for f in failed_files:
                lines.append(f"=== {f['filename']}")
                lines.append('')
                lines.append('----')
                lines.append(f.get('error_message', 'Unknown error'))
                lines.append('----')
                lines.append('')

        # Metadata
        lines.append('== Metadata')
        lines.append('')
        lines.append(f"* AI Provider: {self.data.get('ai_provider', 'Unknown')}")
        lines.append(f"* AI Model: {self.data.get('ai_model', 'Unknown')}")
        lines.append(f"* Client ID: {self.client_id}")
        lines.append(f"* Session ID: {self.session_id}")
        lines.append(f"* Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Rollback info
        rollback_time = self.data.get('main_session', {}).get('rollback_generated_at')
        if rollback_time:
            lines.append(f"* Rollback Script: Available (generated {rollback_time})")
        lines.append('')

        return '\n'.join(lines)

    def save_report(self, export_dir=None):
        """
        Save report to migration_report.adoc in session directory.

        :param str export_dir: Directory to save (uses session dir if not provided)
        :return: Path to saved file
        :rtype: str
        """
        if not export_dir:
            export_dir = self.data.get('export_directory')

        if not export_dir:
            raise ValueError("No export directory available")

        content = self.generate_asciidoc()
        report_path = os.path.join(export_dir, 'migration_report.adoc')

        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(content)

        logger.info(f"Migration report saved to: {report_path}")
        return report_path


def generate_migration_report(conn, client_id, session_id=None, save_to_file=True):
    """
    Convenience function to generate a migration report.

    :param conn: Database connection
    :param int client_id: Client ID
    :param int session_id: Optional session ID (defaults to latest)
    :param bool save_to_file: Whether to save to file
    :return: Tuple of (asciidoc_content, file_path or None)
    :rtype: tuple
    """
    generator = MigrationReportGenerator(conn, client_id, session_id)
    generator.gather_data()
    content = generator.generate_asciidoc()

    file_path = None
    if save_to_file and generator.data.get('export_directory'):
        file_path = generator.save_report()

    return content, file_path

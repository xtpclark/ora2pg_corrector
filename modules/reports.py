"""
Migration Reports Module - AsciiDoc report generation.

Generates detailed migration reports in AsciiDoc format for
documentation and stakeholder communication.
"""

import os
import re
import logging
from datetime import datetime
from .db import execute_query

logger = logging.getLogger(__name__)

# Oracle feature detection patterns with PostgreSQL recommendations
# Each entry: pattern (regex), feature name, severity, recommendation
ORACLE_FEATURE_PATTERNS = [
    # JSON/XML features
    {
        'pattern': r'\bJSON_TABLE\s*\(',
        'feature': 'JSON_TABLE',
        'severity': 'high',
        'description': 'Oracle JSON_TABLE function for extracting relational data from JSON',
        'recommendation': '''PostgreSQL does not support JSON_TABLE syntax. Convert to:
* `jsonb_array_elements()` for iterating arrays
* `jsonb_to_recordset()` for extracting multiple columns
* `LATERAL JOIN` with JSON path expressions
* `jsonb_path_query()` for SQL/JSON path queries (PG 12+)

Example conversion:
[source,sql]
----
-- Oracle:
SELECT * FROM table, JSON_TABLE(col, '$.items[*]' COLUMNS(name VARCHAR2 PATH '$.name'))

-- PostgreSQL:
SELECT t.*, elem->>'name' as name
FROM table t
CROSS JOIN LATERAL jsonb_array_elements(t.col->'items') AS elem
----'''
    },
    {
        'pattern': r'\bXMLTABLE\s*\(',
        'feature': 'XMLTABLE',
        'severity': 'medium',
        'description': 'Oracle XMLTABLE for querying XML data',
        'recommendation': '''PostgreSQL has XMLTABLE but syntax differs slightly.
* Column definitions use different syntax
* PASSING clause format varies
* Consider xpath() for simpler cases'''
    },
    # Hierarchical queries
    {
        'pattern': r'\bCONNECT\s+BY\b',
        'feature': 'CONNECT BY (Hierarchical Query)',
        'severity': 'high',
        'description': 'Oracle hierarchical query syntax for tree-structured data',
        'recommendation': '''Replace with recursive CTE (WITH RECURSIVE):
[source,sql]
----
-- Oracle:
SELECT * FROM emp START WITH mgr IS NULL CONNECT BY PRIOR emp_id = mgr

-- PostgreSQL:
WITH RECURSIVE emp_tree AS (
  SELECT * FROM emp WHERE mgr IS NULL
  UNION ALL
  SELECT e.* FROM emp e JOIN emp_tree t ON e.mgr = t.emp_id
)
SELECT * FROM emp_tree
----'''
    },
    {
        'pattern': r'\bSTART\s+WITH\b',
        'feature': 'START WITH (Hierarchical Query)',
        'severity': 'high',
        'description': 'Oracle hierarchical query starting condition',
        'recommendation': 'Part of CONNECT BY syntax. See CONNECT BY recommendation above.'
    },
    {
        'pattern': r'\bSYS_CONNECT_BY_PATH\b',
        'feature': 'SYS_CONNECT_BY_PATH',
        'severity': 'high',
        'description': 'Oracle function to build path in hierarchical queries',
        'recommendation': 'Use string concatenation in recursive CTE: `path || \'/\' || name`'
    },
    {
        'pattern': r'\bLEVEL\b(?!\s*\d)',
        'feature': 'LEVEL pseudo-column',
        'severity': 'medium',
        'description': 'Oracle LEVEL pseudo-column in hierarchical queries',
        'recommendation': 'Add explicit level counter in recursive CTE: `1 AS level` in base, `level + 1` in recursive part'
    },
    # Oracle-specific functions
    {
        'pattern': r'\bDECODE\s*\(',
        'feature': 'DECODE',
        'severity': 'low',
        'description': 'Oracle DECODE conditional function',
        'recommendation': 'Replace with CASE expression (usually auto-converted by Ora2Pg)'
    },
    {
        'pattern': r'\bNVL2\s*\(',
        'feature': 'NVL2',
        'severity': 'low',
        'description': 'Oracle NVL2 three-argument null function',
        'recommendation': 'Replace with CASE: `CASE WHEN x IS NOT NULL THEN y ELSE z END`'
    },
    {
        'pattern': r'\bROWNUM\b',
        'feature': 'ROWNUM',
        'severity': 'medium',
        'description': 'Oracle ROWNUM pseudo-column for row limiting',
        'recommendation': '''Replace with:
* `ROW_NUMBER() OVER ()` for row numbering
* `LIMIT n` for simple row limiting
* `FETCH FIRST n ROWS ONLY` (SQL:2008 standard)'''
    },
    {
        'pattern': r'\bROWID\b',
        'feature': 'ROWID',
        'severity': 'medium',
        'description': 'Oracle ROWID pseudo-column (physical row address)',
        'recommendation': '''PostgreSQL equivalent is `ctid` but semantics differ:
* ctid changes after VACUUM
* Better to use surrogate primary keys
* For deduplication, use `ctid` with DISTINCT ON'''
    },
    {
        'pattern': r'\(\+\)',
        'feature': 'Oracle (+) outer join',
        'severity': 'low',
        'description': 'Old Oracle outer join syntax',
        'recommendation': 'Replace with ANSI JOIN syntax: `LEFT JOIN`, `RIGHT JOIN` (usually auto-converted)'
    },
    {
        'pattern': r'\bMINUS\b',
        'feature': 'MINUS',
        'severity': 'low',
        'description': 'Oracle MINUS set operator',
        'recommendation': 'Replace with `EXCEPT` (usually auto-converted by Ora2Pg)'
    },
    {
        'pattern': r'\bLISTAGG\s*\(',
        'feature': 'LISTAGG',
        'severity': 'low',
        'description': 'Oracle string aggregation function',
        'recommendation': 'Replace with `STRING_AGG(column, delimiter ORDER BY ...)` (usually auto-converted)'
    },
    {
        'pattern': r'\bWM_CONCAT\s*\(',
        'feature': 'WM_CONCAT',
        'severity': 'low',
        'description': 'Oracle undocumented string aggregation',
        'recommendation': 'Replace with `STRING_AGG(column, \',\')`'
    },
    # PL/SQL packages
    {
        'pattern': r'\bDBMS_OUTPUT\.',
        'feature': 'DBMS_OUTPUT',
        'severity': 'medium',
        'description': 'Oracle debug output package',
        'recommendation': 'Replace with `RAISE NOTICE \'message\'` in PL/pgSQL'
    },
    {
        'pattern': r'\bDBMS_LOB\.',
        'feature': 'DBMS_LOB',
        'severity': 'high',
        'description': 'Oracle large object package',
        'recommendation': '''PostgreSQL large object handling:
* Use `lo_*` functions for large objects
* Or use BYTEA/TEXT with appropriate functions
* `DBMS_LOB.SUBSTR` → `SUBSTRING(column FROM start FOR length)`'''
    },
    {
        'pattern': r'\bUTL_FILE\.',
        'feature': 'UTL_FILE',
        'severity': 'high',
        'description': 'Oracle file I/O package',
        'recommendation': '''PostgreSQL alternatives:
* `COPY` command for bulk file operations
* `pg_read_file()` / `pg_write_file()` (superuser only)
* External scripts via PL/Python or PL/Perl'''
    },
    {
        'pattern': r'\bDBMS_SCHEDULER\.',
        'feature': 'DBMS_SCHEDULER',
        'severity': 'high',
        'description': 'Oracle job scheduling package',
        'recommendation': '''Use external schedulers:
* pg_cron extension
* pgAgent
* OS-level cron jobs
* Application-level scheduling'''
    },
    {
        'pattern': r'\bDBMS_SQL\.',
        'feature': 'DBMS_SQL',
        'severity': 'medium',
        'description': 'Oracle dynamic SQL package',
        'recommendation': 'Use `EXECUTE` in PL/pgSQL with format() for safe variable substitution'
    },
    # Oracle-specific syntax
    {
        'pattern': r'\bAS\s+OF\s+(TIMESTAMP|SCN)\b',
        'feature': 'Flashback Query',
        'severity': 'high',
        'description': 'Oracle flashback query for point-in-time data',
        'recommendation': '''No direct PostgreSQL equivalent. Alternatives:
* Implement temporal tables (system-versioned)
* Use audit/history tables
* pg_audit for change tracking
* Consider timescaledb for time-series data'''
    },
    {
        'pattern': r'\bMODEL\s+DIMENSION\b',
        'feature': 'MODEL clause',
        'severity': 'high',
        'description': 'Oracle MODEL clause for spreadsheet-like calculations',
        'recommendation': 'No direct equivalent. Use window functions, recursive CTEs, or application logic'
    },
    {
        'pattern': r'\bPIVOT\s*\(',
        'feature': 'PIVOT',
        'severity': 'medium',
        'description': 'Oracle PIVOT for row-to-column transformation',
        'recommendation': '''PostgreSQL alternatives:
* crosstab() from tablefunc extension
* FILTER clause with aggregates: `SUM(val) FILTER (WHERE category = \'X\')`
* CASE expressions in aggregate'''
    },
    {
        'pattern': r'\bUNPIVOT\s*\(',
        'feature': 'UNPIVOT',
        'severity': 'medium',
        'description': 'Oracle UNPIVOT for column-to-row transformation',
        'recommendation': 'Use `LATERAL` join with `VALUES` or `UNION ALL` queries'
    },
    # Data types and storage
    {
        'pattern': r'\bVIRTUAL\s+(?:COLUMN)?',
        'feature': 'Virtual Column',
        'severity': 'medium',
        'description': 'Oracle virtual (computed) column',
        'recommendation': 'Use `GENERATED ALWAYS AS (expression) STORED` in PostgreSQL 12+'
    },
    {
        'pattern': r'\bVARCHAR2\s*\(\s*\d+\s+BYTE\s*\)',
        'feature': 'VARCHAR2 BYTE semantics',
        'severity': 'low',
        'description': 'Oracle BYTE length semantics',
        'recommendation': 'PostgreSQL VARCHAR uses character semantics. Adjust length for multi-byte characters.'
    },
    {
        'pattern': r'\bINTERVAL\s+YEAR.*?TO\s+MONTH\b',
        'feature': 'INTERVAL YEAR TO MONTH',
        'severity': 'low',
        'description': 'Oracle year-month interval type',
        'recommendation': 'Use PostgreSQL `INTERVAL` with year/month components'
    },
    # Other Oracle-specific features
    {
        'pattern': r'\bSYS_GUID\s*\(\s*\)',
        'feature': 'SYS_GUID',
        'severity': 'low',
        'description': 'Oracle globally unique identifier function',
        'recommendation': 'Replace with `gen_random_uuid()` (PG 13+) or uuid-ossp extension'
    },
    {
        'pattern': r'\bAUTONOMOUS_TRANSACTION\b',
        'feature': 'Autonomous Transaction',
        'severity': 'high',
        'description': 'Oracle pragma for independent transactions',
        'recommendation': '''No direct equivalent. Alternatives:
* Use dblink to connect to same database
* Separate the logic into application layer
* Use background workers'''
    },
    {
        'pattern': r'\bBULK\s+COLLECT\b',
        'feature': 'BULK COLLECT',
        'severity': 'medium',
        'description': 'Oracle bulk data retrieval into collections',
        'recommendation': 'PL/pgSQL automatically handles arrays. Use `SELECT array_agg(col)` or cursor loops.'
    },
    {
        'pattern': r'\bFORALL\b',
        'feature': 'FORALL',
        'severity': 'medium',
        'description': 'Oracle bulk DML statement',
        'recommendation': 'Use set-based operations or `EXECUTE` with arrays in PL/pgSQL'
    },
    {
        'pattern': r'\bBLOB\b|\bCLOB\b|\bNCLOB\b|\bBFILE\b',
        'feature': 'LOB data types',
        'severity': 'medium',
        'description': 'Oracle large object types',
        'recommendation': '''PostgreSQL equivalents:
* BLOB → BYTEA or Large Objects (lo)
* CLOB → TEXT
* Check for empty export files (LOB columns may not export properly)'''
    },
    # Oracle 12c+ IDENTITY columns
    {
        'pattern': r'\bGENERATED\s+(?:ALWAYS|BY\s+DEFAULT)\s+(?:ON\s+NULL\s+)?AS\s+IDENTITY\b',
        'feature': 'IDENTITY Column (Oracle 12c+)',
        'severity': 'medium',
        'description': 'Oracle 12c+ auto-increment identity column',
        'recommendation': '''PostgreSQL supports IDENTITY columns (PG 10+):
* `GENERATED ALWAYS AS IDENTITY` - PostgreSQL equivalent works directly
* `GENERATED BY DEFAULT AS IDENTITY` - PostgreSQL equivalent works directly
* Oracle's `ON NULL` clause has no direct equivalent - use trigger if needed

NOTE: Ora2Pg with FILE_PER_TABLE mode may fail to export tables with IDENTITY columns
due to a path handling bug with AUTOINCREMENT files. If exports fail:
* Disable FILE_PER_TABLE temporarily for these tables
* Or export with FILE_PER_TABLE=0 and split manually
* Or use Oracle Data Pump for direct extraction'''
    },
]


def detect_oracle_features(sql_content, filename=None):
    """
    Scan SQL content for Oracle-specific features.

    :param str sql_content: SQL content to scan
    :param str filename: Optional filename for context
    :return: List of detected features with details
    :rtype: list
    """
    detected = []
    for feature_info in ORACLE_FEATURE_PATTERNS:
        pattern = feature_info['pattern']
        matches = re.findall(pattern, sql_content, re.IGNORECASE)
        if matches:
            detected.append({
                'feature': feature_info['feature'],
                'severity': feature_info['severity'],
                'description': feature_info['description'],
                'recommendation': feature_info['recommendation'],
                'occurrences': len(matches),
                'filename': filename
            })
    return detected


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

        # Find the base session for this migration run
        # DDL sessions contain all object types, so they don't need aggregation
        # TABLE sessions may have related VIEW/PROCEDURE sessions that follow
        if self.session_id:
            # If given a specific session, use it directly
            query = '''SELECT session_id, session_name, export_directory, export_type,
                              workflow_status, created_at, rollback_generated_at
                       FROM migration_sessions
                       WHERE session_id = ?'''
            cursor = execute_query(self.conn, query, (self.session_id,))
            main_session = cursor.fetchone()
        else:
            # Get latest DDL or TABLE session (prefer DDL as it's the newer format)
            query = '''SELECT session_id, session_name, export_directory, export_type,
                              workflow_status, created_at, rollback_generated_at
                       FROM migration_sessions
                       WHERE client_id = ? AND export_type IN ('DDL', 'TABLE')
                       ORDER BY session_id DESC LIMIT 1'''
            cursor = execute_query(self.conn, query, (self.client_id,))
            main_session = cursor.fetchone()

            # If no DDL/TABLE session, get the latest session of any type
            if not main_session:
                query = '''SELECT session_id, session_name, export_directory, export_type,
                                  workflow_status, created_at, rollback_generated_at
                           FROM migration_sessions
                           WHERE client_id = ?
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

        # DDL sessions are self-contained (all object types in one session)
        # TABLE sessions may have related VIEW/PROCEDURE sessions that follow
        export_type = main_session['export_type']

        if export_type == 'DDL':
            # DDL sessions are standalone - just use this session
            self.data['sessions'] = [dict(main_session)]
        else:
            # Legacy TABLE session - find related sessions until next TABLE/DDL session
            query = '''SELECT session_id, session_name, export_directory, export_type,
                              workflow_status, created_at
                       FROM migration_sessions
                       WHERE client_id = ? AND session_id >= ?
                       AND session_id < COALESCE(
                           (SELECT MIN(session_id) FROM migration_sessions
                            WHERE client_id = ? AND export_type IN ('TABLE', 'DDL') AND session_id > ?),
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

    def _detect_oracle_features(self):
        """
        Scan exported files for Oracle-specific features.

        :return: Dictionary with detected features and empty file warnings
        :rtype: dict
        """
        detected_features = {}  # feature -> list of files
        empty_files = []
        export_dir = self.data.get('export_directory')

        if not export_dir or not os.path.exists(export_dir):
            return {'features': {}, 'empty_files': []}

        # Scan all SQL files in the export directory
        for filename in os.listdir(export_dir):
            if not filename.endswith('.sql'):
                continue

            filepath = os.path.join(export_dir, filename)
            try:
                # Check for empty files (tables with LOB columns often export empty)
                file_size = os.path.getsize(filepath)
                if file_size == 0:
                    empty_files.append(filename)
                    continue

                with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()

                features = detect_oracle_features(content, filename)
                for feat in features:
                    feat_name = feat['feature']
                    if feat_name not in detected_features:
                        detected_features[feat_name] = {
                            'severity': feat['severity'],
                            'description': feat['description'],
                            'recommendation': feat['recommendation'],
                            'files': []
                        }
                    detected_features[feat_name]['files'].append({
                        'filename': filename,
                        'occurrences': feat['occurrences']
                    })

            except (IOError, OSError) as e:
                logger.warning(f"Could not read file {filepath}: {e}")

        self.data['oracle_features'] = detected_features
        self.data['empty_files'] = empty_files

        return {'features': detected_features, 'empty_files': empty_files}

    def _append_feature_section(self, lines, feat_name, feat_info):
        """
        Append a feature subsection to the report lines.

        :param list lines: List of report lines to append to
        :param str feat_name: Feature name
        :param dict feat_info: Feature info with description, recommendation, files
        """
        lines.append(f'==== {feat_name}')
        lines.append('')
        lines.append(f"_{feat_info['description']}_")
        lines.append('')

        # List affected files
        affected_files = feat_info.get('files', [])
        if affected_files:
            total_occurrences = sum(f['occurrences'] for f in affected_files)
            lines.append(f'*Found in {len(affected_files)} file(s) ({total_occurrences} occurrence(s)):*')
            lines.append('')
            for af in affected_files:
                lines.append(f"* `{af['filename']}` ({af['occurrences']} occurrence(s))")
            lines.append('')

        # Recommendation
        lines.append('*Recommendation:*')
        lines.append('')
        # Handle multi-line recommendations (preserve formatting)
        recommendation = feat_info.get('recommendation', 'No specific recommendation.')
        lines.append(recommendation)
        lines.append('')

    def generate_asciidoc(self):
        """
        Generate AsciiDoc formatted report.

        :return: AsciiDoc report as string
        :rtype: str
        """
        if not self.data:
            self.gather_data()

        # Detect Oracle-specific features in exported files
        self._detect_oracle_features()

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

        # Session name
        session_name = self.data.get('main_session', {}).get('session_name', f'Session {self.session_id}')
        lines.append(f'|Session |*{session_name}*')

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

        # Special Considerations Section
        oracle_features = self.data.get('oracle_features', {})
        empty_files = self.data.get('empty_files', [])

        if oracle_features or empty_files:
            lines.append('== Special Considerations')
            lines.append('')
            lines.append('The following Oracle-specific features were detected in the exported files. ')
            lines.append('These may require manual review or additional conversion effort.')
            lines.append('')

            # Empty files warning (likely LOB or IDENTITY columns)
            if empty_files:
                lines.append('=== Empty Export Files')
                lines.append('')
                lines.append('[WARNING]')
                lines.append('====')
                lines.append('The following files exported as empty (0 bytes). Common causes:')
                lines.append('')
                lines.append('* *IDENTITY columns (Oracle 12c+)*: Ora2Pg with FILE_PER_TABLE mode may fail ')
                lines.append('  due to a path handling bug with AUTOINCREMENT files')
                lines.append('* *LOB columns*: Tables with BLOB, CLOB, or JSON columns may not export ')
                lines.append('  properly with the current configuration')
                lines.append('* *Complex data types*: XMLTYPE, SDO_GEOMETRY, or custom object types')
                lines.append('')
                lines.append('*Files:*')
                for ef in sorted(empty_files):
                    lines.append(f'* `{ef}`')
                lines.append('')
                lines.append('*Recommendations:*')
                lines.append('')
                lines.append('* For IDENTITY columns: Set `FILE_PER_TABLE=0` temporarily or export these tables separately')
                lines.append('* For LOB columns: Export using Oracle Data Pump or manual SQL extraction')
                lines.append('* Check Ora2Pg logs for specific errors (e.g., `FATAL: Can\'t open AUTOINCREMENT_...`)')
                lines.append('====')
                lines.append('')

            # Sort features by severity (high -> medium -> low)
            severity_order = {'high': 0, 'medium': 1, 'low': 2}
            sorted_features = sorted(
                oracle_features.items(),
                key=lambda x: (severity_order.get(x[1]['severity'], 3), x[0])
            )

            # Group by severity for display
            high_sev = [(k, v) for k, v in sorted_features if v['severity'] == 'high']
            medium_sev = [(k, v) for k, v in sorted_features if v['severity'] == 'medium']
            low_sev = [(k, v) for k, v in sorted_features if v['severity'] == 'low']

            if high_sev:
                lines.append('=== High Priority Items')
                lines.append('')
                lines.append('These features have no direct PostgreSQL equivalent and require significant ')
                lines.append('manual conversion effort.')
                lines.append('')
                for feat_name, feat_info in high_sev:
                    self._append_feature_section(lines, feat_name, feat_info)

            if medium_sev:
                lines.append('=== Medium Priority Items')
                lines.append('')
                lines.append('These features require some conversion but have well-known PostgreSQL alternatives.')
                lines.append('')
                for feat_name, feat_info in medium_sev:
                    self._append_feature_section(lines, feat_name, feat_info)

            if low_sev:
                lines.append('=== Low Priority Items')
                lines.append('')
                lines.append('These features are usually auto-converted by Ora2Pg or have straightforward equivalents.')
                lines.append('')
                for feat_name, feat_info in low_sev:
                    self._append_feature_section(lines, feat_name, feat_info)

        # Metadata
        lines.append('== Metadata')
        lines.append('')
        session_name = self.data.get('main_session', {}).get('session_name', f'Session {self.session_id}')
        lines.append(f"* Session: {session_name} (ID: {self.session_id})")
        lines.append(f"* Client: {self.data.get('client_name', 'Unknown')} (ID: {self.client_id})")
        lines.append(f"* AI Provider: {self.data.get('ai_provider', 'Unknown')}")
        lines.append(f"* AI Model: {self.data.get('ai_model', 'Unknown')}")
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

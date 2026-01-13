import os
import subprocess
import re
import html
import logging
import requests
import tempfile
import shutil
import certifi
from cryptography.fernet import Fernet
import psycopg2
from psycopg2 import sql as psql
import json
from datetime import datetime
from .db import execute_query, is_postgres, insert_returning_id
from .constants import get_session_dir, mask_sensitive_config, calculate_ai_cost
from .oracle_preprocessing import preprocess_oracle_sql

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# PostgreSQL reserved words that must be quoted when used as identifiers
PG_RESERVED_WORDS = {
    'all', 'analyse', 'analyze', 'and', 'any', 'array', 'as', 'asc', 'asymmetric',
    'authorization', 'binary', 'both', 'case', 'cast', 'check', 'collate', 'collation',
    'column', 'concurrently', 'constraint', 'create', 'cross', 'current_catalog',
    'current_date', 'current_role', 'current_schema', 'current_time', 'current_timestamp',
    'current_user', 'default', 'deferrable', 'desc', 'distinct', 'do', 'else', 'end',
    'except', 'exists', 'extract', 'false', 'fetch', 'for', 'foreign', 'freeze', 'from',
    'full', 'grant', 'group', 'having', 'ilike', 'in', 'index', 'initially', 'inner',
    'insert', 'intersect', 'into', 'is', 'isnull', 'join', 'lateral', 'leading', 'left',
    'like', 'limit', 'localtime', 'localtimestamp', 'natural', 'not', 'notnull', 'null',
    'off', 'offset', 'on', 'only', 'or', 'order', 'outer', 'over', 'overlaps', 'partition',
    'placing', 'precision', 'primary', 'references', 'returning', 'right', 'select',
    'session_user', 'set', 'similar', 'some', 'symmetric', 'table', 'tablesample', 'then',
    'to', 'trailing', 'true', 'union', 'unique', 'update', 'user', 'using', 'values',
    'variadic', 'verbose', 'when', 'where', 'window', 'with'
}


def quote_reserved_words(sql):
    """
    Quote PostgreSQL reserved words used as identifiers (column/table names).

    This handles cases like:
    - Column definitions: limit bigint -> "limit" bigint
    - ALTER TABLE: ALTER COLUMN limit -> ALTER COLUMN "limit"

    :param str sql: SQL to process
    :return: SQL with reserved words quoted
    :rtype: str
    """
    # Pattern to match column definitions in CREATE TABLE
    # Matches: word followed by data type (bigint, varchar, etc.)
    def quote_column_def(match):
        col_name = match.group(1)
        rest = match.group(2)
        # Don't quote "WITH" when it's part of a timestamp type specifier (WITH TIME ZONE)
        # or other SQL keywords that commonly appear in type definitions
        # Note: rest only contains " time" (the captured type), so check for TIME or LOCAL
        if col_name.lower() == 'with' and re.match(r'\s+(?:time|local)\b', rest, re.IGNORECASE):
            return match.group(0)
        if col_name.lower() in PG_RESERVED_WORDS:
            return f'"{col_name}"{rest}'
        return match.group(0)

    # Pattern: word at start of column definition (after comma/paren, optional whitespace)
    # followed by a data type keyword
    col_def_pattern = r'(?<=[\(\,\s])(\b\w+\b)(\s+(?:bigint|smallint|integer|int|numeric|decimal|real|double|boolean|bool|char|varchar|text|bytea|timestamp|date|time|interval|uuid|json|jsonb|xml|array|serial|bigserial)\b)'
    sql = re.sub(col_def_pattern, quote_column_def, sql, flags=re.IGNORECASE)

    # Pattern for ALTER COLUMN statements
    def quote_alter_column(match):
        prefix = match.group(1)
        col_name = match.group(2)
        rest = match.group(3)
        if col_name.lower() in PG_RESERVED_WORDS:
            return f'{prefix}"{col_name}"{rest}'
        return match.group(0)

    alter_col_pattern = r'(ALTER\s+(?:TABLE\s+\w+\s+)?COLUMN\s+)(\b\w+\b)(\s)'
    sql = re.sub(alter_col_pattern, quote_alter_column, sql, flags=re.IGNORECASE)

    # Pattern for INDEX definitions: ON table (column_name)
    def quote_index_col(match):
        prefix = match.group(1)
        col_name = match.group(2)
        suffix = match.group(3)
        if col_name.lower() in PG_RESERVED_WORDS:
            return f'{prefix}"{col_name}"{suffix}'
        return match.group(0)

    # Match column names in index definitions (inside parentheses after ON table)
    index_col_pattern = r'(\bON\s+\w+\s*\([^)]*?)(\b\w+\b)(\s*(?:,|\)))'
    sql = re.sub(index_col_pattern, quote_index_col, sql, flags=re.IGNORECASE)

    # Pattern for PRIMARY KEY, UNIQUE, FOREIGN KEY column lists
    def quote_constraint_col(match):
        prefix = match.group(1)
        col_name = match.group(2)
        suffix = match.group(3)
        if col_name.lower() in PG_RESERVED_WORDS:
            return f'{prefix}"{col_name}"{suffix}'
        return match.group(0)

    # Match column names in PRIMARY KEY, UNIQUE, or FOREIGN KEY (column_list)
    constraint_col_pattern = r'(\b(?:PRIMARY\s+KEY|UNIQUE|FOREIGN\s+KEY)\s*\([^)]*?)(\b\w+\b)(\s*(?:,|\)))'
    sql = re.sub(constraint_col_pattern, quote_constraint_col, sql, flags=re.IGNORECASE)

    return sql


class Ora2PgAICorrector:
    """
    Handles the core logic for running Ora2Pg, correcting SQL using AI, 
    and validating the results against a PostgreSQL database.
    """
    def __init__(self, output_dir, ai_settings, encryption_key):
        """
        Initializes the Ora2PgAICorrector.

        :param str output_dir: The base directory for saving output files.
        :param dict ai_settings: Configuration for the AI service (endpoint, model, key).
        :param bytes encryption_key: The key used for encrypting and decrypting secrets.
        """
        self.ora2pg_path = 'ora2pg'
        self.sqlplus_path = 'sqlplus'
        self.output_dir = output_dir
        self.ai_settings = ai_settings
        self.encryption_key = encryption_key
        self.fernet = Fernet(encryption_key)

    def _validate_oracle_identifier(self, identifier, identifier_type="identifier"):
        """
        Validates Oracle identifiers to prevent SQL injection.
        Oracle identifiers can contain alphanumeric characters, underscore, dollar sign, and hash.
        They must start with a letter (or underscore/dollar in some cases).
        Maximum length is 128 characters (as of Oracle 12.2).
        
        :param str identifier: The identifier to validate
        :param str identifier_type: Type of identifier for error messages
        :return: The validated identifier
        :raises ValueError: If the identifier is invalid
        """
        if not identifier:
            raise ValueError(f"Oracle {identifier_type} cannot be empty")
        
        # Check length
        if len(identifier) > 128:
            raise ValueError(f"Oracle {identifier_type} exceeds maximum length of 128 characters")
        
        # Check for valid Oracle identifier pattern
        # Allows alphanumeric, underscore, dollar sign, and hash
        # Must start with letter or underscore (we'll be more permissive and allow $ and #)
        oracle_identifier_pattern = re.compile(r'^[A-Za-z_$#][A-Za-z0-9_$#]*$')
        
        if not oracle_identifier_pattern.match(identifier):
            raise ValueError(
                f"Invalid Oracle {identifier_type}: '{identifier}'. "
                f"Must contain only alphanumeric characters, underscore, dollar sign, or hash, "
                f"and start with a letter, underscore, dollar sign, or hash."
            )
        
        # Check for SQL injection attempts (additional safety)
        dangerous_patterns = [
            r';\s*--',  # Comment after semicolon
            r';\s*/',   # Start of multi-line comment
            r'union\s+select',  # UNION SELECT
            r'drop\s+',  # DROP statements
            r'delete\s+from',  # DELETE statements
            r'update\s+.*\s+set',  # UPDATE statements
            r'insert\s+into',  # INSERT statements
            r'exec\s*\(',  # EXEC calls
            r'execute\s+immediate',  # Dynamic SQL
        ]
        
        identifier_lower = identifier.lower()
        for pattern in dangerous_patterns:
            if re.search(pattern, identifier_lower):
                raise ValueError(f"Potential SQL injection detected in {identifier_type}: '{identifier}'")
        
        return identifier

    def _parse_dsn(self, dsn_string):
        """
        Parses a DBI-style DSN string into a dictionary.

        :param str dsn_string: The DSN string, e.g., 'dbi:Oracle:host=...;service_name=...'.
        :return: A dictionary of DSN components.
        :rtype: dict
        """
        if not dsn_string: return {}
        dsn_string = dsn_string.replace('dbi:Oracle:', '')
        pairs = dsn_string.split(';')
        dsn_dict = {}
        for pair in pairs:
            if '=' in pair:
                key, value = pair.split('=', 1)
                dsn_dict[key.lower()] = value
        return dsn_dict

    ### NEW CENTRAL HELPER METHOD ###
    def _build_sqlplus_connect_string(self, client_config):
        """
        Builds a SQL*Plus EZCONNECT connection string from client configuration.
        Handles both SERVICE_NAME and SID connection types.

        :param dict client_config: The client configuration dictionary.
        :return: A tuple containing the connect string and an error message (if any).
        :rtype: tuple(str | None, str | None)
        """
        dsn_string = client_config.get('oracle_dsn')
        user = client_config.get('oracle_user')
        password = client_config.get('oracle_pwd')

        if not all([dsn_string, user, password]):
            return None, "Oracle connection details (DSN, user, password) are incomplete."
        
        dsn_params = self._parse_dsn(dsn_string)
        host = dsn_params.get('host')
        port = dsn_params.get('port')
        
        if not all([host, port]):
            return None, "Oracle DSN must include host and port."

        # Check for SERVICE_NAME first, then fall back to SID
        service_name = dsn_params.get('service_name')
        sid = dsn_params.get('sid')

        if service_name:
            # Format for Service Name: user/pass@host:port/service_name
            connect_string = f"{user}/{password}@{host}:{port}/{service_name}"
            return connect_string, None
        elif sid:
            # Format for SID: user/pass@host:port:sid (note the colon)
            connect_string = f"{user}/{password}@{host}:{port}:{sid}"
            return connect_string, None
        else:
            return None, "Oracle DSN must include either 'service_name' or 'sid'."

    def _run_single_ora2pg_command(self, config, extra_args=None):
        """
        Internal helper to run a single ora2pg command with a given configuration.
        ...
        """
        config_content = ""
        if 'pg_version' not in config:
            config['pg_version'] = '13'
            logger.info("PG_VERSION not set, using default of 13.")

        for key, value in config.items():
            if key.startswith('ai_') or key == 'validation_pg_dsn' or value is None:
                continue
            if isinstance(value, bool):
                value = 1 if value else 0
            config_content += f"{key.upper()} {value}\n"
        
        config_path = None
        try:
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.conf', dir='/tmp') as temp_config:
                temp_config.write(config_content)
                config_path = temp_config.name
            
            logger.info(f"Generated temporary ora2pg config at: {config_path}")
            logger.info(f"Config content:\n{config_content}")

            command = [self.ora2pg_path, '-c', config_path] + (extra_args or [])
            logger.info(f"Executing command: {' '.join(command)}")

            # Run from /tmp to allow Ora2Pg to write temp files (temp_pass2_file.dat)
            # This is needed for VIEW and PROCEDURE exports which use multi-pass processing
            result = subprocess.run(command, capture_output=True, text=True, timeout=300, cwd='/tmp')

            logger.info(f"Raw stdout: {result.stdout[:500]}...")
            logger.info(f"Raw stderr: {result.stderr}")
            
            return result.stdout.strip(), result.stderr.strip(), result.returncode

        except Exception as e:
            logger.error(f"An unexpected error occurred during single Ora2Pg command execution: {e}", exc_info=True)
            return None, str(e), 1
        finally:
            if config_path and os.path.exists(config_path):
                os.remove(config_path)
                logger.info(f"Removed temporary config file: {config_path}")
                
    def _get_object_list(self, app_db_conn, client_config):
        """
        Fetches a complete list of objects from the Oracle schema and enriches it
        with information about whether Ora2Pg supports each object type for export.
        """
        try:
            from .db import execute_query
            cursor = execute_query(app_db_conn, "SELECT allowed_values FROM ora2pg_config_options WHERE option_name = 'TYPE'")
            type_row = cursor.fetchone()
            if not type_row:
                return None, "Ora2Pg supported types not found in the application database."
            
            supported_ora2pg_types = set(type_row['allowed_values'].split(','))
            logger.info(f"Found supported Ora2Pg types: {supported_ora2pg_types}")

        except Exception as e:
            logger.error(f"Failed to query supported Ora2Pg types: {e}")
            return None, f"Failed to query supported Ora2Pg types: {e}"

        logger.info(f"Fetching ALL objects from Oracle schema using SQL*Plus.")
        
        schema = client_config.get('schema')
        if not schema:
            return None, "Oracle schema is not configured."

        # SECURITY FIX: Validate schema name to prevent SQL injection
        try:
            validated_schema = self._validate_oracle_identifier(schema, "schema name")
        except ValueError as e:
            logger.error(f"Schema validation failed: {e}")
            return None, str(e)

        # Use the central helper method
        connect_string, error = self._build_sqlplus_connect_string(client_config)
        if error:
            return None, error

        # SQL query with proper formatting suppression and pipe delimiter
        sql_query = f"""
            SET PAGESIZE 0
            SET LINESIZE 32767
            SET FEEDBACK OFF
            SET HEADING OFF
            SET TRIMOUT ON
            SET TRIMSPOOL ON
            SET SERVEROUTPUT OFF
            SET VERIFY OFF
            SET ECHO OFF
            SET TAB OFF
            
            SELECT object_type || '|' || object_name 
            FROM all_objects 
            WHERE owner = '{validated_schema.upper()}' 
            ORDER BY object_type, object_name;
            
            EXIT;
        """
        
        command = [self.sqlplus_path, '-S', connect_string]
        
        try:
            process = subprocess.run(command, input=sql_query, capture_output=True, text=True, timeout=60)

            if process.returncode != 0:
                error_message = process.stderr.strip() or process.stdout.strip()
                logger.error(f"SQL*Plus failed with exit code {process.returncode}: {error_message}")
                return None, f"SQL*Plus connection failed: {error_message}"
            
            discovered_objects_raw = [line.strip() for line in process.stdout.strip().split('\n') if line.strip()]

            enriched_objects = []
            for line in discovered_objects_raw:
                if '|' in line:
                    obj_type, obj_name = line.split('|', 1)
                    is_supported = obj_type.strip() in supported_ora2pg_types
                    enriched_objects.append({
                        'type': obj_type.strip(),
                        'name': obj_name.strip(),
                        'supported': is_supported
                    })

            logger.info(f"Discovered {len(enriched_objects)} total objects in schema '{validated_schema}'.")
            return enriched_objects, None

        except subprocess.TimeoutExpired:
            return None, "SQL*Plus connection timed out."
        except Exception as e:
            logger.error(f"An unexpected error occurred during SQL*Plus execution: {e}", exc_info=True)
            return None, f"An unexpected error occurred: {str(e)}"
            
    def get_oracle_ddl(self, client_config, object_type, object_name, pretty=False):
        """
        Extracts the original Oracle DDL for a single object using SQL*Plus.
        
        :param dict client_config: Client configuration
        :param str object_type: Type of object (TABLE, VIEW, etc.)
        :param str object_name: Name of the object
        :param bool pretty: If True, returns cleaned DDL without storage clauses
        """
        logger.info(f"Fetching Oracle DDL for {object_type} {object_name} (pretty={pretty}).")
    
        schema = client_config.get('schema')
        if not schema:
            return None, "Oracle schema is not configured."
    
        # SECURITY FIX: Validate all identifiers to prevent SQL injection
        try:
            validated_schema = self._validate_oracle_identifier(schema, "schema name")
            validated_object_type = self._validate_oracle_identifier(object_type, "object type")
            validated_object_name = self._validate_oracle_identifier(object_name, "object name")
        except ValueError as e:
            logger.error(f"Identifier validation failed: {e}")
            return None, str(e)
    
        # Use the central helper method
        connect_string, error = self._build_sqlplus_connect_string(client_config)
        if error:
            return None, error
        
        # Build the SQL query with optional pretty formatting
        if pretty:
            sql_query = f"""
                SET LONG 2000000
                SET PAGESIZE 0
                SET LINESIZE 32767
                SET HEADING OFF
                SET FEEDBACK OFF
                SET VERIFY OFF
                SET ECHO OFF
                SET TERMOUT OFF
                SET TRIMOUT ON
                SET TRIMSPOOL ON
                
                BEGIN
                    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', FALSE);
                    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'TABLESPACE', FALSE);
                    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', FALSE);
                    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', TRUE);
                    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'PRETTY', TRUE);
                END;
                /
                
                SELECT DBMS_METADATA.GET_DDL('{validated_object_type.upper()}', '{validated_object_name.upper()}', '{validated_schema.upper()}') FROM DUAL;
                EXIT;
            """
        else:
            sql_query = f"""
                SET LONG 2000000
                SET PAGESIZE 0
                SET LINESIZE 32767
                SET HEADING OFF
                SET FEEDBACK OFF
                SET VERIFY OFF
                SET ECHO OFF
                SET TERMOUT OFF
                SET TRIMOUT ON
                SET TRIMSPOOL ON
                
                SELECT DBMS_METADATA.GET_DDL('{validated_object_type.upper()}', '{validated_object_name.upper()}', '{validated_schema.upper()}') FROM DUAL;
                EXIT;
            """
        
        command = [self.sqlplus_path, '-S', connect_string]
        
        try:
            process = subprocess.run(command, input=sql_query, capture_output=True, text=True, timeout=60)
    
            if process.returncode != 0:
                error_message = process.stderr.strip() or process.stdout.strip()
                logger.error(f"SQL*Plus DDL fetch failed: {error_message}")
                return None, f"SQL*Plus failed: {error_message}"
            
            ddl = process.stdout.strip()
            if not ddl or "ORA-" in ddl:
                return None, f"Could not retrieve DDL for {object_name}. Reason: {ddl}"
    
            logger.info(f"Successfully fetched DDL for {object_name}.")
            return ddl, None
    
        except subprocess.TimeoutExpired:
            return None, "SQL*Plus DDL fetch timed out."
        except Exception as e:
            logger.error(f"Unexpected error during DDL fetch: {e}", exc_info=True)
            return None, str(e)

    def run_ora2pg_export(self, client_id, db_conn, client_config, extra_args=None, session_name=None, existing_session_id=None):
        """
        Manages the full Ora2Pg export process, including session creation and file persistence.

        :param session_name: Optional friendly name for the session (defaults to timestamp)
        :param existing_session_id: Optional session_id to add files to an existing session
        """
        is_report = extra_args and any(arg in extra_args for arg in ['SHOW_REPORT', 'SHOW_VERSION'])
        if is_report:
            stdout, stderr, returncode = self._run_single_ora2pg_command(client_config, extra_args)
            if returncode != 0: return {}, stderr
            return {'sql_output': stdout}, None

        try:
            export_type = client_config.get('type', 'TABLE').upper()

            # Use existing session or create a new one
            if existing_session_id:
                session_id = existing_session_id
                # Get the existing session's export directory
                cursor = execute_query(db_conn, 'SELECT export_directory FROM migration_sessions WHERE session_id = ?', (session_id,))
                row = cursor.fetchone()
                if row:
                    persistent_export_dir = row[0]
                else:
                    return {}, f"Session {session_id} not found"
                logger.info(f"Adding to existing session {session_id} at {persistent_export_dir}")
            else:
                if not session_name:
                    session_name = f"Export - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

                # Capture config snapshot with sensitive values masked
                config_snapshot = json.dumps(mask_sensitive_config(client_config))
                ai_model = client_config.get('ai_model', '')

                session_id = insert_returning_id(
                    db_conn, 'migration_sessions',
                    ('client_id', 'session_name', 'export_directory', 'export_type', 'config_snapshot', 'ai_model'),
                    (client_id, session_name, "pending", export_type, config_snapshot, ai_model),
                    'session_id'
                )

                persistent_export_dir = get_session_dir(client_id, session_id)
                os.makedirs(persistent_export_dir, exist_ok=True)

                execute_query(db_conn, 'UPDATE migration_sessions SET export_directory = ? WHERE session_id = ?', (persistent_export_dir, session_id))

                db_conn.commit()
                logger.info(f"Created persistent session {session_id} at {persistent_export_dir}")

            file_per_table = str(client_config.get('file_per_table', '0')) in ['true', 'True', '1']
            run_config = client_config.copy()
            
            if file_per_table and export_type == 'TABLE' and 'ALLOW' in run_config:
                logger.info("Executing multi-file DDL export strategy (looping).")
                generated_files = []
                tables = [t.strip() for t in run_config['ALLOW'].split(',')]
                total_tables = len(tables)

                # Update session status to exporting now that we have a session
                execute_query(db_conn,
                    'UPDATE migration_sessions SET workflow_status = ?, current_phase = ?, total_count = ? WHERE session_id = ?',
                    ('exporting', 'export', total_tables, session_id))
                db_conn.commit()

                for idx, table_name in enumerate(tables):
                    # Update progress for each table being exported
                    execute_query(db_conn,
                        'UPDATE migration_sessions SET processed_count = ?, current_file = ? WHERE session_id = ?',
                        (idx, f"Exporting {table_name}...", session_id))
                    db_conn.commit()

                    single_table_config = run_config.copy()
                    single_table_config['ALLOW'] = table_name
                    single_table_config['OUTPUT'] = os.path.join(persistent_export_dir, f"{table_name}.sql")
                    single_table_config.pop('OUTPUT_DIR', None)

                    _, error, returncode = self._run_single_ora2pg_command(single_table_config)
                    if returncode == 0:
                        generated_files.append(f"{table_name}.sql")
                    else:
                        logger.warning(f"Skipping table {table_name} for session {session_id} due to export error: {error}")

                # Mark export phase complete
                execute_query(db_conn,
                    'UPDATE migration_sessions SET processed_count = ?, current_file = ? WHERE session_id = ?',
                    (total_tables, 'Export complete', session_id))
                db_conn.commit()
            else:
                logger.info("Executing single-command export strategy.")
                # Always use OUTPUT_DIR + OUTPUT separately to avoid Ora2Pg path bugs
                # with IDENTITY columns (AUTOINCREMENT file path construction)
                run_config['OUTPUT_DIR'] = persistent_export_dir
                run_config['OUTPUT'] = f"output_{export_type.lower()}.sql"

                # Get list of files BEFORE export to detect new files
                files_before = set(os.listdir(persistent_export_dir)) if os.path.exists(persistent_export_dir) else set()

                _, stderr, returncode = self._run_single_ora2pg_command(run_config)

                if returncode != 0:
                    # Only rollback if this is a new session (not adding to existing)
                    if not existing_session_id:
                        logger.error(f"Ora2Pg command failed. Rolling back session {session_id}.")
                        execute_query(db_conn, 'DELETE FROM migration_sessions WHERE session_id = ?', (session_id,))
                        db_conn.commit()
                        shutil.rmtree(persistent_export_dir)
                    return {}, stderr

                # Track ALL new .sql files created by this export (ora2pg may create multiple files)
                # e.g., for views: output_view.sql AND EMP_DETAILS_VIEW_output_view.sql
                files_after = set(os.listdir(persistent_export_dir))
                new_files = files_after - files_before
                generated_files = [f for f in new_files if f.endswith('.sql')]
                logger.info(f"Export created {len(generated_files)} file(s): {generated_files}")

            for filename in generated_files:
                # Avoid duplicate entries - only insert if file doesn't already exist for this session
                cursor = execute_query(db_conn,
                    'SELECT file_id FROM migration_files WHERE session_id = ? AND filename = ?',
                    (session_id, filename))
                if not cursor.fetchone():
                    execute_query(db_conn, 'INSERT INTO migration_files (session_id, filename) VALUES (?, ?)', (session_id, filename))
            db_conn.commit()
            
            if not file_per_table and len(generated_files) == 1:
                with open(os.path.join(persistent_export_dir, generated_files[0]), 'r', encoding='utf-8') as f:
                    sql_output = f.read()
                return {'sql_output': sql_output, 'session_id': session_id, 'files': generated_files}, None
            else:
                return {'files': generated_files, 'session_id': session_id, 'directory': persistent_export_dir}, None

        except Exception as e:
            logger.error(f"An unexpected error occurred during Ora2Pg persistence execution: {e}", exc_info=True)
            return {}, f"An unexpected error occurred: {str(e)}"

    def _strip_psql_metacommands(self, sql):
        """
        Remove psql-specific metacommands that can't be executed via psycopg2.

        Ora2Pg exports include psql commands like:
        - \\set ON_ERROR_STOP ON
        - \\i filename
        - \\copy

        These must be stripped for validation via psycopg2.
        """
        lines = sql.split('\n')
        cleaned_lines = []
        for line in lines:
            stripped = line.strip()
            # Skip psql metacommands (lines starting with backslash)
            if stripped.startswith('\\'):
                continue
            cleaned_lines.append(line)
        return '\n'.join(cleaned_lines)

    def _extract_table_names(self, sql):
        """
        Extracts table names from a SQL query (FROM/JOIN clauses), ignoring CTEs.
        Used for auto_create_ddl to find referenced tables.
        """
        cte_pattern = re.compile(r'\bWITH\s+(?:RECURSIVE\s+)?([\w\s,]+)\bAS', re.IGNORECASE | re.DOTALL)
        cte_match = cte_pattern.search(sql)
        cte_names = set()
        if cte_match:
            cte_definitions = cte_match.group(1)
            for part in cte_definitions.split(','):
                name_match = re.search(r'(\w+)\s*\(?.*', part.strip())
                if name_match:
                    cte_names.add(name_match.group(1).lower())

        table_pattern = re.compile(
            r'\b(?:FROM|JOIN)\s+([\w\.]+)[\s\w]*?(?:\s+AS\s+[\w]+)?',
            re.IGNORECASE | re.MULTILINE
        )
        matches = table_pattern.findall(sql)

        table_names = {name for name in matches if name.lower() not in cte_names}
        logger.info(f"Extracted referenced tables: {table_names} (ignoring CTEs: {cte_names})")
        return table_names

    def _split_fk_constraints(self, sql):
        """
        Splits SQL into non-FK statements and FK constraint statements.
        FK constraints need to be deferred when there are circular dependencies.

        Returns tuple: (main_sql, fk_statements)
        - main_sql: SQL with FK constraints removed
        - fk_statements: List of FK constraint statements
        """
        # Pattern to match ALTER TABLE ADD CONSTRAINT ... FOREIGN KEY statements
        fk_pattern = re.compile(
            r'ALTER\s+TABLE\s+[\w\.]+\s+ADD\s+CONSTRAINT\s+[\w]+\s+FOREIGN\s+KEY[^;]+;',
            re.IGNORECASE | re.DOTALL
        )

        fk_statements = fk_pattern.findall(sql)
        main_sql = fk_pattern.sub('', sql)

        # Clean up any extra whitespace
        main_sql = re.sub(r'\n\s*\n', '\n\n', main_sql)

        if fk_statements:
            logger.info(f"Split out {len(fk_statements)} FK constraint(s) for deferred execution")

        return main_sql.strip(), fk_statements

    def _extract_created_objects(self, sql):
        """
        Extracts objects being CREATED from DDL statements.
        Used for clean_slate to drop only the object being created, not referenced tables.

        Returns list of tuples: [(object_type, object_name), ...]
        """
        objects = []

        # Patterns for different CREATE statements
        patterns = [
            # CREATE TABLE [IF NOT EXISTS] [schema.]name
            (r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:[\w]+\.)?([\w]+)', 'TABLE'),
            # CREATE [OR REPLACE] VIEW [schema.]name
            (r'CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(?:[\w]+\.)?([\w]+)', 'VIEW'),
            # CREATE [OR REPLACE] [MATERIALIZED] VIEW
            (r'CREATE\s+(?:OR\s+REPLACE\s+)?MATERIALIZED\s+VIEW\s+(?:[\w]+\.)?([\w]+)', 'MATERIALIZED VIEW'),
            # CREATE [OR REPLACE] FUNCTION [schema.]name
            (r'CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(?:[\w]+\.)?([\w]+)', 'FUNCTION'),
            # CREATE [OR REPLACE] PROCEDURE [schema.]name
            (r'CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+(?:[\w]+\.)?([\w]+)', 'PROCEDURE'),
            # CREATE [UNIQUE] INDEX [CONCURRENTLY] [IF NOT EXISTS] name
            (r'CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?([\w]+)', 'INDEX'),
            # CREATE SEQUENCE [IF NOT EXISTS] [schema.]name
            (r'CREATE\s+SEQUENCE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:[\w]+\.)?([\w]+)', 'SEQUENCE'),
            # CREATE TYPE [schema.]name
            (r'CREATE\s+TYPE\s+(?:[\w]+\.)?([\w]+)', 'TYPE'),
            # CREATE [OR REPLACE] TRIGGER name
            (r'CREATE\s+(?:OR\s+REPLACE\s+)?TRIGGER\s+([\w]+)', 'TRIGGER'),
        ]

        for pattern, obj_type in patterns:
            for match in re.finditer(pattern, sql, re.IGNORECASE):
                obj_name = match.group(1)
                objects.append((obj_type, obj_name))

        if objects:
            logger.info(f"Extracted objects being created: {objects}")

        return objects

    def ai_correct_sql(self, sql, source_dialect='oracle'):
        """
        Sends SQL code to an AI model for conversion to PostgreSQL.
        
        :param str sql: The SQL code to convert
        :param str source_dialect: The source SQL dialect (oracle, mysql, sqlserver, postgres, generic)
        :return: Tuple of (corrected_sql, metrics)
        :rtype: tuple
        """
        if not sql:
            return sql, {'status': 'no_content', 'tokens_used': 0, 'input_tokens': 0, 'output_tokens': 0}
        
        # Map dialect values to readable names
        dialect_names = {
            'oracle': 'Oracle',
            'mysql': 'MySQL',
            'sqlserver': 'SQL Server',
            'postgres': 'PostgreSQL',
            'generic': 'Generic SQL'
        }
        
        source_name = dialect_names.get(source_dialect.lower(), 'Oracle')
        
        # If source is already PostgreSQL, just do cleanup/optimization
        if source_dialect.lower() == 'postgres':
            system_instruction = "You are a PostgreSQL expert. Review and optimize PostgreSQL code for best practices."
            full_prompt = f"""Review this PostgreSQL code and provide an optimized version if improvements are needed. If the code is already optimal, return it unchanged. Provide only the SQL code.
    
    PostgreSQL SQL:
    ```sql
    {sql}
    ```"""
        else:
            # For other dialects, do full conversion
            system_instruction = f"You are an expert in database migrations. Convert {source_name} SQL to PostgreSQL, replacing {source_name}-specific constructs with PostgreSQL equivalents. Handle data types, functions, syntax, and PL/SQL to PL/pgSQL conversions. Output only valid PostgreSQL SQL that can be executed directly."
            full_prompt = f"""Convert this {source_name} SQL to PostgreSQL-compatible SQL. Provide only the converted SQL code with no explanations or markdown formatting.

IMPORTANT REQUIREMENTS:
1. Output only pure PostgreSQL SQL - no psql metacommands (lines starting with \\)
2. Remove any lines like: \\set, \\i, \\copy, \\encoding, etc.
3. Keep valid SQL commands like: SET client_encoding, CREATE TABLE, etc.
4. Convert Oracle data types:
   - NUMBER→NUMERIC, VARCHAR2→VARCHAR, NVARCHAR2→VARCHAR
   - CLOB→TEXT, NCLOB→TEXT, BLOB→BYTEA, RAW→BYTEA
   - LONG→TEXT, LONG RAW→BYTEA
   - TIMESTAMP WITH LOCAL TIME ZONE→TIMESTAMPTZ or TIMESTAMP WITH TIME ZONE
   - TIMESTAMP(n) WITH LOCAL TIME ZONE→TIMESTAMP(n) WITH TIME ZONE
5. Convert Oracle functions: NVL→COALESCE, SYSDATE→CURRENT_TIMESTAMP
   - NVL2(expr,val1,val2)→CASE WHEN expr IS NOT NULL THEN val1 ELSE val2 END
6. Convert Oracle TYPE definitions:
   - CREATE TYPE name AS OBJECT (...)→CREATE TYPE name AS (...)
   - VARRAY(n) OF type→type[] (array syntax)
   - CREATE TYPE name AS VARRAY(n) OF type→CREATE DOMAIN name AS type[]
7. For PL/SQL: Convert to PL/pgSQL (CREATE OR REPLACE FUNCTION/PROCEDURE)
8. CRITICAL: Quote PostgreSQL reserved words used as identifiers with double quotes. Reserved words include:
   ALL, AND, ANY, ARRAY, AS, ASC, AUTHORIZATION, BOTH, CASE, CAST, CHECK, COLLATE, COLUMN,
   CONCURRENTLY, CONSTRAINT, CREATE, CROSS, CURRENT, DEFAULT, DEFERRABLE, DESC, DISTINCT, DO,
   ELSE, END, EXCEPT, EXISTS, EXTRACT, FALSE, FETCH, FOR, FOREIGN, FROM, FULL, GRANT, GROUP,
   HAVING, ILIKE, IN, INDEX, INNER, INSERT, INTERSECT, INTO, IS, ISNULL, JOIN, LEADING, LEFT,
   LIKE, LIMIT, LOCALTIME, LOCALTIMESTAMP, NATURAL, NOT, NOTNULL, NULL, OFF, OFFSET, ON, ONLY,
   OR, ORDER, OUTER, OVER, PARTITION, PRECISION, PRIMARY, REFERENCES, RETURNING, RIGHT, SELECT,
   SESSION_USER, SET, SOME, TABLE, THEN, TO, TRAILING, TRUE, UNION, UNIQUE, UPDATE, USER, USING,
   VALUES, WHEN, WHERE, WINDOW, WITH
   Example: A column named "limit" must be quoted as "limit" in CREATE TABLE and all references.

Original {source_name} SQL:
```sql
{sql}
```"""
        
        try:
            return self._make_ai_call(system_instruction, full_prompt)
        except Exception as e:
            logger.error(f"AI SQL conversion from {source_name} failed: {e}", exc_info=False)
            return sql, {'status': 'error', 'error_message': str(e), 'tokens_used': 0, 'input_tokens': 0, 'output_tokens': 0}


    def _get_ddl_from_ai(self, failed_sql, error_message, object_name):
        """
        Asks the AI to generate a DDL statement for a missing object.

        :return: Tuple of (ddl_sql, metrics) where metrics contains token counts
        """
        system_instruction = "You are a PostgreSQL expert. Your task is to generate the necessary DDL to resolve a missing object error."

        full_prompt = f"""The following PostgreSQL query failed with the error: `{error_message}`.
The missing object is named '{object_name}'. Please generate the necessary CREATE TABLE or CREATE TYPE statement for the '{object_name}' object AND ONLY THAT OBJECT.
Infer column names and reasonable data types from the query context.
Provide only the raw DDL SQL code, with no explanations or markdown.
Query:
```sql
{failed_sql}
```"""
        try:
            ddl_sql, metrics = self._make_ai_call(system_instruction, full_prompt)
            return ddl_sql, metrics
        except Exception as e:
            logger.error(f"AI DDL generation failed for object '{object_name}': {e}", exc_info=False)
            return None, {'input_tokens': 0, 'output_tokens': 0}

    def _get_type_ddl_from_ai(self, failed_sql, error_message, type_name):
        """
        Asks the AI to generate a CREATE TYPE statement for a missing PostgreSQL type.

        This handles Oracle composite types (TYPE AS OBJECT) and array types (VARRAY)
        that need to be converted to PostgreSQL equivalents.

        :return: Tuple of (ddl_sql, metrics) where metrics contains token counts
        """
        system_instruction = """You are a PostgreSQL expert specializing in Oracle-to-PostgreSQL migration.
Your task is to generate CREATE TYPE statements for missing PostgreSQL composite types.

Common Oracle-to-PostgreSQL type conversions:
- Oracle TYPE AS OBJECT -> PostgreSQL CREATE TYPE AS (composite type)
- Oracle VARRAY -> PostgreSQL array type (type[]) or CREATE DOMAIN
- VARCHAR2 -> VARCHAR
- NUMBER -> NUMERIC
- CLOB -> TEXT
- BLOB -> BYTEA
- DATE -> TIMESTAMP (Oracle DATE includes time)"""

        full_prompt = f"""The following PostgreSQL query failed with the error: `{error_message}`.
The missing type is named '{type_name}'.

Please generate the necessary CREATE TYPE statement for '{type_name}'.
Infer the type structure (fields and their data types) from the query context.

Guidelines:
1. Use PostgreSQL composite type syntax: CREATE TYPE name AS (field1 type1, field2 type2, ...);
2. Convert Oracle data types to PostgreSQL equivalents
3. If the type appears to be an array/collection type, use CREATE DOMAIN name AS element_type[];
4. Provide only the raw DDL SQL code, with no explanations or markdown

Query:
```sql
{failed_sql}
```"""
        try:
            ddl_sql, metrics = self._make_ai_call(system_instruction, full_prompt)
            return ddl_sql, metrics
        except Exception as e:
            logger.error(f"AI TYPE DDL generation failed for type '{type_name}': {e}", exc_info=False)
            return None, {'input_tokens': 0, 'output_tokens': 0}

    def _get_consolidated_ddl_from_ai(self, sql_query, missing_tables):
        """
        Asks the AI to generate all necessary DDL for a list of missing tables.
        """
        system_instruction = "You are a PostgreSQL expert. Your task is to generate all necessary DDL to satisfy a query."
        table_list = ", ".join(missing_tables)
        
        full_prompt = f"""The following PostgreSQL query needs these tables to exist: `{table_list}`.
Please generate all necessary `CREATE TABLE` statements for these missing tables.
Infer columns, data types, and relationships (like foreign keys) from the full query's context.
Provide only the raw DDL SQL code, with no explanations or markdown.

Query:
```sql
{sql_query}
```"""
        try:
            ddl_sql, _ = self._make_ai_call(system_instruction, full_prompt)
            return ddl_sql
        except Exception as e:
            logger.error(f"AI consolidated DDL generation failed for tables '{table_list}': {e}", exc_info=False)
            return None

    def _get_query_fix_from_ai(self, failed_sql, error_message):
        """
        Asks the AI to fix a SQL query based on an error message.

        :return: Tuple of (fixed_sql, metrics) where metrics contains token counts
        """
        system_instruction = "You are a PostgreSQL expert. Your task is to correct a SQL query that failed validation. Output only valid PostgreSQL SQL that can be executed directly via a database driver (not psql CLI)."
        full_prompt = f"""The following PostgreSQL query failed with the error: `{error_message}`.

Please correct the query to resolve the issue. Provide only the corrected, complete SQL query with no explanations.

IMPORTANT:
- Remove any psql metacommands (lines starting with \\) like \\set, \\i, \\copy
- Keep valid SQL statements like SET, CREATE, INSERT, etc.
- Output pure PostgreSQL SQL only
- CRITICAL: If the error mentions "syntax error" near a word, check if it's a PostgreSQL reserved word.
  Reserved words used as column/table names MUST be quoted with double quotes.
  Common reserved words: ALL, AND, ANY, ARRAY, AS, ASC, BOTH, CASE, CAST, CHECK, COLLATE, COLUMN,
  CONSTRAINT, CREATE, CROSS, CURRENT, DEFAULT, DESC, DISTINCT, DO, ELSE, END, EXCEPT, EXISTS,
  FALSE, FETCH, FOR, FOREIGN, FROM, FULL, GRANT, GROUP, HAVING, IN, INDEX, INNER, INSERT,
  INTERSECT, INTO, IS, JOIN, LEADING, LEFT, LIKE, LIMIT, NATURAL, NOT, NULL, OFFSET, ON, ONLY,
  OR, ORDER, OUTER, OVER, PARTITION, PRIMARY, REFERENCES, RETURNING, RIGHT, SELECT, SET, SOME,
  TABLE, THEN, TO, TRAILING, TRUE, UNION, UNIQUE, UPDATE, USER, USING, VALUES, WHEN, WHERE, WITH
  Example: A column named "limit" must be quoted as "limit" everywhere it appears.

Failed Query:
```sql
{failed_sql}
```"""
        try:
            fixed_sql, metrics = self._make_ai_call(system_instruction, full_prompt)
            return fixed_sql, metrics
        except Exception as e:
            logger.error(f"AI query fix generation failed: {e}", exc_info=False)
            return None, {'input_tokens': 0, 'output_tokens': 0}

    # --- DDL Cache Methods ---

    def _check_ddl_cache(self, db_conn, client_id, object_name):
        """
        Check if DDL exists in cache for the given object.

        :param db_conn: Database connection
        :param int client_id: Client ID for cache lookup
        :param str object_name: Name of the object (table, type, etc.)
        :return: Cached DDL string or None if not found
        """
        try:
            query = '''SELECT cache_id, generated_ddl FROM ddl_cache
                       WHERE client_id = ? AND LOWER(object_name) = LOWER(?)'''
            cursor = execute_query(db_conn, query, (client_id, object_name))
            row = cursor.fetchone()
            if row:
                # Update hit count and last_used timestamp
                update_query = '''UPDATE ddl_cache
                                  SET hit_count = hit_count + 1, last_used = CURRENT_TIMESTAMP
                                  WHERE cache_id = ?'''
                execute_query(db_conn, update_query, (row['cache_id'],))
                db_conn.commit()
                logger.info(f"DDL cache HIT for object '{object_name}' (client {client_id})")
                return row['generated_ddl']
            logger.info(f"DDL cache MISS for object '{object_name}' (client {client_id})")
            return None
        except Exception as e:
            logger.warning(f"DDL cache lookup failed for '{object_name}': {e}")
            return None

    def _store_ddl_cache(self, db_conn, client_id, object_name, ddl, session_id=None,
                         object_type='TABLE', export_dir=None):
        """
        Store generated DDL in cache (database) and optionally to file.

        :param db_conn: Database connection
        :param int client_id: Client ID
        :param str object_name: Name of the object
        :param str ddl: Generated DDL SQL
        :param int session_id: Optional session ID to link the cache entry
        :param str object_type: Type of object (TABLE, TYPE, etc.)
        :param str export_dir: Optional directory to save DDL file for review
        """
        try:
            ai_provider = self.ai_settings.get('ai_provider', 'unknown')
            ai_model = self.ai_settings.get('ai_model', 'unknown')

            # Insert or update cache entry (SQLite uses INSERT OR REPLACE, PostgreSQL uses ON CONFLICT)
            if is_postgres():
                query = '''INSERT INTO ddl_cache
                           (client_id, session_id, object_name, object_type, generated_ddl,
                            ai_provider, ai_model, hit_count, created_at, last_used)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                           ON CONFLICT (client_id, object_name) DO UPDATE SET
                           generated_ddl = EXCLUDED.generated_ddl,
                           ai_provider = EXCLUDED.ai_provider,
                           ai_model = EXCLUDED.ai_model,
                           last_used = CURRENT_TIMESTAMP'''
            else:
                query = '''INSERT OR REPLACE INTO ddl_cache
                           (client_id, session_id, object_name, object_type, generated_ddl,
                            ai_provider, ai_model, hit_count, created_at, last_used)
                           VALUES (?, ?, ?, ?, ?, ?, ?, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)'''

            execute_query(db_conn, query, (client_id, session_id, object_name, object_type,
                                           ddl, ai_provider, ai_model))
            db_conn.commit()
            logger.info(f"DDL cached for object '{object_name}' (client {client_id})")

            # Save to file for human review if export_dir provided
            if export_dir:
                self._save_ddl_to_file(export_dir, object_name, ddl, object_type, ai_provider, ai_model)

        except Exception as e:
            logger.warning(f"Failed to cache DDL for '{object_name}': {e}")

    def _save_ddl_to_file(self, export_dir, object_name, ddl, object_type, ai_provider, ai_model):
        """
        Save AI-generated DDL to file for human review.
        Creates ai_generated_ddl/ subdirectory and maintains a manifest.
        """
        try:
            ddl_dir = os.path.join(export_dir, 'ai_generated_ddl')
            os.makedirs(ddl_dir, exist_ok=True)

            # Sanitize filename
            safe_name = re.sub(r'[^\w\-.]', '_', object_name.lower())
            ddl_file = os.path.join(ddl_dir, f"{safe_name}.sql")

            # Write DDL file with header
            with open(ddl_file, 'w', encoding='utf-8') as f:
                f.write(f"-- AI-Generated DDL for: {object_name}\n")
                f.write(f"-- Type: {object_type}\n")
                f.write(f"-- Generated by: {ai_provider} / {ai_model}\n")
                f.write(f"-- Generated at: {datetime.now().isoformat()}\n")
                f.write("-- \n")
                f.write("-- Review this file before applying to production!\n")
                f.write("-- ============================================\n\n")
                f.write(ddl)

            logger.info(f"DDL saved to file: {ddl_file}")

            # Update manifest
            self._update_ddl_manifest(ddl_dir, object_name, object_type, f"{safe_name}.sql",
                                      ai_provider, ai_model)

        except Exception as e:
            logger.warning(f"Failed to save DDL file for '{object_name}': {e}")

    def _update_ddl_manifest(self, ddl_dir, object_name, object_type, filename, ai_provider, ai_model):
        """
        Update the _manifest.json file in the ai_generated_ddl directory.
        """
        manifest_path = os.path.join(ddl_dir, '_manifest.json')

        try:
            # Load existing manifest or create new
            if os.path.exists(manifest_path):
                with open(manifest_path, 'r', encoding='utf-8') as f:
                    manifest = json.load(f)
            else:
                manifest = {
                    'generated_at': datetime.now().isoformat(),
                    'ai_provider': ai_provider,
                    'ai_model': ai_model,
                    'objects': []
                }

            # Check if object already in manifest
            existing = next((o for o in manifest['objects'] if o['name'] == object_name), None)
            if existing:
                existing['file'] = filename
                existing['updated_at'] = datetime.now().isoformat()
            else:
                manifest['objects'].append({
                    'name': object_name,
                    'type': object_type,
                    'file': filename,
                    'applied': False,
                    'created_at': datetime.now().isoformat()
                })

            # Write updated manifest
            with open(manifest_path, 'w', encoding='utf-8') as f:
                json.dump(manifest, f, indent=2)

        except Exception as e:
            logger.warning(f"Failed to update DDL manifest: {e}")

    def _make_ai_call(self, system_instruction, full_prompt):
        """
        Makes a generic call to the configured AI service.
        Supports: Google AI, Anthropic, and OpenAI-compatible APIs.

        Corporate proxy settings:
        - ai_user: User identifier for tracking/auditing
        - ai_user_header: Custom header name to send the user identifier
        - ssl_cert_path: Path to SSL certificate for corporate proxies
        - ai_ssl_verify: Whether to verify SSL certificates (default: True)
        """
        api_key = self.ai_settings.get('ai_api_key')
        api_endpoint = self.ai_settings.get('ai_endpoint')
        ai_model = self.ai_settings.get('ai_model')
        headers = {'Content-Type': 'application/json'}

        if not api_key or not api_endpoint or not ai_model:
            raise ValueError("AI settings (API Key, Endpoint, Model) are not fully configured.")

        # Corporate proxy settings
        ai_user = self.ai_settings.get('ai_user', 'anonymous')
        ai_user_header = self.ai_settings.get('ai_user_header', '')
        ssl_cert_path = self.ai_settings.get('ssl_cert_path', '')
        ai_ssl_verify = self.ai_settings.get('ai_ssl_verify', True)

        # Handle ai_ssl_verify as string 'true'/'false' or boolean
        if isinstance(ai_ssl_verify, str):
            ai_ssl_verify = ai_ssl_verify.lower() in ('true', '1', 'yes')

        # Add custom user header if configured (for corporate tracking)
        if ai_user_header:
            headers[ai_user_header] = ai_user

        # Determine SSL verification setting
        # Priority: 1) Custom cert path, 2) certifi bundle (helps macOS/Homebrew), 3) system default
        verify_ssl = ai_ssl_verify
        if verify_ssl:
            if ssl_cert_path:
                verify_ssl = ssl_cert_path  # Use custom cert path
            else:
                verify_ssl = certifi.where()  # Use certifi bundle as fallback

        # Determine provider type from endpoint
        is_google = "generativelanguage.googleapis.com" in api_endpoint
        is_anthropic = "anthropic.com" in api_endpoint

        if is_google:
            model_name = ai_model.replace('-latest', '')
            api_url = f"{api_endpoint.rstrip('/')}/models/{model_name}:generateContent?key={api_key}"
            payload = {
                "contents": [{"parts": [{"text": full_prompt}]}],
                "systemInstruction": {"parts": [{"text": system_instruction}]},
                "generationConfig": { "temperature": float(self.ai_settings.get('ai_temperature', 0.2)), "maxOutputTokens": int(self.ai_settings.get('ai_max_output_tokens', 8192)) }
            }
        elif is_anthropic:
            # Anthropic Messages API
            api_url = f"{api_endpoint.rstrip('/')}/messages"
            headers['x-api-key'] = api_key
            headers['anthropic-version'] = '2023-06-01'
            headers['content-type'] = 'application/json'
            payload = {
                "model": ai_model,
                "max_tokens": int(self.ai_settings.get('ai_max_output_tokens', 4096)),
                "system": system_instruction,
                "messages": [{"role": "user", "content": full_prompt}]
            }
            # Add temperature if not using default
            temp = float(self.ai_settings.get('ai_temperature', 0.2))
            if temp > 0:
                payload["temperature"] = temp
        else:
            # OpenAI-compatible API
            api_url = f"{api_endpoint.rstrip('/')}/chat/completions"
            headers['Authorization'] = f'Bearer {api_key}'
            payload = {
                "model": ai_model,
                "messages": [
                    {"role": "system", "content": system_instruction},
                    {"role": "user", "content": full_prompt}
                ],
                "temperature": float(self.ai_settings.get('ai_temperature', 0.2)),
                "max_tokens": int(self.ai_settings.get('ai_max_output_tokens', 4096)),
                "user": ai_user  # For corporate tracking/auditing
            }

        try:
            response = requests.post(api_url, json=payload, headers=headers, timeout=300, verify=verify_ssl)
            response.raise_for_status()
            response_data = response.json()
            generated_text = ""
            max_token_error_msg = "The AI model stopped generating because the maximum token limit was reached. Try increasing the 'Max Output Tokens' in your settings or switch to an AI model with a larger context window (e.g., gpt-4-turbo)."

            if is_google:
                candidates = response_data.get('candidates', [])
                if not candidates: raise ValueError(f"AI response is missing 'candidates'. Full response: {response_data}")
                finish_reason = candidates[0].get('finishReason')
                if finish_reason == 'MAX_TOKENS': raise ValueError(max_token_error_msg)
                if 'content' in candidates[0] and 'parts' in candidates[0]['content'] and candidates[0]['content']['parts']: generated_text = candidates[0]['content']['parts'][0].get('text', '').strip()
                else: raise ValueError(f"Unexpected response structure from Google AI: {response_data}")
            elif is_anthropic:
                # Anthropic response format
                content = response_data.get('content', [])
                if not content: raise ValueError(f"AI response is missing 'content'. Full response: {response_data}")
                stop_reason = response_data.get('stop_reason')
                if stop_reason == 'max_tokens': raise ValueError(max_token_error_msg)
                # Extract text from content blocks
                for block in content:
                    if block.get('type') == 'text':
                        generated_text += block.get('text', '')
                generated_text = generated_text.strip()
            else:
                choices = response_data.get('choices', [])
                if not choices: raise ValueError(f"AI response is missing 'choices'. Full response: {response_data}")
                finish_reason = choices[0].get('finish_reason')
                if finish_reason == 'length': raise ValueError(max_token_error_msg)
                if 'message' in choices[0] and 'content' in choices[0]['message']: generated_text = choices[0]['message']['content'].strip()
                else: raise ValueError(f"Unexpected response structure from AI provider: {response_data}")

            generated_text = re.sub(r'^```sql\n|```$', '', generated_text, flags=re.MULTILINE).strip()
            if not generated_text: raise ValueError("AI returned an empty response.")

            # Extract token usage - different providers have different structures
            if is_google:
                usage_meta = response_data.get('usageMetadata', {})
                input_tokens = usage_meta.get('promptTokenCount', 0)
                output_tokens = usage_meta.get('candidatesTokenCount', 0)
                total_tokens = usage_meta.get('totalTokenCount', input_tokens + output_tokens)
            elif is_anthropic:
                usage = response_data.get('usage', {})
                input_tokens = usage.get('input_tokens', 0)
                output_tokens = usage.get('output_tokens', 0)
                total_tokens = input_tokens + output_tokens
            else:
                # OpenAI-compatible API
                usage = response_data.get('usage', {})
                input_tokens = usage.get('prompt_tokens', 0)
                output_tokens = usage.get('completion_tokens', 0)
                total_tokens = usage.get('total_tokens', input_tokens + output_tokens)

            metrics = {
                'status': 'success',
                'tokens_used': total_tokens,
                'input_tokens': input_tokens,
                'output_tokens': output_tokens
            }
            return generated_text, metrics
        except requests.exceptions.Timeout:
            logger.error("AI request timed out.")
            raise ValueError('AI service request timed out.')
        
    def validate_sql(self, sql, pg_dsn, clean_slate=False, auto_create_ddl=True,
                      cache_context=None, defer_fk=False, metrics=None):
        """
        Validates SQL against a PostgreSQL database, with AI-powered retry logic.

        :param str sql: SQL to validate
        :param str pg_dsn: PostgreSQL connection string
        :param bool clean_slate: Drop tables before validation
        :param bool auto_create_ddl: Auto-create missing tables
        :param dict cache_context: Optional context for DDL caching:
            - db_conn: App database connection for cache
            - client_id: Client ID for cache key
            - export_dir: Directory to save DDL files for review
        :param bool defer_fk: If True, split out FK constraints and return them
                              for deferred execution (handles circular dependencies)
        :param dict metrics: Optional dict to accumulate token metrics:
            - input_tokens: Total input tokens used
            - output_tokens: Total output tokens used
            - ai_attempts: Number of AI calls made
        :return: Tuple of (success, message, corrected_sql, deferred_fk_statements)
                 If defer_fk=False, deferred_fk_statements is None
        """
        # Initialize metrics if provided
        if metrics is not None:
            metrics.setdefault('input_tokens', 0)
            metrics.setdefault('output_tokens', 0)
            metrics.setdefault('ai_attempts', 0)
        # Strip psql metacommands (like \set) that can't be executed via psycopg2
        sql = self._strip_psql_metacommands(sql)

        # Apply Oracle-to-PostgreSQL preprocessing (timestamps, types, etc.)
        sql = preprocess_oracle_sql(sql)

        # Quote PostgreSQL reserved words used as identifiers
        sql = quote_reserved_words(sql)

        # If deferring FK constraints, split them out
        deferred_fk_statements = []
        if defer_fk:
            sql, deferred_fk_statements = self._split_fk_constraints(sql)

        if clean_slate:
            # Extract objects being CREATED (not referenced tables)
            created_objects = self._extract_created_objects(sql)
            if created_objects:
                try:
                    with psycopg2.connect(pg_dsn) as conn:
                        with conn.cursor() as cursor:
                            conn.set_session(autocommit=True)
                            for obj_type, obj_name in created_objects:
                                # Build appropriate DROP statement for each object type
                                identifier = psql.Identifier(obj_name)

                                if obj_type == 'TABLE':
                                    drop_sql = psql.SQL("DROP TABLE IF EXISTS {} CASCADE")
                                elif obj_type == 'VIEW':
                                    drop_sql = psql.SQL("DROP VIEW IF EXISTS {} CASCADE")
                                elif obj_type == 'MATERIALIZED VIEW':
                                    drop_sql = psql.SQL("DROP MATERIALIZED VIEW IF EXISTS {} CASCADE")
                                elif obj_type == 'FUNCTION':
                                    # Functions need () to drop all overloads
                                    drop_sql = psql.SQL("DROP FUNCTION IF EXISTS {} CASCADE")
                                elif obj_type == 'PROCEDURE':
                                    drop_sql = psql.SQL("DROP PROCEDURE IF EXISTS {} CASCADE")
                                elif obj_type == 'INDEX':
                                    drop_sql = psql.SQL("DROP INDEX IF EXISTS {} CASCADE")
                                elif obj_type == 'SEQUENCE':
                                    drop_sql = psql.SQL("DROP SEQUENCE IF EXISTS {} CASCADE")
                                elif obj_type == 'TYPE':
                                    drop_sql = psql.SQL("DROP TYPE IF EXISTS {} CASCADE")
                                elif obj_type == 'TRIGGER':
                                    # Triggers need ON table, but we can't easily determine it
                                    # Skip trigger drops in clean_slate
                                    continue
                                else:
                                    continue

                                drop_statement = drop_sql.format(identifier)
                                logger.info(f"Executing clean slate: {drop_statement.as_string(conn)}")
                                cursor.execute(drop_statement)
                except psycopg2.Error as e:
                    logger.error(f"Clean slate failed: {e}")
                    return False, f"Clean slate pre-validation step failed: {e}", None, []

        if auto_create_ddl:
            try:
                needed_tables = self._extract_table_names(sql)
                if needed_tables:
                    with psycopg2.connect(pg_dsn) as conn:
                        with conn.cursor() as cursor:
                            cursor.execute("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'")
                            existing_tables = {row[0] for row in cursor.fetchall()}

                            missing_tables = needed_tables - existing_tables

                            if missing_tables:
                                # Check cache for each missing table
                                tables_needing_ai = set()
                                cached_ddl_parts = []

                                if cache_context and cache_context.get('db_conn') and cache_context.get('client_id'):
                                    for table_name in missing_tables:
                                        cached = self._check_ddl_cache(
                                            cache_context['db_conn'],
                                            cache_context['client_id'],
                                            table_name
                                        )
                                        if cached:
                                            cached_ddl_parts.append(cached)
                                        else:
                                            tables_needing_ai.add(table_name)
                                else:
                                    tables_needing_ai = missing_tables

                                # Apply cached DDL first
                                if cached_ddl_parts:
                                    for ddl in cached_ddl_parts:
                                        try:
                                            cursor.execute(ddl)
                                        except psycopg2.Error:
                                            pass  # Table may already exist
                                    logger.info(f"Applied {len(cached_ddl_parts)} cached DDL(s)")

                                # Ask AI only for truly missing tables
                                if tables_needing_ai:
                                    logger.info(f"Proactively found missing tables: {tables_needing_ai}. Asking AI for consolidated DDL.")
                                    consolidated_ddl = self._get_consolidated_ddl_from_ai(sql, tables_needing_ai)
                                    if consolidated_ddl:
                                        cursor.execute(consolidated_ddl)
                                        logger.info(f"Applied consolidated DDL for: {tables_needing_ai}")

                                        # Cache each table's DDL for future use
                                        if cache_context and cache_context.get('db_conn') and cache_context.get('client_id'):
                                            for table_name in tables_needing_ai:
                                                self._store_ddl_cache(
                                                    cache_context['db_conn'],
                                                    cache_context['client_id'],
                                                    table_name,
                                                    consolidated_ddl,
                                                    export_dir=cache_context.get('export_dir')
                                                )

            except psycopg2.Error as e:
                logger.warning(f"Proactive DDL check failed: {e}. Falling back to reactive validation.")
            except Exception as e:
                logger.error(f"An unexpected error occurred during proactive DDL check: {e}")

        max_retries = 5
        current_sql = sql
        for attempt in range(max_retries):
            try:
                with psycopg2.connect(pg_dsn) as conn:
                    with conn.cursor() as cursor:
                        conn.set_session(autocommit=True)
                        cursor.execute("SET client_min_messages TO WARNING")
                        cursor.execute(current_sql)
                logger.info("SQL validation successful.")
                
                final_message = "Validation successful"
                if current_sql != sql:
                    final_message = "Validation successful after applying AI corrections to the query."

                return True, final_message, current_sql if current_sql != sql else None, deferred_fk_statements
            
            except psycopg2.Error as e:
                error_message = str(e).strip()
                logger.info(f"PostgreSQL error: {error_message}")
                missing_relation_match = re.search(r'relation "([\w\.]+)" does not exist', error_message)
                missing_type_match = re.search(r'type "([\w\.]+)(?:\[\])?" does not exist', error_message)

                if missing_relation_match:
                    if auto_create_ddl:
                        object_name = missing_relation_match.group(1)

                        # Check cache first
                        ddl_to_execute = None
                        from_cache = False
                        if cache_context and cache_context.get('db_conn') and cache_context.get('client_id'):
                            ddl_to_execute = self._check_ddl_cache(
                                cache_context['db_conn'],
                                cache_context['client_id'],
                                object_name
                            )
                            if ddl_to_execute:
                                from_cache = True
                                logger.info(f"Attempt {attempt + 1}/{max_retries}: Using cached DDL for '{object_name}'.")

                        # If not in cache, ask AI
                        if not ddl_to_execute:
                            logger.info(f"Attempt {attempt + 1}/{max_retries}: Missing relation '{object_name}'. Asking AI for DDL.")
                            ddl_to_execute, ai_metrics = self._get_ddl_from_ai(current_sql, error_message, object_name)
                            # Accumulate metrics if provided
                            if metrics is not None:
                                metrics['input_tokens'] += ai_metrics.get('input_tokens', 0)
                                metrics['output_tokens'] += ai_metrics.get('output_tokens', 0)
                                metrics['ai_attempts'] += 1

                        if not ddl_to_execute:
                            return False, f"Validation failed: AI could not generate DDL for '{object_name}'.", None, []
                        try:
                            with psycopg2.connect(pg_dsn) as conn_ddl:
                                with conn_ddl.cursor() as cursor_ddl:
                                    conn_ddl.set_session(autocommit=True)
                                    cursor_ddl.execute(ddl_to_execute)
                            logger.info(f"Applied DDL for '{object_name}'. Retrying.")

                            # Cache the DDL if it came from AI (not from cache)
                            if not from_cache and cache_context and cache_context.get('db_conn') and cache_context.get('client_id'):
                                self._store_ddl_cache(
                                    cache_context['db_conn'],
                                    cache_context['client_id'],
                                    object_name,
                                    ddl_to_execute,
                                    export_dir=cache_context.get('export_dir')
                                )

                        except psycopg2.Error as ddl_error:
                            return False, f"Validation failed: AI-generated DDL was invalid. Error: {ddl_error}", None, []
                    else:
                        logger.error(f"Validation failed: {error_message}. Auto-create DDL is disabled.")
                        return False, f"Validation failed: {error_message}. Auto-create DDL is disabled.", None, []
                elif missing_type_match:
                    if auto_create_ddl:
                        type_name = missing_type_match.group(1)

                        # Check cache first
                        ddl_to_execute = None
                        from_cache = False
                        if cache_context and cache_context.get('db_conn') and cache_context.get('client_id'):
                            ddl_to_execute = self._check_ddl_cache(
                                cache_context['db_conn'],
                                cache_context['client_id'],
                                type_name
                            )
                            if ddl_to_execute:
                                from_cache = True
                                logger.info(f"Attempt {attempt + 1}/{max_retries}: Using cached DDL for type '{type_name}'.")

                        # If not in cache, ask AI
                        if not ddl_to_execute:
                            logger.info(f"Attempt {attempt + 1}/{max_retries}: Missing type '{type_name}'. Asking AI for TYPE DDL.")
                            ddl_to_execute, ai_metrics = self._get_type_ddl_from_ai(current_sql, error_message, type_name)
                            # Accumulate metrics if provided
                            if metrics is not None:
                                metrics['input_tokens'] += ai_metrics.get('input_tokens', 0)
                                metrics['output_tokens'] += ai_metrics.get('output_tokens', 0)
                                metrics['ai_attempts'] += 1

                        if not ddl_to_execute:
                            return False, f"Validation failed: AI could not generate DDL for type '{type_name}'.", None, []

                        try:
                            with psycopg2.connect(pg_dsn) as conn_ddl:
                                with conn_ddl.cursor() as cursor_ddl:
                                    conn_ddl.set_session(autocommit=True)
                                    cursor_ddl.execute(ddl_to_execute)
                            logger.info(f"Applied TYPE DDL for '{type_name}'. Retrying.")

                            # Cache the DDL if it came from AI (not from cache)
                            if not from_cache and cache_context and cache_context.get('db_conn') and cache_context.get('client_id'):
                                self._store_ddl_cache(
                                    cache_context['db_conn'],
                                    cache_context['client_id'],
                                    type_name,
                                    ddl_to_execute,
                                    export_dir=cache_context.get('export_dir')
                                )

                        except psycopg2.Error as ddl_error:
                            return False, f"Validation failed: AI-generated TYPE DDL was invalid. Error: {ddl_error}", None, []
                    else:
                        logger.error(f"Validation failed: {error_message}. Auto-create DDL is disabled.")
                        return False, f"Validation failed: {error_message}. Auto-create DDL is disabled.", None, []
                else:
                    logger.info(f"Attempt {attempt + 1}/{max_retries}: Non-relation error encountered. Asking AI to fix query.")
                    new_sql, ai_metrics = self._get_query_fix_from_ai(current_sql, error_message)
                    # Accumulate metrics if provided
                    if metrics is not None:
                        metrics['input_tokens'] += ai_metrics.get('input_tokens', 0)
                        metrics['output_tokens'] += ai_metrics.get('output_tokens', 0)
                        metrics['ai_attempts'] += 1
                    if not new_sql:
                         return False, f"Validation failed: AI could not fix the query error: {error_message}", None, []
                    logger.info("AI provided a potential query fix. Retrying validation with the new query.")
                    current_sql = new_sql

        return False, f"Validation failed after {max_retries} attempts.", None, []

    def save_corrected_file(self, original_sql, corrected_sql, filename="corrected_output.sql"):
        """
        Saves the corrected SQL to a file in the output directory.
        """
        if '..' in filename or filename.startswith('/'):
            raise ValueError("Invalid filename provided.")

        output_path = os.path.join(self.output_dir, filename)
        try:
            os.makedirs(self.output_dir, exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(corrected_sql)
            logger.info(f"Corrected SQL saved to {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"Failed to save corrected SQL to {output_path}: {e}")
            raise

import os
import subprocess
import re
import html
import logging
import requests
import tempfile
import shutil
from cryptography.fernet import Fernet
import psycopg2
from psycopg2 import sql as psql
import json
from datetime import datetime
from .db import execute_query

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
            
            result = subprocess.run(command, capture_output=True, text=True, timeout=300)

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

    def run_ora2pg_export(self, client_id, db_conn, client_config, extra_args=None):
        """
        Manages the full Ora2Pg export process, including session creation and file persistence.
        """
        is_report = extra_args and any(arg in extra_args for arg in ['SHOW_REPORT', 'SHOW_VERSION'])
        if is_report:
            stdout, stderr, returncode = self._run_single_ora2pg_command(client_config, extra_args)
            if returncode != 0: return {}, stderr
            return {'sql_output': stdout}, None

        try:
            session_name = f"Export - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            export_type = client_config.get('type', 'TABLE').upper()
            
            if os.environ.get('DB_BACKEND', 'sqlite') == 'sqlite':
                cursor = execute_query(db_conn, 'INSERT INTO migration_sessions (client_id, session_name, export_directory, export_type) VALUES (?, ?, ?, ?)', (client_id, session_name, "pending", export_type))
                session_id = cursor.lastrowid
            else:
                cursor = execute_query(db_conn, 'INSERT INTO migration_sessions (client_id, session_name, export_directory, export_type) VALUES (%s, %s, %s, %s) RETURNING session_id', (client_id, session_name, "pending", export_type))
                session_id = cursor.fetchone()['session_id']

            persistent_export_dir = os.path.join('/app/project_data', str(client_id), str(session_id))
            os.makedirs(persistent_export_dir, exist_ok=True)

            if os.environ.get('DB_BACKEND', 'sqlite') == 'sqlite':
                execute_query(db_conn, 'UPDATE migration_sessions SET export_directory = ? WHERE session_id = ?', (persistent_export_dir, session_id))
            else:
                execute_query(db_conn, 'UPDATE migration_sessions SET export_directory = %s WHERE session_id = %s', (persistent_export_dir, session_id))
            
            db_conn.commit()
            logger.info(f"Created persistent session {session_id} at {persistent_export_dir}")

            file_per_table = str(client_config.get('file_per_table', '0')) in ['true', 'True', '1']
            run_config = client_config.copy()
            
            if file_per_table and export_type == 'TABLE' and 'ALLOW' in run_config:
                logger.info("Executing multi-file DDL export strategy (looping).")
                generated_files = []
                tables = [t.strip() for t in run_config['ALLOW'].split(',')]
                for table_name in tables:
                    single_table_config = run_config.copy()
                    single_table_config['ALLOW'] = table_name
                    single_table_config['OUTPUT'] = os.path.join(persistent_export_dir, f"{table_name}.sql")
                    single_table_config.pop('OUTPUT_DIR', None)
                    
                    _, error, returncode = self._run_single_ora2pg_command(single_table_config)
                    if returncode == 0:
                        generated_files.append(f"{table_name}.sql")
                    else:
                        logger.warning(f"Skipping table {table_name} for session {session_id} due to export error: {error}")
            else:
                logger.info("Executing single-command export strategy.")
                if file_per_table:
                    run_config['OUTPUT_DIR'] = persistent_export_dir
                    run_config['OUTPUT'] = f"output_{export_type.lower()}.sql"
                else:
                    run_config['OUTPUT'] = os.path.join(persistent_export_dir, f"output_{export_type.lower()}.sql")
                    run_config.pop('OUTPUT_DIR', None)

                _, stderr, returncode = self._run_single_ora2pg_command(run_config)
                
                if returncode != 0:
                    logger.error(f"Ora2Pg command failed. Rolling back session {session_id}.")
                    if os.environ.get('DB_BACKEND', 'sqlite') == 'sqlite':
                        execute_query(db_conn, 'DELETE FROM migration_sessions WHERE session_id = ?', (session_id,))
                    else:
                        execute_query(db_conn, 'DELETE FROM migration_sessions WHERE session_id = %s', (session_id,))
                    db_conn.commit()
                    shutil.rmtree(persistent_export_dir)
                    return {}, stderr
                
                generated_files = [f for f in os.listdir(persistent_export_dir) if f.endswith('.sql')]

            for filename in generated_files:
                if os.environ.get('DB_BACKEND', 'sqlite') == 'sqlite':
                    execute_query(db_conn, 'INSERT INTO migration_files (session_id, filename) VALUES (?, ?)', (session_id, filename))
                else:
                    execute_query(db_conn, 'INSERT INTO migration_files (session_id, filename) VALUES (%s, %s)', (session_id, filename))
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

    def _extract_table_names(self, sql):
        """
        Extracts table names from a SQL query, ignoring CTEs.
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
        logger.info(f"Extracted table names for cleanup: {table_names} (ignoring CTEs: {cte_names})")
        return table_names

    def ai_correct_sql(self, sql, source_dialect='oracle'):
        """
        Sends SQL code to an AI model for conversion to PostgreSQL.
        
        :param str sql: The SQL code to convert
        :param str source_dialect: The source SQL dialect (oracle, mysql, sqlserver, postgres, generic)
        :return: Tuple of (corrected_sql, metrics)
        :rtype: tuple
        """
        if not sql:
            return sql, {'status': 'no_content', 'tokens_used': 0}
        
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
            system_instruction = f"You are an expert in database migrations. Convert {source_name} SQL to PostgreSQL, replacing {source_name}-specific constructs with PostgreSQL equivalents. Handle data types, functions, syntax, and PL/SQL to PL/pgSQL conversions."
            full_prompt = f"""Convert this {source_name} SQL to PostgreSQL-compatible SQL. Provide only the converted SQL code with no explanations.
    
    Key conversions needed:
    - Data types ({source_name} → PostgreSQL)
    - Functions and operators
    - Procedural code (if any)
    - Syntax differences
    
    Original {source_name} SQL:
    ```sql
    {sql}
    ```"""
        
        try:
            return self._make_ai_call(system_instruction, full_prompt)
        except Exception as e:
            logger.error(f"AI SQL conversion from {source_name} failed: {e}", exc_info=False)
            return sql, {'status': 'error', 'error_message': str(e), 'tokens_used': 0}


    def _get_ddl_from_ai(self, failed_sql, error_message, object_name):
        """
        Asks the AI to generate a DDL statement for a missing object.
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
            ddl_sql, _ = self._make_ai_call(system_instruction, full_prompt)
            return ddl_sql
        except Exception as e:
            logger.error(f"AI DDL generation failed for object '{object_name}': {e}", exc_info=False)
            return None

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
        """
        system_instruction = "You are a PostgreSQL expert. Your task is to correct a SQL query that failed validation."
        full_prompt = f"""The following PostgreSQL query failed with the error: `{error_message}`.
Please correct the query to resolve the issue. Provide only the corrected, complete SQL query.
Failed Query:
```sql
{failed_sql}
```"""
        try:
            fixed_sql, _ = self._make_ai_call(system_instruction, full_prompt)
            return fixed_sql
        except Exception as e:
            logger.error(f"AI query fix generation failed: {e}", exc_info=False)
            return None

    def _make_ai_call(self, system_instruction, full_prompt):
        """
        Makes a generic call to the configured AI service.
        """
        api_key = self.ai_settings.get('ai_api_key')
        api_endpoint = self.ai_settings.get('ai_endpoint')
        ai_model = self.ai_settings.get('ai_model')
        headers = {}
        
        if not api_key or not api_endpoint or not ai_model:
            raise ValueError("AI settings (API Key, Endpoint, Model) are not fully configured.")

        if "generativelanguage.googleapis.com" in api_endpoint:
            model_name = ai_model.replace('-latest', '')
            api_url = f"{api_endpoint.rstrip('/')}/models/{model_name}:generateContent?key={api_key}"
            payload = {
                "contents": [{"parts": [{"text": full_prompt}]}],
                "systemInstruction": {"parts": [{"text": system_instruction}]},
                "generationConfig": { "temperature": float(self.ai_settings.get('ai_temperature', 0.2)), "maxOutputTokens": int(self.ai_settings.get('ai_max_output_tokens', 8192)) }
            }
        else:
            api_url = f"{api_endpoint.rstrip('/')}/chat/completions"
            headers['Authorization'] = f'Bearer {api_key}'
            payload = { "model": ai_model, "messages": [{"role": "system", "content": system_instruction}, {"role": "user", "content": full_prompt}], "temperature": float(self.ai_settings.get('ai_temperature', 0.2)), "max_tokens": int(self.ai_settings.get('ai_max_output_tokens', 4096)) }
        
        try:
            response = requests.post(api_url, json=payload, headers=headers, timeout=300)
            response.raise_for_status()
            response_data = response.json()
            generated_text = ""
            max_token_error_msg = "The AI model stopped generating because the maximum token limit was reached. Try increasing the 'Max Output Tokens' in your settings or switch to an AI model with a larger context window (e.g., gpt-4-turbo)."

            if "generativelanguage.googleapis.com" in api_endpoint:
                candidates = response_data.get('candidates', [])
                if not candidates: raise ValueError(f"AI response is missing 'candidates'. Full response: {response_data}")
                finish_reason = candidates[0].get('finishReason')
                if finish_reason == 'MAX_TOKENS': raise ValueError(max_token_error_msg)
                if 'content' in candidates[0] and 'parts' in candidates[0]['content'] and candidates[0]['content']['parts']: generated_text = candidates[0]['content']['parts'][0].get('text', '').strip()
                else: raise ValueError(f"Unexpected response structure from Google AI: {response_data}")
            else:
                choices = response_data.get('choices', [])
                if not choices: raise ValueError(f"AI response is missing 'choices'. Full response: {response_data}")
                finish_reason = choices[0].get('finish_reason')
                if finish_reason == 'length': raise ValueError(max_token_error_msg)
                if 'message' in choices[0] and 'content' in choices[0]['message']: generated_text = choices[0]['message']['content'].strip()
                else: raise ValueError(f"Unexpected response structure from AI provider: {response_data}")

            generated_text = re.sub(r'^```sql\n|```$', '', generated_text, flags=re.MULTILINE).strip()
            if not generated_text: raise ValueError("AI returned an empty response.")

            metrics = { 'status': 'success', 'tokens_used': response_data.get('usage', {}).get('total_tokens', 0) if 'usage' in response_data else response_data.get('usageMetadata', {}).get('totalTokenCount', 0) }
            return generated_text, metrics
        except requests.exceptions.Timeout:
            logger.error("AI request timed out.")
            raise ValueError('AI service request timed out.')
        
    def validate_sql(self, sql, pg_dsn, clean_slate=False, auto_create_ddl=True):
        """
        Validates SQL against a PostgreSQL database, with AI-powered retry logic.
        """
        if clean_slate:
            table_names = self._extract_table_names(sql)
            if table_names:
                try:
                    with psycopg2.connect(pg_dsn) as conn:
                        with conn.cursor() as cursor:
                            conn.set_session(autocommit=True)
                            for table in table_names:
                                parts = table.split('.')
                                if len(parts) > 1:
                                    identifier = psql.Identifier(parts[0], parts[1])
                                else:
                                    identifier = psql.Identifier(parts[0])
                                
                                drop_statement = psql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(identifier)
                                logger.info(f"Executing clean slate: {drop_statement.as_string(conn)}")
                                cursor.execute(drop_statement)
                except psycopg2.Error as e:
                    logger.error(f"Clean slate failed: {e}")
                    return False, f"Clean slate pre-validation step failed: {e}", None

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
                                logger.info(f"Proactively found missing tables: {missing_tables}. Asking AI for consolidated DDL.")
                                consolidated_ddl = self._get_consolidated_ddl_from_ai(sql, missing_tables)
                                if consolidated_ddl:
                                    cursor.execute(consolidated_ddl)
                                    logger.info(f"Applied consolidated DDL for: {missing_tables}")

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

                return True, final_message, current_sql if current_sql != sql else None
            
            except psycopg2.Error as e:
                error_message = str(e).strip()
                missing_relation_match = re.search(r'relation "([\w\.]+)" does not exist', error_message)
                
                if missing_relation_match:
                    if auto_create_ddl:
                        object_name = missing_relation_match.group(1)
                        logger.info(f"Attempt {attempt + 1}/{max_retries}: Missing relation '{object_name}'. Asking AI for DDL.")
                        
                        ddl_to_execute = self._get_ddl_from_ai(current_sql, error_message, object_name)

                        if not ddl_to_execute:
                            return False, f"Validation failed: AI could not generate DDL for '{object_name}'.", None
                        try:
                            with psycopg2.connect(pg_dsn) as conn_ddl:
                                with conn_ddl.cursor() as cursor_ddl:
                                    conn_ddl.set_session(autocommit=True)
                                    cursor_ddl.execute(ddl_to_execute)
                            logger.info(f"Applied DDL for '{object_name}'. Retrying.")
                        except psycopg2.Error as ddl_error:
                            return False, f"Validation failed: AI-generated DDL was invalid. Error: {ddl_error}", None
                    else:
                        logger.error(f"Validation failed: {error_message}. Auto-create DDL is disabled.")
                        return False, f"Validation failed: {error_message}. Auto-create DDL is disabled.", None
                else:
                    logger.info(f"Attempt {attempt + 1}/{max_retries}: Non-relation error encountered. Asking AI to fix query.")
                    new_sql = self._get_query_fix_from_ai(current_sql, error_message)
                    if not new_sql:
                         return False, f"Validation failed: AI could not fix the query error: {error_message}", None
                    logger.info("AI provided a potential query fix. Retrying validation with the new query.")
                    current_sql = new_sql

        return False, f"Validation failed after {max_retries} attempts.", None

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

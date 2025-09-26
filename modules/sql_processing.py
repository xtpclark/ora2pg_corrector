import os
import subprocess
import re
import html
import logging
import requests
from cryptography.fernet import Fernet
import psycopg2
from psycopg2 import sql as psql

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Ora2PgAICorrector:
    """Handles the core logic for SQL correction and validation."""
    def __init__(self, output_dir, ai_settings, encryption_key):
        self.ora2pg_path = 'ora2pg'
        self.output_dir = output_dir
        self.ai_settings = ai_settings
        self.encryption_key = encryption_key
        self.fernet = Fernet(encryption_key)
        
    def _extract_table_names(self, sql):
        """Extracts table names from a SQL query using regex, ignoring CTEs."""
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

    def ai_correct_sql(self, sql):
        """Uses an AI model to correct Oracle-specific SQL for PostgreSQL."""
        if not sql:
            return sql, {'status': 'no_content', 'tokens_used': 0}
        
        system_instruction = "You are an expert in Oracle to PostgreSQL migration. Correct the SQL code for PostgreSQL compatibility, replacing Oracle-specific constructs with equivalents."
        full_prompt = f"""Provide only the corrected SQL code.

Original SQL:
```sql
{sql}
```"""
        try:
            return self._make_ai_call(system_instruction, full_prompt)
        except Exception as e:
            logger.error(f"AI correction failed: {e}", exc_info=False)
            return sql, {'status': 'error', 'error_message': str(e), 'tokens_used': 0}

    def _get_ddl_from_ai(self, failed_sql, error_message, object_name):
        """Asks the AI to generate DDL for a specific missing object."""
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

    # --- NEW: Function for consolidated DDL generation ---
    def _get_consolidated_ddl_from_ai(self, sql_query, missing_tables):
        """Asks the AI to generate DDL for a list of missing tables."""
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
        """Asks the AI to correct a query based on a validation error."""
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
        # ... This function is unchanged ...
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
        else: # OpenAI-like
            api_url = f"{api_endpoint.rstrip('/')}/chat/completions"
            headers['Authorization'] = f'Bearer {api_key}'
            payload = { "model": ai_model, "messages": [{"role": "system", "content": system_instruction}, {"role": "user", "content": full_prompt}], "temperature": float(self.ai_settings.get('ai_temperature', 0.2)), "max_tokens": int(self.ai_settings.get('ai_max_output_tokens', 4096)) }
        
        try:
            response = requests.post(api_url, json=payload, headers=headers, timeout=120)
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
            else: # OpenAI-like
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
        if clean_slate:
            # ... This clean_slate logic is unchanged ...
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

        # --- CHANGE: Proactive DDL Generation Logic ---
        if auto_create_ddl:
            try:
                needed_tables = self._extract_table_names(sql)
                if needed_tables:
                    with psycopg2.connect(pg_dsn) as conn:
                        with conn.cursor() as cursor:
                            # Check which tables already exist
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
                # ... This try/except block for reactive validation is unchanged ...
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
                else: # Attempt to fix the query itself
                    logger.info(f"Attempt {attempt + 1}/{max_retries}: Non-relation error encountered. Asking AI to fix query.")
                    new_sql = self._get_query_fix_from_ai(current_sql, error_message)
                    if not new_sql:
                         return False, f"Validation failed: AI could not fix the query error: {error_message}", None
                    logger.info("AI provided a potential query fix. Retrying validation with the new query.")
                    current_sql = new_sql

        return False, f"Validation failed after {max_retries} attempts.", None

    def save_corrected_file(self, original_sql, corrected_sql):
        # ... This function is unchanged ...
        output_path = os.path.join(self.output_dir, 'corrected_output.sql')
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(corrected_sql)
            logger.info(f"Corrected SQL saved to {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"Failed to save corrected SQL: {e}")
            raise

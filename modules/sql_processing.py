import os
import subprocess
import re
import html
import logging
import requests
from cryptography.fernet import Fernet
import psycopg2

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

    def ai_correct_sql(self, sql):
        """Uses an AI model to correct Oracle-specific SQL for PostgreSQL."""
        if not sql:
            return sql, {'status': 'no_content', 'tokens_used': 0}
        
        system_instruction = "You are an expert in Oracle to PostgreSQL migration. Correct the SQL code for PostgreSQL compatibility, replacing Oracle-specific constructs with equivalents."
        full_prompt = f"""Replace Oracle-specific constructs with PostgreSQL equivalents:
- NVL -> COALESCE
- DBMS_OUTPUT.PUT_LINE -> RAISE NOTICE
- DECODE -> CASE
- SYSDATE -> CURRENT_TIMESTAMP
- TO_DATE -> TO_TIMESTAMP
- AUTONOMOUS_TRANSACTION -> function with explicit transaction control
- CONNECT BY -> WITH RECURSIVE
- NUMBER -> NUMERIC or INTEGER as appropriate
- Empty string ('') -> handle as NULL or empty string per PostgreSQL behavior
Ensure the corrected SQL is syntactically correct and maintains the original functionality. Provide only the corrected SQL code.

Original SQL:
```sql
{sql}
```"""
        try:
            return self._make_ai_call(system_instruction, full_prompt)
        except Exception as e:
            logger.error(f"AI correction failed: {e}", exc_info=False)
            return sql, {'status': 'error', 'error_message': str(e), 'tokens_used': 0}

    def _get_ddl_from_ai(self, failed_sql, error_message):
        """Asks the AI to generate DDL for a missing object."""
        system_instruction = "You are a PostgreSQL expert. Your task is to generate the necessary DDL to resolve a missing object error."
        full_prompt = f"""The following PostgreSQL query failed with the error: `{error_message}`. 
Please generate the `CREATE TABLE` or `CREATE TYPE` statements needed to define the missing objects. 
Infer column names and reasonable data types (e.g., VARCHAR, INTEGER, NUMERIC, TIMESTAMP) from the query context. 
Provide only the raw DDL SQL code, with no explanations or markdown.
Query:
```sql
{failed_sql}
```"""
        try:
            ddl_sql, _ = self._make_ai_call(system_instruction, full_prompt)
            return ddl_sql
        except Exception as e:
            logger.error(f"AI DDL generation failed: {e}", exc_info=False)
            return None

    def _get_query_fix_from_ai(self, failed_sql, error_message):
        """Asks the AI to correct a query based on a validation error."""
        system_instruction = "You are a PostgreSQL expert. Your task is to correct a SQL query that failed validation."
        full_prompt = f"""The following PostgreSQL query failed with the error: `{error_message}`.
Please correct the query to resolve the issue. For example, if the error is about a function not matching argument types, add the necessary explicit type casts (e.g., `CAST(column AS NUMERIC)`).
Provide only the corrected, complete SQL query, with no explanations or markdown.
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
        """A shared helper function to make API calls to the AI service."""
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

            if "generativelanguage.googleapis.com" in api_endpoint:
                candidates = response_data.get('candidates', [])
                if not candidates: raise ValueError(f"AI response is missing 'candidates'. Full response: {response_data}")
                finish_reason = candidates[0].get('finishReason')
                if finish_reason == 'MAX_TOKENS': raise ValueError("AI stopped generating due to maximum token limit. The input SQL is too large.")
                if 'content' in candidates[0] and 'parts' in candidates[0]['content'] and candidates[0]['content']['parts']: generated_text = candidates[0]['content']['parts'][0].get('text', '').strip()
                else: raise ValueError(f"Unexpected response structure from Google AI: {response_data}")
            else: # OpenAI-like
                choices = response_data.get('choices', [])
                if not choices: raise ValueError(f"AI response is missing 'choices'. Full response: {response_data}")
                finish_reason = choices[0].get('finish_reason')
                if finish_reason == 'length': raise ValueError("AI stopped generating due to maximum token limit. The input SQL is too large.")
                if 'message' in choices[0] and 'content' in choices[0]['message']: generated_text = choices[0]['message']['content'].strip()
                else: raise ValueError(f"Unexpected response structure from AI provider: {response_data}")

            generated_text = re.sub(r'^```sql\n|```$', '', generated_text, flags=re.MULTILINE).strip()
            if not generated_text: raise ValueError("AI returned an empty response.")

            metrics = { 'status': 'success', 'tokens_used': response_data.get('usage', {}).get('total_tokens', 0) if 'usage' in response_data else response_data.get('usageMetadata', {}).get('totalTokenCount', 0) }
            return generated_text, metrics
        except requests.exceptions.Timeout:
            logger.error("AI request timed out.")
            raise ValueError('AI service request timed out.')
        
    def validate_sql(self, sql, pg_dsn):
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
                missing_relation_match = re.search(r'relation "(\w+)" does not exist', error_message)
                
                if missing_relation_match:
                    object_name = missing_relation_match.group(1)
                    logger.info(f"Attempt {attempt + 1}/{max_retries}: Missing relation '{object_name}'. Asking AI for DDL.")
                    ddl_to_execute = self._get_ddl_from_ai(current_sql, error_message)
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
                else: # Attempt to fix the query itself
                    logger.info(f"Attempt {attempt + 1}/{max_retries}: Non-relation error encountered. Asking AI to fix query.")
                    new_sql = self._get_query_fix_from_ai(current_sql, error_message)
                    if not new_sql:
                         return False, f"Validation failed: AI could not fix the query error: {error_message}", None
                    logger.info("AI provided a potential query fix. Retrying validation with the new query.")
                    current_sql = new_sql

        return False, f"Validation failed after {max_retries} attempts.", None

    def save_corrected_file(self, original_sql, corrected_sql):
        output_path = os.path.join(self.output_dir, 'corrected_output.sql')
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(corrected_sql)
            logger.info(f"Corrected SQL saved to {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"Failed to save corrected SQL: {e}")
            raise


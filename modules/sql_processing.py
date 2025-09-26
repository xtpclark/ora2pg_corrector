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

    def load_sql_file(self, filename):
        """Loads SQL content from a given file."""
        file_path = os.path.join(self.output_dir, filename)
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            logger.info(f"Loaded SQL content from {file_path}")
            return {'logs': f'Successfully loaded content from {filename}.', 'sql_content': sql_content}
        except Exception as e:
            logger.error(f"Error loading SQL file {file_path}: {e}")
            return {'logs': f'Failed to load file: {str(e)}', 'sql_content': ''}

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
            api_key = self.ai_settings.get('ai_api_key')
            api_endpoint = self.ai_settings.get('ai_endpoint')
            ai_model = self.ai_settings.get('ai_model')

            if not all([api_key, api_endpoint, ai_model]):
                 return sql, {'status': 'error', 'error_message': 'AI settings are not fully configured.', 'tokens_used': 0}

            headers = {}
            
            if "generativelanguage.googleapis.com" in api_endpoint:
                model_name = ai_model.replace('-latest', '')
                api_url = f"{api_endpoint.rstrip('/')}/models/{model_name}:generateContent?key={api_key}"
                payload = {
                    "contents": [{"parts": [{"text": full_prompt}]}],
                    "systemInstruction": {"parts": [{"text": system_instruction}]},
                    "generationConfig": {
                        "temperature": float(self.ai_settings.get('ai_temperature', 0.2)),
                        "maxOutputTokens": int(self.ai_settings.get('ai_max_output_tokens', 8192))
                    }
                }
            else:
                api_url = f"{api_endpoint.rstrip('/')}/chat/completions"
                headers['Authorization'] = f'Bearer {api_key}'
                payload = {
                    "model": ai_model,
                    "messages": [{"role": "system", "content": system_instruction}, {"role": "user", "content": full_prompt}],
                    "temperature": float(self.ai_settings.get('ai_temperature', 0.2)),
                    "max_tokens": int(self.ai_settings.get('ai_max_output_tokens', 4096)),
                }
            
            # Added a 120-second timeout to the request
            response = requests.post(api_url, json=payload, headers=headers, timeout=120)
            response.raise_for_status()

            if "generativelanguage.googleapis.com" in api_endpoint:
                corrected_sql = response.json()['candidates'][0]['content']['parts'][0]['text'].strip()
            else:
                corrected_sql = response.json()['choices'][0]['message']['content'].strip()
                
            corrected_sql = re.sub(r'^```sql\n|```$', '', corrected_sql, flags=re.MULTILINE)
            metrics = {
                'status': 'success',
                'tokens_used': response.json().get('usage', {}).get('total_tokens', 0) if 'usage' in response.json() else 0
            }
            return corrected_sql, metrics
        except requests.exceptions.Timeout:
            logger.error("AI correction failed: The request timed out.")
            return sql, {'status': 'error', 'error_message': 'The AI service took too long to respond.', 'tokens_used': 0}
        except Exception as e:
            logger.error(f"AI correction failed: {e}")
            return sql, {'status': 'error', 'error_message': str(e), 'tokens_used': 0}

    def validate_sql(self, sql, pg_dsn):
        """Validate the provided SQL against a PostgreSQL database."""
        try:
            conn = psycopg2.connect(pg_dsn)
            cursor = conn.cursor()
            conn.set_session(autocommit=True)
            cursor.execute("SET client_min_messages TO WARNING")
            cursor.execute(sql)
            cursor.close()
            conn.close()
            logger.info("SQL validation successful.")
            return True, "Validation successful"
        except Exception as e:
            logger.error(f"SQL validation failed: {e}")
            return False, f"Validation failed: {str(e)}"

    def save_corrected_file(self, original_sql, corrected_sql):
        """Saves the corrected SQL to a file."""
        output_path = os.path.join(self.output_dir, 'corrected_output.sql')
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(corrected_sql)
            logger.info(f"Corrected SQL saved to {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"Failed to save corrected SQL: {e}")
            raise


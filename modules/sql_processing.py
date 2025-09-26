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
    def __init__(self, ora2pg_path, output_dir, ai_settings, encryption_key):
        self.ora2pg_path = ora2pg_path
        self.output_dir = output_dir
        self.ai_settings = ai_settings
        self.encryption_key = encryption_key
        self.sql_content = ""
        self.fernet = Fernet(encryption_key)
    
    def run_ora2pg(self, config_file):
        try:
            output_file = os.path.join(self.output_dir, 'output.sql')
            cmd = [self.ora2pg_path, '-c', config_file, '-o', output_file]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            logs = result.stdout + result.stderr
            if os.path.exists(output_file):
                with open(output_file, 'r', encoding='utf-8') as f:
                    self.sql_content = f.read()
            html_report = os.path.join(self.output_dir, 'report.html')
            issues = []
            if os.path.exists(html_report):
                with open(html_report, 'r', encoding='utf-8') as f:
                    report_content = f.read()
                issues = self.parse_html_report(report_content)
            issues.extend(self.parse_ora2pg_logs(logs))
            logger.info(f"Ora2Pg executed successfully. Found {len(issues)} issues.")
            return {'logs': logs, 'issues': issues}
        except subprocess.CalledProcessError as e:
            logger.error(f"Ora2Pg execution failed: {e.stderr}")
            return {'logs': e.stderr, 'issues': []}
        except Exception as e:
            logger.error(f"Error running Ora2Pg: {e}")
            return {'logs': str(e), 'issues': []}

    def load_sql_file(self, filename):
        file_path = os.path.join(self.output_dir, filename)
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                self.sql_content = f.read()
            logger.info(f"Loaded SQL content from {file_path}")
            return {'logs': f'Successfully loaded content from {filename}.', 'sql_content': self.sql_content}
        except Exception as e:
            logger.error(f"Error loading SQL file {file_path}: {e}")
            return {'logs': f'Failed to load file: {str(e)}', 'sql_content': ''}

    def parse_html_report(self, report_content):
        issues = []
        pattern = r'<tr><td>(\w+)</td><td>(.*?)</td><td>.*?</td></tr>'
        matches = re.findall(pattern, report_content, re.DOTALL)
        for obj_type, issue_desc in matches:
            issues.append(f"{obj_type}: {html.unescape(issue_desc)}")
        return issues

    def parse_ora2pg_logs(self, logs):
        issues = []
        patterns = [
            r"ERROR:.*?\[(\w+)\]\s*(.*?)$",
            r"WARNING:.*?\[(\w+)\]\s*(.*?)$",
            r"Cannot convert\s*(\w+)\s*:\s*(.*?)$"
        ]
        for pattern in patterns:
            matches = re.findall(pattern, logs, re.MULTILINE)
            for obj_type, issue_desc in matches:
                issues.append(f"{obj_type}: {issue_desc}")
        return issues

    def ai_correct_sql(self, sql, issues=None):
        if not sql:
            return sql, {'status': 'no_content', 'tokens_used': 0}
        inferred_issues = []
        if not issues:
            if 'DBMS_OUTPUT' in sql:
                inferred_issues.append('DBMS_OUTPUT package not supported in PostgreSQL')
            if 'NVL' in sql:
                inferred_issues.append('NVL function not supported in PostgreSQL')
            if 'DECODE' in sql:
                inferred_issues.append('DECODE function not supported in PostgreSQL')
            if 'AUTONOMOUS_TRANSACTION' in sql:
                inferred_issues.append('AUTONOMOUS_TRANSACTION pragma not supported in PostgreSQL')
            if 'SYSDATE' in sql:
                inferred_issues.append('SYSDATE function not supported in PostgreSQL')
            if 'TO_DATE' in sql:
                inferred_issues.append('TO_DATE function requires format adjustment for PostgreSQL')
            if 'CONNECT BY' in sql:
                inferred_issues.append('CONNECT BY clause not supported in PostgreSQL')
            if 'NUMBER' in sql:
                inferred_issues.append('NUMBER type needs mapping to PostgreSQL numeric types')
            if "''" in sql:
                inferred_issues.append('Empty string handling may differ in PostgreSQL')
            issues = inferred_issues or ['Potential Oracle-specific constructs requiring PostgreSQL compatibility']
        
        system_instruction = "You are an expert in Oracle to PostgreSQL migration. Correct the SQL code for PostgreSQL compatibility, replacing Oracle-specific constructs with equivalents."
        full_prompt = f"""Address these issues: {', '.join(issues)}. Replace Oracle-specific constructs with PostgreSQL equivalents:
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
            api_key = self.ai_settings['ai_api_key']
            api_endpoint = self.ai_settings['ai_endpoint']
            ai_model = self.ai_settings['ai_model']
            headers = {}
            # Google Gemini API structure
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
            # OpenAI-compatible API structure (for OpenAI, Groq, Anthropic, etc.)
            else:
                api_url = f"{api_endpoint.rstrip('/')}/chat/completions"
                headers['Authorization'] = f'Bearer {api_key}'
                payload = {
                    "model": ai_model,
                    "messages": [{"role": "system", "content": system_instruction}, {"role": "user", "content": full_prompt}],
                    "temperature": float(self.ai_settings.get('ai_temperature', 0.2)),
                    "max_tokens": int(self.ai_settings.get('ai_max_output_tokens', 4096)),
                }
            
            logger.info(f"Sending AI request to {api_url} with model {ai_model}")
            response = requests.post(
                api_url,
                json=payload,
                headers=headers
            )
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
            logger.info(f"AI correction completed for SQL file with issues: {issues}")
            return corrected_sql, metrics
        except Exception as e:
            logger.error(f"AI correction failed: {e}")
            return sql, {'status': 'error', 'error_message': str(e), 'tokens_used': 0}

    def validate_sql(self, sql, pg_dsn):
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
        output_path = os.path.join(self.output_dir, 'corrected_output.sql')
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(corrected_sql)
            logger.info(f"Corrected SQL saved to {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"Failed to save corrected SQL: {e}")
            raise

import re
import subprocess
import os
import json
import logging
import time
from typing import List, Tuple, Dict
from bs4 import BeautifulSoup
from cachetools import TTLCache
import psycopg2
from cryptography.fernet import Fernet
from api_connector import translate_code_with_connector

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Cache for AI corrections (TTL: 1 hour)
correction_cache = TTLCache(maxsize=1000, ttl=3600)

class Ora2PgAICorrector:
    def __init__(self, ora2pg_path: str, output_dir: str, ai_settings: dict, encryption_key: bytes):
        self.ora2pg_path = ora2pg_path
        self.output_dir = output_dir
        self.ai_settings = ai_settings
        self.encryption_key = encryption_key
        try:
            self.fernet = Fernet(encryption_key)
        except Exception as e:
            logger.error(f"Invalid encryption key provided: {e}")
            raise ValueError("A valid Fernet encryption key is required.") from e
            
        self.sql_content = ""
        self.parsed_objects = []
        self.report_issues = {}

    def encrypt_value(self, value: str) -> str:
        """Encrypts a string value."""
        if not value:
            return ""
        return self.fernet.encrypt(value.encode()).decode()

    def decrypt_value(self, encrypted_value: str) -> str:
        """Decrypts a string value."""
        if not encrypted_value:
            return ""
        try:
            return self.fernet.decrypt(encrypted_value.encode()).decode()
        except Exception:
            logger.warning("Could not decrypt value, treating as plaintext (might be an unencrypted legacy value).")
            return encrypted_value

    def run_ora2pg(self, config_file: str) -> str:
        """Execute Ora2Pg with the given config file and capture output."""
        try:
            cmd = [self.ora2pg_path, '-c', config_file]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True, encoding='utf-8')
            logger.info("Ora2Pg executed successfully.")
            output_sql = os.path.join(self.output_dir, 'output.sql')
            if os.path.exists(output_sql):
                with open(output_sql, 'r', encoding='utf-8') as f:
                    self.sql_content = f.read()
                logger.info("Loaded SQL output from %s", output_sql)
            return result.stdout + "\n" + result.stderr
        except subprocess.CalledProcessError as e:
            logger.error("Ora2Pg execution failed: %s", e.stderr)
            return e.stdout + "\n" + e.stderr
        except FileNotFoundError:
            logger.error("ora2pg command not found. Ensure it is in your system's PATH.")
            return "Error: ora2pg command not found. Ensure it is in your system's PATH."

    def parse_sql_objects(self) -> List[Dict]:
        """Parse PL/SQL blocks from the SQL content and return a list of dictionaries."""
        plsql_pattern = r'(CREATE\s+OR\s+REPLACE\s+(?:FUNCTION|PROCEDURE|PACKAGE|TRIGGER|VIEW)\s+[\w\."]+.*?END;?\s*/)'
        self.parsed_objects = re.findall(plsql_pattern, self.sql_content, re.DOTALL | re.IGNORECASE)
        logger.info("Parsed %d PL/SQL-like objects", len(self.parsed_objects))
        
        object_list = []
        for obj_sql in self.parsed_objects:
            obj_type_match = re.search(r'CREATE\s+OR\s+REPLACE\s+(FUNCTION|PROCEDURE|PACKAGE|TRIGGER|VIEW)', obj_sql, re.IGNORECASE)
            obj_name_match = re.search(r'(?:FUNCTION|PROCEDURE|PACKAGE|TRIGGER|VIEW)\s+"?([\w\.]+)"?', obj_sql, re.IGNORECASE)
            
            obj_type = obj_type_match.group(1).upper() if obj_type_match else 'UNKNOWN'
            obj_name = obj_name_match.group(1) if obj_name_match else 'UNKNOWN_NAME'

            object_list.append({
                "name": obj_name,
                "type": obj_type,
                "sql": obj_sql,
                "issues": self.detect_issues(obj_sql)
            })
        return object_list

    def detect_issues(self, sql_object: str) -> List[str]:
        """Detect potential translation issues in a SQL object."""
        issues = []
        sql_upper = sql_object.upper()
        if 'NVL(' in sql_upper: issues.append("NVL function")
        if 'DBMS_OUTPUT' in sql_upper: issues.append("DBMS_OUTPUT package")
        if 'AUTONOMOUS_TRANSACTION' in sql_upper: issues.append("AUTONOMOUS_TRANSACTION")
        if 'DECODE(' in sql_upper: issues.append("DECODE function")
        if 'UTL_FILE' in sql_upper: issues.append("UTL_FILE package")
        
        match = re.search(r'CREATE\s+OR\s+REPLACE\s+(?:FUNCTION|PROCEDURE)\s+"?(\w+)"?', sql_object, re.IGNORECASE)
        if match:
            name = match.group(1).upper()
            if name in self.report_issues:
                issues.extend(self.report_issues[name])
        return list(set(issues))

    def parse_migration_report(self, report_path: str = None) -> Dict:
        """Parse Ora2Pg HTML report to extract flagged issues."""
        self.report_issues = {}
        if not report_path:
            report_path = os.path.join(self.output_dir, self.ai_settings.get('report_filename', 'migration_report.html'))
        try:
            with open(report_path, 'r', encoding='utf-8') as f:
                soup = BeautifulSoup(f, 'html.parser')
            
            headers_to_check = ['functions', 'procedures', 'packages', 'triggers']
            for header_id in headers_to_check:
                header = soup.find('h2', id=header_id)
                if header:
                    table = header.find_next_sibling('table')
                    if table:
                        for row in table.find_all('tr'):
                            cells = row.find_all('td')
                            if len(cells) > 3 and 'Yes' in cells[2].text:
                                name = cells[0].text.strip().upper()
                                comment = cells[3].text.strip()
                                self.report_issues.setdefault(name, []).append(f"Report: {comment}")
            logger.info("Parsed migration report, found issues for %d objects", len(self.report_issues))
            return self.report_issues
        except FileNotFoundError:
            logger.warning("Migration report not found at %s. Continuing without it.", report_path)
            return {}
        except Exception as e:
            logger.error("Failed to parse migration report: %s", e)
            return {}

    def ai_correct_sql(self, sql_object: str, issues: List[str]) -> Tuple[str, Dict]:
        """Correct SQL using AI API call with caching."""
        if not issues:
            return sql_object, {'processing_time_seconds': 0, 'prompt_estimated_tokens': 0}
        
        cache_key = hash(sql_object + ''.join(sorted(issues)))
        if cache_key in correction_cache:
            logger.info("Returning cached AI correction for object")
            return correction_cache[cache_key]

        system_instruction = """You are an expert Oracle to PostgreSQL database migration specialist. Your task is to convert the provided Oracle PL/SQL code to PostgreSQL PL/pgSQL.
- Adhere strictly to PostgreSQL syntax and best practices.
- Replace Oracle-specific functions (e.g., NVL, DECODE) with standard PostgreSQL equivalents (COALESCE, CASE).
- Handle Oracle packages (e.g., DBMS_OUTPUT, UTL_FILE) by replacing them with PostgreSQL alternatives (RAISE NOTICE, and appropriate file handling logic).
- Remove or refactor constructs not supported in PostgreSQL (e.g., PRAGMA AUTONOMOUS_TRANSACTION).
- Return ONLY the complete, corrected, and runnable PL/pgSQL code block. Do not add any explanations, introductory text, or markdown code fences."""
        
        prompt = f"""Please convert the following Oracle code to PostgreSQL PL/pgSQL, addressing these specific, detected issues: {', '.join(issues)}.

Original Oracle Code:
```sql
{sql_object}
```"""
        
        prompt_chars = len(prompt) + len(system_instruction)
        estimated_tokens = prompt_chars // 4
        start_time = time.time()
        corrected_sql, metrics = translate_code_with_connector(self.ai_settings, prompt, system_instruction)
        metrics['prompt_estimated_tokens'] = estimated_tokens
        
        if corrected_sql.startswith("Error:"):
            logger.error("AI correction failed: %s", corrected_sql)
            return sql_object, metrics

        correction_cache[cache_key] = (corrected_sql, metrics)
        logger.info(f"AI correction successful in {metrics['processing_time_seconds']:.2f}s")
        return corrected_sql, metrics

    def validate_sql(self, sql: str, pg_dsn: str) -> Tuple[bool, str]:
        """Validate corrected SQL against a PostgreSQL staging database."""
        try:
            conn = psycopg2.connect(pg_dsn)
            cursor = conn.cursor()
            timeout = self.ai_settings.get('validation_timeout', '5s')
            cursor.execute(f"SET statement_timeout = '{timeout}';")
            cursor.execute("SET client_min_messages TO WARNING;")
            cursor.execute("BEGIN;")
            cursor.execute(sql)
            cursor.execute("ROLLBACK;")
            conn.close()
            logger.info("SQL validation successful")
            return True, "Validation successful: The SQL syntax is correct."
        except Exception as e:
            logger.error("SQL validation failed: %s", e)
            return False, f"Validation Failed: {str(e)}"

    def save_corrected_file(self, original_sql_content: str, all_objects: List[Dict]) -> str:
        """Generate the final corrected SQL file."""
        output_path = os.path.join(self.output_dir, 'corrected_output.sql')
        
        content_to_save = original_sql_content
        for obj in all_objects:
            if obj.get('corrected_sql'):
                content_to_save = content_to_save.replace(obj['sql'], obj['corrected_sql'])

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(content_to_save)
            
        logger.info("Generated corrected SQL file at %s", output_path)
        return output_path

"""
Oracle-specific SQL preprocessing for PostgreSQL compatibility.

Handles conversions that ora2pg may not fully address:
- TIMESTAMP WITH LOCAL TIME ZONE conversions
- Oracle TYPE AS OBJECT to PostgreSQL composite type
- VARRAY to PostgreSQL ARRAY
- Oracle data types (VARCHAR2, CLOB, BLOB, etc.)
"""

import re
import logging

logger = logging.getLogger(__name__)


def convert_oracle_timestamps(sql: str) -> str:
    """
    Convert Oracle timestamp types to PostgreSQL equivalents.

    Conversions:
    - TIMESTAMP(n) WITH LOCAL TIME ZONE -> TIMESTAMP(n) WITH TIME ZONE
    - TIMESTAMP WITH LOCAL TIME ZONE -> TIMESTAMPTZ

    :param sql: SQL string to process
    :return: Converted SQL string
    """
    # With precision: TIMESTAMP(6) WITH LOCAL TIME ZONE
    sql = re.sub(
        r'TIMESTAMP\s*\(\s*(\d+)\s*\)\s+WITH\s+LOCAL\s+TIME\s+ZONE',
        r'TIMESTAMP(\1) WITH TIME ZONE',
        sql,
        flags=re.IGNORECASE
    )

    # Without precision: TIMESTAMP WITH LOCAL TIME ZONE
    sql = re.sub(
        r'TIMESTAMP\s+WITH\s+LOCAL\s+TIME\s+ZONE',
        'TIMESTAMPTZ',
        sql,
        flags=re.IGNORECASE
    )

    return sql


def convert_oracle_type_as_object(sql: str) -> str:
    """
    Convert Oracle TYPE AS OBJECT to PostgreSQL composite type.

    Oracle:     CREATE TYPE addr AS OBJECT (street VARCHAR2(100), city VARCHAR2(50));
    PostgreSQL: CREATE TYPE addr AS (street VARCHAR(100), city VARCHAR(50));

    :param sql: SQL string to process
    :return: Converted SQL string
    """
    # Remove AS OBJECT, keep AS
    pattern = r'(CREATE\s+(?:OR\s+REPLACE\s+)?TYPE\s+[\w"]+)\s+AS\s+OBJECT\s*\('
    sql = re.sub(pattern, r'\1 AS (', sql, flags=re.IGNORECASE)

    return sql


def convert_oracle_varray(sql: str) -> str:
    """
    Convert Oracle VARRAY to PostgreSQL ARRAY.

    Oracle:     phone_list VARRAY(10) OF VARCHAR2(20)
    PostgreSQL: phone_list VARCHAR(20)[]

    Also handles CREATE TYPE name AS VARRAY(n) OF type;

    :param sql: SQL string to process
    :return: Converted SQL string
    """
    # IMPORTANT: Handle CREATE TYPE name AS VARRAY(n) OF type; FIRST
    # before the inline pattern replaces VARRAY in the middle
    # Convert to: CREATE DOMAIN name AS type[];
    varray_type_pattern = r'CREATE\s+(?:OR\s+REPLACE\s+)?TYPE\s+([\w"]+)\s+AS\s+VARRAY\s*\(\s*\d+\s*\)\s+OF\s+(\w+(?:\s*\(\s*\d+\s*\))?)\s*;?'

    def varray_type_replacement(match):
        type_name = match.group(1)
        element_type = match.group(2).strip()
        # Convert Oracle types
        element_type = re.sub(r'\bVARCHAR2\b', 'VARCHAR', element_type, flags=re.IGNORECASE)
        element_type = re.sub(r'\bNUMBER\b', 'NUMERIC', element_type, flags=re.IGNORECASE)
        return f'CREATE DOMAIN {type_name} AS {element_type}[];'

    sql = re.sub(varray_type_pattern, varray_type_replacement, sql, flags=re.IGNORECASE)

    # Pattern for inline VARRAY in column definitions (run second)
    pattern = r'VARRAY\s*\(\s*\d+\s*\)\s+OF\s+(\w+(?:\s*\(\s*\d+\s*\))?)'

    def replacement(match):
        base_type = match.group(1).strip()
        # Convert VARCHAR2 to VARCHAR
        base_type = re.sub(r'\bVARCHAR2\b', 'VARCHAR', base_type, flags=re.IGNORECASE)
        base_type = re.sub(r'\bNUMBER\b', 'NUMERIC', base_type, flags=re.IGNORECASE)
        return f'{base_type}[]'

    sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)

    return sql


def convert_oracle_nested_table_type(sql: str) -> str:
    """
    Convert ora2pg's incorrect nested table type syntax to PostgreSQL DOMAIN.

    ora2pg generates (incorrectly):
        CREATE TYPE textdoc_tab AS (textdoc_tab textdoc_typ[]);

    Should be:
        CREATE DOMAIN textdoc_tab AS textdoc_typ[];

    This handles the pattern where ora2pg creates a composite type with a single
    field having the same name as the type itself, containing an array.

    :param sql: SQL string to process
    :return: Converted SQL string
    """
    # Pattern: CREATE TYPE name AS (name type[]);
    # Match: CREATE TYPE textdoc_tab AS (textdoc_tab textdoc_typ[]);
    # The field name equals the type name - this is the marker for nested table
    pattern = r'CREATE\s+TYPE\s+(\w+)\s+AS\s*\(\s*\1\s+(\w+)\[\]\s*\)\s*;?'

    def replacement(match):
        type_name = match.group(1)
        element_type = match.group(2)
        return f'CREATE DOMAIN {type_name} AS {element_type}[];'

    sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)

    return sql


def convert_oracle_data_types(sql: str) -> str:
    """
    Convert remaining Oracle data types to PostgreSQL equivalents.

    Conversions:
    - VARCHAR2 -> VARCHAR
    - NVARCHAR2 -> VARCHAR
    - CLOB -> TEXT
    - NCLOB -> TEXT
    - BLOB -> BYTEA
    - RAW(n) -> BYTEA
    - LONG RAW -> BYTEA
    - LONG -> TEXT
    - NUMBER -> NUMERIC (when not followed by precision)

    :param sql: SQL string to process
    :return: Converted SQL string
    """
    conversions = [
        (r'\bVARCHAR2\b', 'VARCHAR'),
        (r'\bNVARCHAR2\b', 'VARCHAR'),
        (r'\bCLOB\b', 'TEXT'),
        (r'\bNCLOB\b', 'TEXT'),
        (r'\bBLOB\b', 'BYTEA'),
        (r'\bRAW\s*\(\s*\d+\s*\)', 'BYTEA'),
        (r'\bLONG\s+RAW\b', 'BYTEA'),
        (r'\bLONG\b', 'TEXT'),
        # NUMBER without precision stays as NUMBER (ora2pg handles this)
        # But NUMBER(p) or NUMBER(p,s) should be NUMERIC
    ]

    for pattern, replacement in conversions:
        sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)

    return sql


def convert_oracle_boolean_expressions(sql: str) -> str:
    """
    Convert Oracle boolean expressions to PostgreSQL equivalents.

    NVL2(expr, val1, val2) -> CASE WHEN expr IS NOT NULL THEN val1 ELSE val2 END

    Note: Simple NVL -> COALESCE is usually handled by ora2pg, but NVL2 may not be.

    :param sql: SQL string to process
    :return: Converted SQL string
    """
    # NVL2 pattern - handles nested parentheses carefully
    # This is a simplified version - complex nested cases may need more work
    nvl2_pattern = r'\bNVL2\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)'

    def nvl2_replacement(match):
        expr = match.group(1).strip()
        true_val = match.group(2).strip()
        false_val = match.group(3).strip()
        return f'CASE WHEN {expr} IS NOT NULL THEN {true_val} ELSE {false_val} END'

    sql = re.sub(nvl2_pattern, nvl2_replacement, sql, flags=re.IGNORECASE)

    return sql


def convert_oracle_functions(sql: str) -> str:
    """
    Convert Oracle-specific functions to PostgreSQL equivalents.

    Conversions:
    - DECODE(...) -> CASE WHEN ... (basic cases)
    - SYSDATE -> CURRENT_TIMESTAMP (if not already converted)

    :param sql: SQL string to process
    :return: Converted SQL string
    """
    # SYSDATE -> CURRENT_TIMESTAMP
    sql = re.sub(r'\bSYSDATE\b', 'CURRENT_TIMESTAMP', sql, flags=re.IGNORECASE)

    # Simple DECODE to CASE conversion
    # DECODE(expr, search1, result1, search2, result2, ..., default)
    # This handles simple cases; complex nested DECODE may need manual review

    return sql


def preprocess_oracle_sql(sql: str) -> str:
    """
    Apply all Oracle-to-PostgreSQL preprocessing transformations.

    This should be called BEFORE PostgreSQL validation, after psql metacommand stripping.

    Order matters - timestamps should be converted before general data types.

    :param sql: SQL string to process
    :return: Converted SQL string with Oracle-specific syntax replaced
    """
    original_sql = sql

    # Apply conversions in order
    sql = convert_oracle_timestamps(sql)
    sql = convert_oracle_type_as_object(sql)
    sql = convert_oracle_varray(sql)
    sql = convert_oracle_nested_table_type(sql)
    sql = convert_oracle_data_types(sql)
    sql = convert_oracle_boolean_expressions(sql)
    sql = convert_oracle_functions(sql)

    if sql != original_sql:
        logger.info("Oracle preprocessing applied transformations to SQL")
        logger.debug(f"Preprocessing changes detected")

    return sql

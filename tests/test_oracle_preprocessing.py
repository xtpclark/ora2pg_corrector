"""
Tests for the Oracle preprocessing module (modules/oracle_preprocessing.py).

Tests Oracle-to-PostgreSQL SQL transformations that handle ora2pg gaps.
"""

import pytest
from modules.oracle_preprocessing import (
    convert_oracle_timestamps,
    convert_oracle_type_as_object,
    convert_oracle_varray,
    convert_oracle_nested_table_type,
    convert_oracle_data_types,
    convert_oracle_boolean_expressions,
    convert_oracle_functions,
    preprocess_oracle_sql
)


class TestTimestampConversion:
    """Test TIMESTAMP WITH LOCAL TIME ZONE conversions."""

    def test_timestamp_with_precision_local_tz(self):
        """TIMESTAMP(6) WITH LOCAL TIME ZONE -> TIMESTAMP(6) WITH TIME ZONE"""
        sql = "order_date TIMESTAMP(6) WITH LOCAL TIME ZONE NOT NULL"
        result = convert_oracle_timestamps(sql)
        assert result == "order_date TIMESTAMP(6) WITH TIME ZONE NOT NULL"

    def test_timestamp_no_precision_local_tz(self):
        """TIMESTAMP WITH LOCAL TIME ZONE -> TIMESTAMPTZ"""
        sql = "created_at TIMESTAMP WITH LOCAL TIME ZONE"
        result = convert_oracle_timestamps(sql)
        assert result == "created_at TIMESTAMPTZ"

    def test_timestamp_with_various_precisions(self):
        """Test different precision values."""
        for precision in [0, 3, 6, 9]:
            sql = f"col TIMESTAMP({precision}) WITH LOCAL TIME ZONE"
            result = convert_oracle_timestamps(sql)
            assert result == f"col TIMESTAMP({precision}) WITH TIME ZONE"

    def test_timestamp_case_insensitive(self):
        """Test case insensitivity."""
        sql = "col timestamp(6) with local time zone"
        result = convert_oracle_timestamps(sql)
        assert "WITH TIME ZONE" in result.upper()
        assert "LOCAL" not in result.upper()

    def test_timestamp_with_spaces(self):
        """Test various whitespace patterns."""
        sql = "col TIMESTAMP ( 6 )  WITH  LOCAL  TIME  ZONE"
        result = convert_oracle_timestamps(sql)
        assert "WITH TIME ZONE" in result
        assert "LOCAL" not in result

    def test_regular_timestamp_unchanged(self):
        """Regular TIMESTAMP should not be modified."""
        sql = "col TIMESTAMP(6)"
        result = convert_oracle_timestamps(sql)
        assert result == sql

    def test_timestamp_with_time_zone_unchanged(self):
        """TIMESTAMP WITH TIME ZONE (without LOCAL) should not be modified."""
        sql = "col TIMESTAMP(6) WITH TIME ZONE"
        result = convert_oracle_timestamps(sql)
        assert result == sql


class TestTypeAsObjectConversion:
    """Test TYPE AS OBJECT to composite type conversions."""

    def test_simple_type_as_object(self):
        """CREATE TYPE x AS OBJECT -> CREATE TYPE x AS"""
        sql = "CREATE TYPE addr AS OBJECT (street VARCHAR2(100), city VARCHAR2(50));"
        result = convert_oracle_type_as_object(sql)
        assert "AS OBJECT" not in result
        assert "CREATE TYPE addr AS (" in result

    def test_type_with_or_replace(self):
        """CREATE OR REPLACE TYPE AS OBJECT"""
        sql = "CREATE OR REPLACE TYPE person_typ AS OBJECT (name VARCHAR2(50));"
        result = convert_oracle_type_as_object(sql)
        assert "AS OBJECT" not in result
        assert "CREATE OR REPLACE TYPE person_typ AS (" in result

    def test_type_with_quoted_name(self):
        """Type name in quotes."""
        sql = 'CREATE TYPE "MyType" AS OBJECT (id NUMBER);'
        result = convert_oracle_type_as_object(sql)
        assert "AS OBJECT" not in result
        assert 'CREATE TYPE "MyType" AS (' in result

    def test_case_insensitive(self):
        """Test case insensitivity."""
        sql = "create type MyType as object (id number)"
        result = convert_oracle_type_as_object(sql)
        assert "OBJECT" not in result.upper() or "AS OBJECT" not in result.upper()


class TestVarrayConversion:
    """Test VARRAY to PostgreSQL array conversions."""

    def test_simple_varray(self):
        """VARRAY(n) OF type -> type[]"""
        sql = "phone_list VARRAY(10) OF VARCHAR2(20)"
        result = convert_oracle_varray(sql)
        assert "VARRAY" not in result
        assert "VARCHAR(20)[]" in result

    def test_varray_with_number(self):
        """VARRAY of NUMBER."""
        sql = "scores VARRAY(100) OF NUMBER"
        result = convert_oracle_varray(sql)
        assert "NUMERIC[]" in result

    def test_create_type_as_varray(self):
        """CREATE TYPE name AS VARRAY -> CREATE DOMAIN"""
        sql = "CREATE TYPE phone_list_typ AS VARRAY(5) OF VARCHAR2(25);"
        result = convert_oracle_varray(sql)
        assert "VARRAY" not in result
        assert "CREATE DOMAIN phone_list_typ AS VARCHAR(25)[]" in result

    def test_varray_case_insensitive(self):
        """Test case insensitivity."""
        sql = "col varray(10) of varchar2(50)"
        result = convert_oracle_varray(sql)
        assert "varray" not in result.lower()


class TestNestedTableTypeConversion:
    """Test ora2pg nested table type conversion (TABLE OF)."""

    def test_nested_table_type_basic(self):
        """Test basic nested table type from ora2pg output."""
        sql = "CREATE TYPE textdoc_tab AS (textdoc_tab textdoc_typ[]);"
        result = convert_oracle_nested_table_type(sql)
        assert "CREATE DOMAIN textdoc_tab AS textdoc_typ[]" in result
        assert "CREATE TYPE" not in result

    def test_nested_table_type_no_semicolon(self):
        """Test without trailing semicolon."""
        sql = "CREATE TYPE my_tab AS (my_tab element_typ[])"
        result = convert_oracle_nested_table_type(sql)
        assert "CREATE DOMAIN my_tab AS element_typ[]" in result

    def test_nested_table_type_case_insensitive(self):
        """Test case insensitivity."""
        sql = "create type ITEMS_TAB as (items_tab item_type[])"
        result = convert_oracle_nested_table_type(sql)
        assert "DOMAIN" in result or "domain" in result.lower()
        assert "item_type[]" in result.lower()

    def test_non_matching_not_converted(self):
        """Regular composite type should not be converted."""
        sql = "CREATE TYPE person AS (name varchar, age int);"
        result = convert_oracle_nested_table_type(sql)
        # Should remain unchanged - field names don't match type name
        assert result == sql


class TestDataTypeConversion:
    """Test Oracle data type conversions."""

    def test_varchar2_to_varchar(self):
        """VARCHAR2 -> VARCHAR"""
        sql = "name VARCHAR2(100)"
        result = convert_oracle_data_types(sql)
        assert "VARCHAR(100)" in result
        assert "VARCHAR2" not in result

    def test_nvarchar2_to_varchar(self):
        """NVARCHAR2 -> VARCHAR"""
        sql = "name NVARCHAR2(50)"
        result = convert_oracle_data_types(sql)
        assert "VARCHAR(50)" in result
        assert "NVARCHAR2" not in result

    def test_clob_to_text(self):
        """CLOB -> TEXT"""
        sql = "description CLOB"
        result = convert_oracle_data_types(sql)
        assert "TEXT" in result
        assert "CLOB" not in result

    def test_nclob_to_text(self):
        """NCLOB -> TEXT"""
        sql = "content NCLOB"
        result = convert_oracle_data_types(sql)
        assert "TEXT" in result
        assert "NCLOB" not in result

    def test_blob_to_bytea(self):
        """BLOB -> BYTEA"""
        sql = "data BLOB"
        result = convert_oracle_data_types(sql)
        assert "BYTEA" in result
        assert "BLOB" not in result

    def test_raw_to_bytea(self):
        """RAW(n) -> BYTEA"""
        sql = "hash RAW(16)"
        result = convert_oracle_data_types(sql)
        assert "BYTEA" in result
        assert "RAW" not in result

    def test_long_raw_to_bytea(self):
        """LONG RAW -> BYTEA"""
        sql = "data LONG RAW"
        result = convert_oracle_data_types(sql)
        assert "BYTEA" in result
        assert "LONG RAW" not in result

    def test_long_to_text(self):
        """LONG -> TEXT"""
        sql = "content LONG"
        result = convert_oracle_data_types(sql)
        assert "TEXT" in result

    def test_multiple_types_in_table(self):
        """Multiple Oracle types in one statement."""
        sql = """CREATE TABLE test (
            id NUMBER,
            name VARCHAR2(100),
            description CLOB,
            data BLOB
        );"""
        result = convert_oracle_data_types(sql)
        assert "VARCHAR(100)" in result
        assert "TEXT" in result
        assert "BYTEA" in result
        assert "VARCHAR2" not in result
        assert "CLOB" not in result
        assert "BLOB" not in result


class TestBooleanExpressionConversion:
    """Test NVL2 and other boolean expression conversions."""

    def test_nvl2_simple(self):
        """NVL2(expr, val1, val2) -> CASE WHEN"""
        sql = "SELECT NVL2(col, 'yes', 'no') FROM t"
        result = convert_oracle_boolean_expressions(sql)
        assert "NVL2" not in result
        assert "CASE WHEN col IS NOT NULL THEN 'yes' ELSE 'no' END" in result

    def test_nvl2_with_expressions(self):
        """NVL2 with more complex expressions."""
        sql = "SELECT NVL2(salary, salary * 1.1, 0) FROM emp"
        result = convert_oracle_boolean_expressions(sql)
        assert "CASE WHEN salary IS NOT NULL THEN salary * 1.1 ELSE 0 END" in result


class TestFunctionConversion:
    """Test Oracle function conversions."""

    def test_sysdate_to_current_timestamp(self):
        """SYSDATE -> CURRENT_TIMESTAMP"""
        sql = "SELECT SYSDATE FROM dual"
        result = convert_oracle_functions(sql)
        assert "CURRENT_TIMESTAMP" in result
        assert "SYSDATE" not in result

    def test_sysdate_case_insensitive(self):
        """Test case insensitivity."""
        sql = "SELECT sysdate FROM dual"
        result = convert_oracle_functions(sql)
        assert "CURRENT_TIMESTAMP" in result


class TestFullPreprocessing:
    """Test the complete preprocessing pipeline."""

    def test_full_orders_table(self):
        """Test with realistic orders table from OE schema."""
        sql = """CREATE TABLE orders (
            order_id NUMBER(12) NOT NULL,
            order_date TIMESTAMP(6) WITH LOCAL TIME ZONE NOT NULL,
            order_mode VARCHAR2(8),
            customer_id NUMBER(6) NOT NULL,
            order_status NUMBER(2),
            order_total NUMBER(8,2)
        );"""
        result = preprocess_oracle_sql(sql)
        assert "WITH TIME ZONE" in result
        assert "LOCAL" not in result
        assert "VARCHAR(" in result
        assert "VARCHAR2" not in result

    def test_full_customers_table_with_types(self):
        """Test table with custom types."""
        sql = """CREATE TABLE customers (
            customer_id NUMBER(6) NOT NULL,
            cust_name VARCHAR2(50),
            cust_address cust_address_typ,
            phone_numbers phone_list_typ
        );"""
        result = preprocess_oracle_sql(sql)
        assert "VARCHAR(" in result
        assert "VARCHAR2" not in result
        # Type references should remain (they reference external types)
        assert "cust_address_typ" in result
        assert "phone_list_typ" in result

    def test_full_type_definitions(self):
        """Test complete type definition conversion."""
        sql = """CREATE TYPE cust_address_typ AS OBJECT (
            street_address VARCHAR2(40),
            city VARCHAR2(30),
            country_id CHAR(2)
        );"""
        result = preprocess_oracle_sql(sql)
        assert "AS OBJECT" not in result
        assert "CREATE TYPE cust_address_typ AS (" in result
        assert "VARCHAR(" in result

    def test_varray_type_definition(self):
        """Test VARRAY type definition."""
        sql = "CREATE TYPE phone_list_typ AS VARRAY(5) OF VARCHAR2(25);"
        result = preprocess_oracle_sql(sql)
        assert "CREATE DOMAIN phone_list_typ AS VARCHAR(25)[]" in result

    def test_preserves_regular_postgresql(self):
        """Ensure valid PostgreSQL is not modified incorrectly."""
        sql = """CREATE TABLE test (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            created_at TIMESTAMPTZ DEFAULT NOW()
        );"""
        result = preprocess_oracle_sql(sql)
        # Should remain largely unchanged (no Oracle-specific syntax)
        assert "SERIAL" in result
        assert "VARCHAR(100)" in result
        assert "TIMESTAMPTZ" in result

    def test_empty_string(self):
        """Empty string should return empty string."""
        assert preprocess_oracle_sql("") == ""

    def test_no_oracle_syntax(self):
        """SQL without Oracle syntax should pass through unchanged."""
        sql = "SELECT id, name FROM users WHERE active = true;"
        result = preprocess_oracle_sql(sql)
        assert result == sql

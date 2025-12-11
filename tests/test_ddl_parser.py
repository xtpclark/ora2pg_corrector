"""
Tests for the DDL parser module (modules/ddl_parser.py).
"""

import pytest


class TestParseDdlFile:
    """Test the parse_ddl_file function."""

    def test_parse_single_table(self):
        """Test parsing a single CREATE TABLE statement."""
        from modules.ddl_parser import parse_ddl_file

        sql = """CREATE TABLE employees (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);"""
        objects = parse_ddl_file(sql)

        assert len(objects) == 1
        assert objects[0]['object_name'] == 'employees'
        assert objects[0]['object_type'] == 'TABLE'
        assert objects[0]['line_start'] == 1
        assert 'CREATE TABLE employees' in objects[0]['ddl']

    def test_parse_multiple_tables(self):
        """Test parsing multiple CREATE TABLE statements."""
        from modules.ddl_parser import parse_ddl_file

        sql = """CREATE TABLE departments (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TABLE employees (
    id INTEGER PRIMARY KEY,
    dept_id INTEGER
);"""
        objects = parse_ddl_file(sql)

        assert len(objects) == 2
        assert objects[0]['object_name'] == 'departments'
        assert objects[1]['object_name'] == 'employees'

    def test_parse_table_if_not_exists(self):
        """Test parsing CREATE TABLE IF NOT EXISTS."""
        from modules.ddl_parser import parse_ddl_file

        sql = """CREATE TABLE IF NOT EXISTS test_table (
    id INTEGER PRIMARY KEY
);"""
        objects = parse_ddl_file(sql)

        assert len(objects) == 1
        assert objects[0]['object_name'] == 'test_table'
        assert objects[0]['object_type'] == 'TABLE'

    def test_parse_unlogged_table(self):
        """Test parsing CREATE UNLOGGED TABLE."""
        from modules.ddl_parser import parse_ddl_file

        sql = """CREATE UNLOGGED TABLE temp_data (
    id INTEGER
);"""
        objects = parse_ddl_file(sql)

        assert len(objects) == 1
        assert objects[0]['object_name'] == 'temp_data'
        assert objects[0]['object_type'] == 'TABLE'

    def test_parse_view(self):
        """Test parsing CREATE VIEW."""
        from modules.ddl_parser import parse_ddl_file

        sql = """CREATE VIEW employee_summary AS
SELECT id, name FROM employees;"""
        objects = parse_ddl_file(sql)

        assert len(objects) == 1
        assert objects[0]['object_name'] == 'employee_summary'
        assert objects[0]['object_type'] == 'VIEW'

    def test_parse_or_replace_view(self):
        """Test parsing CREATE OR REPLACE VIEW."""
        from modules.ddl_parser import parse_ddl_file

        sql = """CREATE OR REPLACE VIEW active_users AS
SELECT * FROM users WHERE active = true;"""
        objects = parse_ddl_file(sql)

        assert len(objects) == 1
        assert objects[0]['object_name'] == 'active_users'
        assert objects[0]['object_type'] == 'VIEW'

    def test_parse_materialized_view(self):
        """Test parsing CREATE MATERIALIZED VIEW."""
        from modules.ddl_parser import parse_ddl_file

        sql = """CREATE MATERIALIZED VIEW sales_summary AS
SELECT product_id, SUM(amount) FROM sales GROUP BY product_id;"""
        objects = parse_ddl_file(sql)

        assert len(objects) == 1
        assert objects[0]['object_name'] == 'sales_summary'
        assert objects[0]['object_type'] == 'VIEW'

    def test_parse_index(self):
        """Test parsing CREATE INDEX."""
        from modules.ddl_parser import parse_ddl_file

        sql = """CREATE INDEX idx_employee_name ON employees(name);"""
        objects = parse_ddl_file(sql)

        assert len(objects) == 1
        assert objects[0]['object_name'] == 'idx_employee_name'
        assert objects[0]['object_type'] == 'INDEX'

    def test_parse_unique_index(self):
        """Test parsing CREATE UNIQUE INDEX."""
        from modules.ddl_parser import parse_ddl_file

        sql = """CREATE UNIQUE INDEX idx_employee_email ON employees(email);"""
        objects = parse_ddl_file(sql)

        assert len(objects) == 1
        assert objects[0]['object_name'] == 'idx_employee_email'
        assert objects[0]['object_type'] == 'INDEX'

    def test_parse_index_if_not_exists(self):
        """Test parsing CREATE INDEX IF NOT EXISTS."""
        from modules.ddl_parser import parse_ddl_file

        sql = """CREATE INDEX IF NOT EXISTS idx_test ON test_table(col);"""
        objects = parse_ddl_file(sql)

        assert len(objects) == 1
        assert objects[0]['object_name'] == 'idx_test'
        assert objects[0]['object_type'] == 'INDEX'

    def test_parse_index_concurrently(self):
        """Test parsing CREATE INDEX CONCURRENTLY."""
        from modules.ddl_parser import parse_ddl_file

        sql = """CREATE INDEX CONCURRENTLY idx_large_table ON large_table(col);"""
        objects = parse_ddl_file(sql)

        assert len(objects) == 1
        assert objects[0]['object_name'] == 'idx_large_table'
        assert objects[0]['object_type'] == 'INDEX'

    def test_parse_sequence(self):
        """Test parsing CREATE SEQUENCE."""
        from modules.ddl_parser import parse_ddl_file

        sql = """CREATE SEQUENCE employee_id_seq START WITH 1 INCREMENT BY 1;"""
        objects = parse_ddl_file(sql)

        assert len(objects) == 1
        assert objects[0]['object_name'] == 'employee_id_seq'
        assert objects[0]['object_type'] == 'SEQUENCE'

    def test_parse_function(self):
        """Test parsing CREATE FUNCTION with dollar-quoted body."""
        from modules.ddl_parser import parse_ddl_file

        sql = """CREATE FUNCTION get_employee_name(emp_id INTEGER)
RETURNS VARCHAR AS $$
BEGIN
    RETURN (SELECT name FROM employees WHERE id = emp_id);
END;
$$ LANGUAGE plpgsql;"""
        objects = parse_ddl_file(sql)

        assert len(objects) == 1
        assert objects[0]['object_name'] == 'get_employee_name'
        assert objects[0]['object_type'] == 'FUNCTION'
        assert 'BEGIN' in objects[0]['ddl']
        assert 'END;' in objects[0]['ddl']

    def test_parse_or_replace_function(self):
        """Test parsing CREATE OR REPLACE FUNCTION."""
        from modules.ddl_parser import parse_ddl_file

        sql = """CREATE OR REPLACE FUNCTION calculate_tax(amount NUMERIC)
RETURNS NUMERIC AS $$
BEGIN
    RETURN amount * 0.1;
END;
$$ LANGUAGE plpgsql;"""
        objects = parse_ddl_file(sql)

        assert len(objects) == 1
        assert objects[0]['object_name'] == 'calculate_tax'
        assert objects[0]['object_type'] == 'FUNCTION'

    def test_parse_procedure(self):
        """Test parsing CREATE PROCEDURE."""
        from modules.ddl_parser import parse_ddl_file

        sql = """CREATE PROCEDURE update_salary(emp_id INTEGER, new_salary NUMERIC)
AS $$
BEGIN
    UPDATE employees SET salary = new_salary WHERE id = emp_id;
END;
$$ LANGUAGE plpgsql;"""
        objects = parse_ddl_file(sql)

        assert len(objects) == 1
        assert objects[0]['object_name'] == 'update_salary'
        assert objects[0]['object_type'] == 'PROCEDURE'

    def test_parse_trigger(self):
        """Test parsing CREATE TRIGGER."""
        from modules.ddl_parser import parse_ddl_file

        sql = """CREATE TRIGGER audit_employees
AFTER INSERT OR UPDATE ON employees
FOR EACH ROW EXECUTE FUNCTION audit_employee_changes();"""
        objects = parse_ddl_file(sql)

        assert len(objects) == 1
        assert objects[0]['object_name'] == 'audit_employees'
        assert objects[0]['object_type'] == 'TRIGGER'

    def test_parse_type(self):
        """Test parsing CREATE TYPE."""
        from modules.ddl_parser import parse_ddl_file

        sql = """CREATE TYPE employee_status AS ENUM ('active', 'inactive', 'terminated');"""
        objects = parse_ddl_file(sql)

        assert len(objects) == 1
        assert objects[0]['object_name'] == 'employee_status'
        assert objects[0]['object_type'] == 'TYPE'

    def test_skip_comments(self):
        """Test that SQL comments are skipped."""
        from modules.ddl_parser import parse_ddl_file

        sql = """-- This is a comment
-- Another comment
CREATE TABLE test_table (id INTEGER);"""
        objects = parse_ddl_file(sql)

        assert len(objects) == 1
        assert objects[0]['object_name'] == 'test_table'

    def test_skip_set_statements(self):
        """Test that SET statements are skipped."""
        from modules.ddl_parser import parse_ddl_file

        sql = """SET search_path TO public;
SET client_encoding TO 'UTF8';
CREATE TABLE test_table (id INTEGER);"""
        objects = parse_ddl_file(sql)

        assert len(objects) == 1
        assert objects[0]['object_name'] == 'test_table'

    def test_parse_quoted_names(self):
        """Test parsing objects with quoted names."""
        from modules.ddl_parser import parse_ddl_file

        sql = """CREATE TABLE "MyTable" (id INTEGER);"""
        objects = parse_ddl_file(sql)

        assert len(objects) == 1
        assert objects[0]['object_name'] == 'mytable'

    def test_line_numbers_tracked(self):
        """Test that line numbers are correctly tracked."""
        from modules.ddl_parser import parse_ddl_file

        sql = """-- Comment line 1
-- Comment line 2

CREATE TABLE first_table (
    id INTEGER
);

CREATE TABLE second_table (
    id INTEGER
);"""
        objects = parse_ddl_file(sql)

        assert len(objects) == 2
        assert objects[0]['line_start'] == 4
        assert objects[1]['line_start'] == 8

    def test_empty_content(self):
        """Test parsing empty content."""
        from modules.ddl_parser import parse_ddl_file

        objects = parse_ddl_file("")
        assert len(objects) == 0

    def test_comments_only(self):
        """Test parsing content with only comments."""
        from modules.ddl_parser import parse_ddl_file

        sql = """-- This is a comment
-- Another comment"""
        objects = parse_ddl_file(sql)
        assert len(objects) == 0

    def test_mixed_object_types(self):
        """Test parsing a file with multiple object types."""
        from modules.ddl_parser import parse_ddl_file

        sql = """CREATE TABLE employees (id INTEGER);

CREATE VIEW employee_view AS SELECT * FROM employees;

CREATE INDEX idx_emp ON employees(id);

CREATE SEQUENCE emp_seq;"""
        objects = parse_ddl_file(sql)

        assert len(objects) == 4
        types = [obj['object_type'] for obj in objects]
        assert 'TABLE' in types
        assert 'VIEW' in types
        assert 'INDEX' in types
        assert 'SEQUENCE' in types


class TestExtractObjectNames:
    """Test the extract_object_names function."""

    def test_extract_all_names(self):
        """Test extracting all object names without filter."""
        from modules.ddl_parser import extract_object_names

        sql = """CREATE TABLE employees (id INTEGER);
CREATE VIEW emp_view AS SELECT * FROM employees;
CREATE INDEX idx_emp ON employees(id);"""
        results = extract_object_names(sql)

        assert len(results) == 3
        names = [r[1] for r in results]
        assert 'employees' in names
        assert 'emp_view' in names
        assert 'idx_emp' in names

    def test_extract_filtered_by_type(self):
        """Test extracting object names filtered by type."""
        from modules.ddl_parser import extract_object_names

        sql = """CREATE TABLE t1 (id INTEGER);
CREATE TABLE t2 (id INTEGER);
CREATE VIEW v1 AS SELECT 1;"""
        results = extract_object_names(sql, object_type='TABLE')

        assert len(results) == 2
        names = [r[1] for r in results]
        assert 't1' in names
        assert 't2' in names
        assert 'v1' not in names

    def test_extract_invalid_type(self):
        """Test extracting with invalid object type returns empty."""
        from modules.ddl_parser import extract_object_names

        sql = """CREATE TABLE test (id INTEGER);"""
        results = extract_object_names(sql, object_type='INVALID_TYPE')

        assert len(results) == 0

    def test_extract_empty_content(self):
        """Test extracting from empty content."""
        from modules.ddl_parser import extract_object_names

        results = extract_object_names("")
        assert len(results) == 0


class TestCountObjectsByType:
    """Test the count_objects_by_type function."""

    def test_count_multiple_types(self):
        """Test counting objects by type."""
        from modules.ddl_parser import count_objects_by_type

        sql = """CREATE TABLE t1 (id INTEGER);
CREATE TABLE t2 (id INTEGER);
CREATE TABLE t3 (id INTEGER);
CREATE VIEW v1 AS SELECT 1;
CREATE INDEX i1 ON t1(id);"""
        counts = count_objects_by_type(sql)

        assert counts.get('TABLE') == 3
        assert counts.get('VIEW') == 1
        assert counts.get('INDEX') == 1

    def test_count_empty_content(self):
        """Test counting objects in empty content."""
        from modules.ddl_parser import count_objects_by_type

        counts = count_objects_by_type("")
        assert counts == {}

    def test_count_single_type(self):
        """Test counting when only one type exists."""
        from modules.ddl_parser import count_objects_by_type

        sql = """CREATE SEQUENCE s1;
CREATE SEQUENCE s2;"""
        counts = count_objects_by_type(sql)

        assert counts.get('SEQUENCE') == 2
        assert counts.get('TABLE') is None


class TestSplitByObject:
    """Test the split_by_object function."""

    def test_split_multiple_objects(self):
        """Test splitting DDL into separate objects."""
        from modules.ddl_parser import split_by_object

        sql = """CREATE TABLE employees (
    id INTEGER
);

CREATE TABLE departments (
    id INTEGER
);"""
        result = split_by_object(sql)

        assert len(result) == 2
        assert ('TABLE', 'employees') in result
        assert ('TABLE', 'departments') in result
        assert 'CREATE TABLE employees' in result[('TABLE', 'employees')]
        assert 'CREATE TABLE departments' in result[('TABLE', 'departments')]

    def test_split_empty_content(self):
        """Test splitting empty content."""
        from modules.ddl_parser import split_by_object

        result = split_by_object("")
        assert result == {}

    def test_split_mixed_types(self):
        """Test splitting content with mixed object types."""
        from modules.ddl_parser import split_by_object

        sql = """CREATE TABLE test_table (id INTEGER);
CREATE VIEW test_view AS SELECT 1;"""
        result = split_by_object(sql)

        assert ('TABLE', 'test_table') in result
        assert ('VIEW', 'test_view') in result


class TestEdgeCases:
    """Test edge cases and complex scenarios."""

    def test_nested_parentheses(self):
        """Test parsing with nested parentheses."""
        from modules.ddl_parser import parse_ddl_file

        sql = """CREATE TABLE complex_table (
    id INTEGER,
    data JSONB DEFAULT '{}',
    CHECK (id > 0 AND (data->>'type' IS NOT NULL))
);"""
        objects = parse_ddl_file(sql)

        assert len(objects) == 1
        assert objects[0]['object_name'] == 'complex_table'

    def test_multiline_function_body(self):
        """Test parsing multiline function with complex body."""
        from modules.ddl_parser import parse_ddl_file

        sql = """CREATE FUNCTION complex_function(p1 INTEGER, p2 TEXT)
RETURNS TABLE(a INTEGER, b TEXT) AS $$
DECLARE
    v_temp INTEGER;
BEGIN
    v_temp := p1 * 2;
    RETURN QUERY
    SELECT v_temp, p2;
END;
$$ LANGUAGE plpgsql;"""
        objects = parse_ddl_file(sql)

        assert len(objects) == 1
        assert objects[0]['object_type'] == 'FUNCTION'
        assert 'DECLARE' in objects[0]['ddl']
        assert 'RETURN QUERY' in objects[0]['ddl']

    def test_single_line_table(self):
        """Test parsing single-line table definition."""
        from modules.ddl_parser import parse_ddl_file

        sql = """CREATE TABLE simple (id INTEGER);"""
        objects = parse_ddl_file(sql)

        assert len(objects) == 1
        assert objects[0]['object_name'] == 'simple'
        assert objects[0]['line_start'] == objects[0]['line_end']

    def test_unclosed_object_at_eof(self):
        """Test handling unclosed object at end of file."""
        from modules.ddl_parser import parse_ddl_file

        # Missing semicolon at end
        sql = """CREATE TABLE incomplete (
    id INTEGER
)"""
        objects = parse_ddl_file(sql)

        # Should still capture the object
        assert len(objects) == 1
        assert objects[0]['object_name'] == 'incomplete'

    def test_case_insensitivity(self):
        """Test that parsing is case-insensitive."""
        from modules.ddl_parser import parse_ddl_file

        sql = """create table lowercase_table (id integer);
CREATE TABLE UPPERCASE_TABLE (ID INTEGER);
Create Table MixedCase_Table (Id Integer);"""
        objects = parse_ddl_file(sql)

        assert len(objects) == 3
        names = [obj['object_name'] for obj in objects]
        assert 'lowercase_table' in names
        assert 'uppercase_table' in names
        assert 'mixedcase_table' in names

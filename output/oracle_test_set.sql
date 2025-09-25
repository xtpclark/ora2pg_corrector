-- Test 1: PL/SQL Procedure with NVL and DBMS_OUTPUT
CREATE OR REPLACE PROCEDURE employee_salary_update(emp_id IN NUMBER, increment IN NUMBER) AS
BEGIN
    DBMS_OUTPUT.PUT_LINE('Updating salary for employee ID: ' || NVL(emp_id, 0));
    UPDATE employees
    SET salary = salary + NVL(increment, 100)
    WHERE employee_id = emp_id;
    COMMIT;
END;
/

-- Test 2: Function with DECODE
CREATE OR REPLACE FUNCTION get_employee_status(status_code IN NUMBER) RETURN VARCHAR2 AS
BEGIN
    RETURN DECODE(status_code, 
                  1, 'Active',
                  2, 'Inactive',
                  3, 'On Leave',
                  'Unknown');
END;
/

-- Test 3: Table with Oracle-specific NUMBER types
CREATE TABLE orders (
    order_id NUMBER(10,0),
    amount NUMBER(12,2),
    discount NUMBER,
    order_date DATE
);

-- Test 4: MERGE Statement
MERGE INTO employees e
USING (SELECT employee_id, salary FROM new_salaries) ns
ON (e.employee_id = ns.employee_id)
WHEN MATCHED THEN
    UPDATE SET e.salary = ns.salary
WHEN NOT MATCHED THEN
    INSERT (employee_id, salary)
    VALUES (ns.employee_id, ns.salary);

-- Test 5: Hierarchical Query with CONNECT BY
SELECT employee_id, first_name, level
FROM employees
START WITH manager_id IS NULL
CONNECT BY PRIOR employee_id = manager_id;

-- Test 6: PL/SQL Block with AUTONOMOUS_TRANSACTION
CREATE OR REPLACE PROCEDURE log_action(action IN VARCHAR2) AS
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    INSERT INTO action_log (action, log_date)
    VALUES (action, SYSDATE);
    COMMIT;
END;
/

-- Test 7: Query with Oracle-specific TO_DATE
SELECT *
FROM orders
WHERE order_date = TO_DATE('2023-01-01', 'YYYY-MM-DD');

-- Test 8: Empty String Handling
CREATE OR REPLACE PROCEDURE process_name(name IN VARCHAR2) AS
BEGIN
    IF name = '' THEN
        DBMS_OUTPUT.PUT_LINE('Name is empty string');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Name: ' || NVL(name, 'NULL'));
    END IF;
END;
/

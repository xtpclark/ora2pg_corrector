-- This script runs as SYS to create the HR user in the pluggable database
ALTER SESSION SET CONTAINER = FREEPDB1;
ALTER SESSION SET "_ORACLE_SCRIPT"=true;

-- Drop user if it exists, to make this script re-runnable
DECLARE
  v_user_exists NUMBER;
BEGIN
  SELECT COUNT(*) INTO v_user_exists FROM dba_users WHERE username = 'HR';
  IF v_user_exists > 0 THEN
    EXECUTE IMMEDIATE 'DROP USER hr CASCADE';
  END IF;
END;
/

-- Create the user, grant permissions, and unlock
CREATE USER hr IDENTIFIED BY hr;
GRANT CONNECT, RESOURCE, DBA TO hr;
ALTER USER hr ACCOUNT UNLOCK;

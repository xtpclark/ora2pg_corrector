-- Create the ADEMPIERE user in the pluggable database
ALTER SESSION SET CONTAINER = FREEPDB1;
ALTER SESSION SET "_ORACLE_SCRIPT"=true;

-- Drop user if it exists, to make this script re-runnable
DECLARE
  v_user_exists NUMBER;
BEGIN
  SELECT COUNT(*) INTO v_user_exists FROM dba_users WHERE username = 'ADEMPIERE';
  IF v_user_exists > 0 THEN
    EXECUTE IMMEDIATE 'DROP USER ADEMPIERE CASCADE';
  END IF;
END;
/

-- Create the user, grant permissions, and unlock
CREATE USER ADEMPIERE IDENTIFIED BY adempiere;
GRANT CONNECT, RESOURCE, DBA TO ADEMPIERE;
ALTER USER ADEMPIERE ACCOUNT UNLOCK;
ALTER USER ADEMPIERE DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS;

#!/bin/bash
# Install Adempiere schema into FREEPDB1 as the ADEMPIERE user
# This script is executed by the gvenzl/oracle-free entrypoint
# The SQL files are mounted from sample_schemas/adempiere/

ORACLE_SID=${ORACLE_SID:-FREE}
SCHEMA_DIR="/opt/oracle/adempiere_schema"

echo "=== Adempiere Schema Installation ==="
echo "Loading DDL files into ADEMPIERE schema in FREEPDB1..."

# Check if schema files are available
if [ ! -d "$SCHEMA_DIR" ]; then
    echo "ERROR: Schema directory $SCHEMA_DIR not found. Skipping Adempiere install."
    echo "Mount sample_schemas/adempiere/ to $SCHEMA_DIR in docker-compose.yml"
    exit 0
fi

# Load main tables (Adempiere.sql) - these use ; as terminator
echo "--- Loading tables from Adempiere.sql ---"
sqlplus -S ADEMPIERE/adempiere@//localhost:1521/FREEPDB1 <<'EOSQL'
SET ECHO OFF
SET FEEDBACK OFF
SET HEADING OFF
WHENEVER SQLERROR CONTINUE
@/opt/oracle/adempiere_schema/Adempiere.sql
EXIT;
EOSQL
echo "--- Tables loaded ---"

# Load sequences (Sequences.sql) - uses / as terminator
# Note: TRUNCATE/DELETE statements will fail on empty tables, that's expected
echo "--- Loading sequences from Sequences.sql ---"
sqlplus -S ADEMPIERE/adempiere@//localhost:1521/FREEPDB1 <<'EOSQL'
SET ECHO OFF
SET FEEDBACK OFF
SET HEADING OFF
WHENEVER SQLERROR CONTINUE
@/opt/oracle/adempiere_schema/Sequences.sql
EXIT;
EOSQL
echo "--- Sequences loaded ---"

# Load views (Views.sql) - many will fail due to missing data/references, that's ok
echo "--- Loading views from Views.sql ---"
sqlplus -S ADEMPIERE/adempiere@//localhost:1521/FREEPDB1 <<'EOSQL'
SET ECHO OFF
SET FEEDBACK OFF
SET HEADING OFF
WHENEVER SQLERROR CONTINUE
@/opt/oracle/adempiere_schema/Views.sql
EXIT;
EOSQL
echo "--- Views loaded ---"

# Load functions (functions-decl.sql) - stub declarations
echo "--- Loading functions from functions-decl.sql ---"
sqlplus -S ADEMPIERE/adempiere@//localhost:1521/FREEPDB1 <<'EOSQL'
SET ECHO OFF
SET FEEDBACK OFF
SET HEADING OFF
WHENEVER SQLERROR CONTINUE
@/opt/oracle/adempiere_schema/functions-decl.sql
EXIT;
EOSQL
echo "--- Functions loaded ---"

# Verify installation
echo "--- Verification ---"
sqlplus -S ADEMPIERE/adempiere@//localhost:1521/FREEPDB1 <<'EOSQL'
SET HEADING ON
SET FEEDBACK ON
SET LINESIZE 80
SET PAGESIZE 50
SELECT object_type, COUNT(*) AS count
  FROM user_objects
 GROUP BY object_type
 ORDER BY object_type;
EXIT;
EOSQL

echo "=== Adempiere Schema Installation Complete ==="

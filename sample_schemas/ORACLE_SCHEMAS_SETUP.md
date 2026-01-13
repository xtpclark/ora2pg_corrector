# Oracle Sample Schemas Setup Guide

This document explains how to recreate the Oracle sample schemas in the `ora2pg_corrector_oracle-free_1` container.

## Prerequisites

- Oracle Free container running: `podman-compose up -d oracle-free`
- Wait for Oracle to initialize (~2-3 minutes for fresh install)
- Check ready status: `podman logs ora2pg_corrector_oracle-free_1 | grep "DATABASE IS READY"`

## Connection Details

- **Host**: `oracle-free` (from app container) or `localhost` (from host)
- **Port**: 1521
- **Service**: FREEPDB1
- **SYS Password**: oracle

## 1. Adempiere Schema (463 tables)

Adempiere is an open-source ERP system with a complex Oracle schema - ideal for testing migrations.

### Create User and Import Schema

```bash
# Copy DDL files to Oracle container
podman cp sample_schemas/adempiere ora2pg_corrector_oracle-free_1:/tmp/

# Create ADEMPIERE user
podman exec ora2pg_corrector_oracle-free_1 sqlplus -s sys/oracle@localhost:1521/FREEPDB1 as sysdba <<'EOF'
CREATE USER adempiere IDENTIFIED BY adempiere DEFAULT TABLESPACE users TEMPORARY TABLESPACE temp;
GRANT CONNECT, RESOURCE, CREATE VIEW, CREATE MATERIALIZED VIEW, CREATE SYNONYM, CREATE SEQUENCE, CREATE TRIGGER, CREATE TYPE, CREATE PROCEDURE TO adempiere;
GRANT UNLIMITED TABLESPACE TO adempiere;
exit;
EOF

# Import the schema
podman exec ora2pg_corrector_oracle-free_1 bash -c "cd /tmp/adempiere && sqlplus -s adempiere/adempiere@localhost:1521/FREEPDB1 @Adempiere.sql"

# Import sequences
podman exec ora2pg_corrector_oracle-free_1 bash -c "cd /tmp/adempiere && sqlplus -s adempiere/adempiere@localhost:1521/FREEPDB1 @Sequences.sql"
```

### Verify Installation

```bash
podman exec ora2pg_corrector_oracle-free_1 sqlplus -s adempiere/adempiere@localhost:1521/FREEPDB1 <<'EOF'
SELECT 'Tables: ' || COUNT(*) FROM user_tables;
SELECT 'Indexes: ' || COUNT(*) FROM user_indexes;
SELECT 'Sequences: ' || COUNT(*) FROM user_sequences;
exit;
EOF
```

Expected output: ~463 tables, ~612 indexes, 3 sequences

## 2. Oracle Sample Schemas (OE, PM, SH)

These are official Oracle sample schemas, smaller and simpler than Adempiere.

### Prerequisites

Clone the Oracle sample schemas repository:
```bash
git clone https://github.com/oracle-samples/db-sample-schemas.git /tmp/oracle-sample-schemas
podman cp /tmp/oracle-sample-schemas ora2pg_corrector_oracle-free_1:/tmp/
```

### Create Users

```bash
podman exec ora2pg_corrector_oracle-free_1 sqlplus -s sys/oracle@localhost:1521/FREEPDB1 as sysdba <<'EOF'
-- Order Entry (OE)
CREATE USER oe IDENTIFIED BY oe DEFAULT TABLESPACE users TEMPORARY TABLESPACE temp;
GRANT CONNECT, RESOURCE, CREATE VIEW, CREATE MATERIALIZED VIEW, CREATE SYNONYM, CREATE SEQUENCE, CREATE TRIGGER, CREATE TYPE, CREATE PROCEDURE TO oe;
GRANT UNLIMITED TABLESPACE TO oe;

-- Product Media (PM)
CREATE USER pm IDENTIFIED BY pm DEFAULT TABLESPACE users TEMPORARY TABLESPACE temp;
GRANT CONNECT, RESOURCE, CREATE VIEW, CREATE SYNONYM, CREATE SEQUENCE, CREATE TYPE TO pm;
GRANT UNLIMITED TABLESPACE TO pm;

-- Sales History (SH)
CREATE USER sh IDENTIFIED BY sh DEFAULT TABLESPACE users TEMPORARY TABLESPACE temp;
GRANT CONNECT, RESOURCE, CREATE VIEW, CREATE SYNONYM, CREATE SEQUENCE, CREATE MATERIALIZED VIEW TO sh;
GRANT UNLIMITED TABLESPACE TO sh;
exit;
EOF
```

### Import OE Schema

```bash
podman exec ora2pg_corrector_oracle-free_1 bash -c "cd /tmp/oracle-sample-schemas/order_entry && sqlplus -s oe/oe@localhost:1521/FREEPDB1 @oe_cre.sql"
```

Note: Some constraints may fail due to missing dependencies - this is expected.

## 3. Troubleshooting

### Oracle Container Keeps Crashing

If the Oracle container exits with code 54 (parameter file error):

```bash
# Remove and recreate
podman rm ora2pg_corrector_oracle-free_1
podman volume rm ora2pg_corrector_oracle_data
podman-compose up -d oracle-free

# Wait for initialization (2-3 minutes)
# Then re-import schemas as above
```

### Check Container Status

```bash
podman ps -a | grep oracle
podman logs ora2pg_corrector_oracle-free_1 | tail -20
```

### Test Connection from App Container

```bash
podman exec ora2pg_corrector_app_1 bash -c "echo 'SELECT 1 FROM dual;' | sqlplus -s sys/oracle@oracle-free:1521/FREEPDB1 as sysdba"
```

## 4. Client Configuration in ora2pg_corrector

For each schema, create a client with these Oracle DSN settings:

| Schema | Oracle DSN |
|--------|------------|
| ADEMPIERE | `dbi:Oracle:host=oracle-free;service_name=FREEPDB1;port=1521` |
| OE | `dbi:Oracle:host=oracle-free;service_name=FREEPDB1;port=1521` |
| PM | `dbi:Oracle:host=oracle-free;service_name=FREEPDB1;port=1521` |
| SH | `dbi:Oracle:host=oracle-free;service_name=FREEPDB1;port=1521` |

Set the **Schema** field to match the username (ADEMPIERE, OE, PM, SH).
Set **Oracle User** and **Oracle Password** to match the schema credentials.

## 5. Quick Full Reset Script

To completely reset and recreate all schemas:

```bash
#!/bin/bash
# reset_oracle_schemas.sh

# Stop and remove Oracle container and volume
podman-compose down
podman volume rm ora2pg_corrector_oracle_data 2>/dev/null

# Start fresh Oracle
podman-compose up -d oracle-free

# Wait for Oracle to be ready
echo "Waiting for Oracle to initialize..."
for i in {1..30}; do
    if podman logs ora2pg_corrector_oracle-free_1 2>&1 | grep -q "DATABASE IS READY"; then
        echo "Oracle is ready!"
        break
    fi
    sleep 10
done

# Copy DDL files
podman cp sample_schemas/adempiere ora2pg_corrector_oracle-free_1:/tmp/

# Create all users
podman exec ora2pg_corrector_oracle-free_1 sqlplus -s sys/oracle@localhost:1521/FREEPDB1 as sysdba <<'EOF'
CREATE USER adempiere IDENTIFIED BY adempiere DEFAULT TABLESPACE users TEMPORARY TABLESPACE temp;
GRANT CONNECT, RESOURCE, CREATE VIEW, CREATE MATERIALIZED VIEW, CREATE SYNONYM, CREATE SEQUENCE, CREATE TRIGGER, CREATE TYPE, CREATE PROCEDURE TO adempiere;
GRANT UNLIMITED TABLESPACE TO adempiere;

CREATE USER oe IDENTIFIED BY oe DEFAULT TABLESPACE users TEMPORARY TABLESPACE temp;
GRANT CONNECT, RESOURCE, CREATE VIEW, CREATE MATERIALIZED VIEW, CREATE SYNONYM, CREATE SEQUENCE, CREATE TRIGGER, CREATE TYPE, CREATE PROCEDURE TO oe;
GRANT UNLIMITED TABLESPACE TO oe;

CREATE USER pm IDENTIFIED BY pm DEFAULT TABLESPACE users TEMPORARY TABLESPACE temp;
GRANT CONNECT, RESOURCE, CREATE VIEW, CREATE SYNONYM, CREATE SEQUENCE, CREATE TYPE TO pm;
GRANT UNLIMITED TABLESPACE TO pm;

CREATE USER sh IDENTIFIED BY sh DEFAULT TABLESPACE users TEMPORARY TABLESPACE temp;
GRANT CONNECT, RESOURCE, CREATE VIEW, CREATE SYNONYM, CREATE SEQUENCE, CREATE MATERIALIZED VIEW TO sh;
GRANT UNLIMITED TABLESPACE TO sh;
exit;
EOF

# Import Adempiere
podman exec ora2pg_corrector_oracle-free_1 bash -c "cd /tmp/adempiere && sqlplus -s adempiere/adempiere@localhost:1521/FREEPDB1 @Adempiere.sql"
podman exec ora2pg_corrector_oracle-free_1 bash -c "cd /tmp/adempiere && sqlplus -s adempiere/adempiere@localhost:1521/FREEPDB1 @Sequences.sql"

echo "Done! Schemas created."
```

## File Locations

- Adempiere DDL: `sample_schemas/adempiere/`
  - `Adempiere.sql` - Main schema (tables, indexes, constraints)
  - `Sequences.sql` - Sequence definitions
  - `Views.sql` - View definitions
  - `functions-decl.sql` - Function declarations

- Oracle Sample Schemas: Clone from https://github.com/oracle-samples/db-sample-schemas

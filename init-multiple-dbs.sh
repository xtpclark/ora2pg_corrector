#!/bin/bash
# This script will be executed when the PostgreSQL container starts for the first time.
set -e

# The VALIDATION_PG_DBNAME variable will be passed from the .env file.
# If it's not set, it defaults to 'staging'.
DB_NAME=${VALIDATION_PG_DBNAME:-staging}

echo "Creating additional database: $DB_NAME"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE $DB_NAME'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$DB_NAME')\gexec
EOSQL
echo "Finished creating database: $DB_NAME"

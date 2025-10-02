#!/bin/sh
# This script is run as root.

# Set the ownership of the directories that the application needs to write to.
# This is crucial for when named volumes are used, as they are mounted as root.
chown -R appuser:appuser /app/data
chown -R appuser:appuser /app/output
chown -R appuser:appuser /app/project_output
chown -R appuser:appuser /app/project_data 

# Set up Oracle Instant Client environment so sqlplus and other tools are found.
#export LD_LIBRARY_PATH=/opt/oracle/instantclient_21_19
#export PATH=$LD_LIBRARY_PATH:$PATH

# --- NEW: Run the database initialization command AS the appuser ---
# gosu appuser flask init-db

# Drop privileges and execute the main command (CMD) as the non-root "appuser".
exec gosu appuser "$@"

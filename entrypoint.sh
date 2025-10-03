#!/bin/sh
# This script is run as root and manages authentication token display

# Set the ownership of the directories that the application needs to write to
chown -R appuser:appuser /app/data
chown -R appuser:appuser /app/output
chown -R appuser:appuser /app/project_output
chown -R appuser:appuser /app/project_data 

# Display token information if auth is enabled
if [ "$AUTH_MODE" != "none" ]; then
    echo "=================================================="
    echo "         Ora2Pg Corrector - Authentication"
    echo "=================================================="
    
    # Check if token file exists
    if [ -f /app/data/.auth_token ]; then
        TOKEN=$(cat /app/data/.auth_token)
        echo "Access Token: $TOKEN"
        echo ""
        echo "Use this token in one of these ways:"
        echo "1. Header:     X-Auth-Token: $TOKEN"
        echo "2. Query:      ?token=$TOKEN"
        echo "3. nginx:      proxy_set_header X-Auth-Token $TOKEN;"
        echo ""
        echo "For nginx, update your configuration with:"
        echo "  proxy_set_header X-Auth-Token $TOKEN;"
    else
        echo "Token will be generated on first startup."
        echo "Check logs for the access token."
    fi
    echo "=================================================="
    echo ""
fi

# Drop privileges and execute the main command (CMD) as the non-root "appuser"
exec gosu appuser "$@"

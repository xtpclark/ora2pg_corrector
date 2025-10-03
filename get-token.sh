#!/bin/bash
# Helper script to get the access token from Docker/Podman container

CONTAINER_NAME="ora2pg_corrector_app_1"  # Default docker-compose name
TOKEN_FILE="/app/data/.auth_token"  # Correct path in container

# Detect container runtime (Docker or Podman)
CONTAINER_CMD=""
if command -v docker &> /dev/null; then
    # Check if docker daemon is running
    if docker ps &> /dev/null; then
        CONTAINER_CMD="docker"
    fi
fi

if [ -z "$CONTAINER_CMD" ] && command -v podman &> /dev/null; then
    CONTAINER_CMD="podman"
fi

if [ -z "$CONTAINER_CMD" ]; then
    echo "Error: Neither Docker nor Podman is available or running."
    echo ""
    echo "Please ensure you have one of the following:"
    echo "  - Docker installed and Docker daemon running"
    echo "  - Podman installed"
    exit 1
fi

# Check if container name is provided as argument
if [ ! -z "$1" ]; then
    CONTAINER_NAME="$1"
fi

echo "======================================="
echo "   Ora2Pg Corrector - Access Token"
echo "======================================="
echo ""
echo "Using container runtime: ${CONTAINER_CMD}"
echo "Container name: ${CONTAINER_NAME}"
echo ""

# Check if container is running
if ! ${CONTAINER_CMD} ps --format '{{.Names}}' 2>/dev/null | grep -q "^${CONTAINER_NAME}$"; then
    echo "Error: Container '${CONTAINER_NAME}' is not running."
    echo ""
    echo "Available containers:"
    ${CONTAINER_CMD} ps --format 'table {{.Names}}\t{{.Status}}'
    echo ""
    echo "Tip: If using podman-compose, try: $0 ora2pg-corrector_app_1"
    echo "     If using docker-compose, try: $0 ora2pg-corrector-app-1"
    exit 1
fi

# Get the token
TOKEN=$(${CONTAINER_CMD} exec ${CONTAINER_NAME} cat ${TOKEN_FILE} 2>/dev/null)

if [ -z "$TOKEN" ]; then
    echo "Error: Could not retrieve token from container."
    echo "The token may not have been generated yet."
    echo ""
    echo "Try checking the container logs:"
    echo "  ${CONTAINER_CMD} logs ${CONTAINER_NAME} | grep -A5 'ACCESS TOKEN'"
    echo ""
    echo "Or restart the container to generate a token:"
    echo "  ${CONTAINER_CMD}-compose down && ${CONTAINER_CMD}-compose up -d"
    exit 1
fi

echo "✓ Successfully retrieved access token!"
echo ""
echo "Access Token: ${TOKEN}"
echo ""
echo "Configuration Examples:"
echo "----------------------"
echo ""
echo "1. For browser access:"
echo "   http://localhost:8000/?token=${TOKEN}"
echo ""
echo "2. For nginx configuration:"
echo "   proxy_set_header X-Auth-Token ${TOKEN};"
echo ""
echo "3. For curl/API testing:"
echo "   curl -H 'X-Auth-Token: ${TOKEN}' http://localhost:8000/api/clients"
echo ""
echo "4. For persistent browser access, bookmark:"
echo "   http://localhost:8000/?token=${TOKEN}"
echo ""
echo "======================================="
echo ""
echo "Save this token securely. It will persist across container restarts."

#!/bin/bash

# Script to manage Airflow instances for versions 2 and 3
# Usage: ./manage-airflow.sh [ACTION] [VERSION]
#   ACTION: up, down, restart, build, logs, status
#   VERSION: 2 or 3

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Print colored message
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Create .env file with appropriate settings
create_env_file() {
    local env_file="$SCRIPT_DIR/.env"

    # For linux we need to set the AIRFLOW_UID to match the host user to avoid permission issues with mounted volumes
    if [[ "$(uname -s)" == "Linux" ]]; then
        AIRFLOW_UID="${AIRFLOW_UID:-${UID:-50000}}"
    else
        # On Mac and Windows we can use the default UID
        AIRFLOW_UID=50000
    fi

    print_info "Creating .env file with AIRFLOW_UID=$AIRFLOW_UID"

    cat > "$env_file" << EOF
AIRFLOW_UID=$AIRFLOW_UID
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
POSTGRES_SCHEMA=public
POSTGRES_THREADS=1
EOF

    print_info ".env file created successfully"
}

# Show usage
show_usage() {
    cat << EOF
Usage: $0 [ACTION] [VERSION] [DOCKER_ARGS...]

Manage Airflow instances for different versions.

Arguments:
  ACTION       - Operation to perform:
                 up       : Start Airflow services
                 down     : Stop Airflow services
                 restart  : Restart Airflow services
                 build    : Build/rebuild Docker images
                 logs     : Show logs from services
                 status   : Show status of services
                 ps       : List running containers
  VERSION      - Airflow version to use:
                 2        : Use Airflow 2.x (docker-compose.yaml)
                 3        : Use Airflow 3.x (docker-compose3.yaml)
  DOCKER_ARGS  - Additional arguments to pass to docker compose command (optional)

Examples:
  $0 up 2                        # Start Airflow 2.x
  $0 up 2 --force-recreate       # Start Airflow 2.x, forcing container recreation
  $0 down 3 -v                   # Stop Airflow 3.x and remove volumes
  $0 restart 2                   # Restart Airflow 2.x
  $0 build 3 --no-cache          # Build images for Airflow 3.x without cache
  $0 logs 2 --tail=100           # Show last 100 lines of logs for Airflow 2.x
  $0 status 3                    # Show status of Airflow 3.x services

Note:
  - Airflow 2.x uses port 8080 for the webserver
  - Airflow 3.x uses port 8080 for the API server
  - Make sure to stop one version before starting another to avoid port conflicts
EOF
}

# Validate arguments
if [ $# -lt 2 ]; then
    print_error "Invalid number of arguments"
    echo ""
    show_usage
    exit 1
fi

ACTION=$1
VERSION=$2
# Get any additional arguments to pass to docker compose
DOCKER_ARGS="${@:3}"

# Validate version
if [[ "$VERSION" != "2" && "$VERSION" != "3" ]]; then
    print_error "Invalid version: $VERSION. Must be 2 or 3."
    echo ""
    show_usage
    exit 1
fi

# Determine compose file based on version
if [ "$VERSION" = "2" ]; then
    COMPOSE_FILE="docker-compose.yaml"
    DOCKERFILE="Dockerfile"
    AIRFLOW_VERSION="2.11.0"
elif [ "$VERSION" = "3" ]; then
    COMPOSE_FILE="docker-compose3.yaml"
    DOCKERFILE="Dockerfile3"
    AIRFLOW_VERSION="3.0.6"
fi

COMPOSE_PATH="$SCRIPT_DIR/$COMPOSE_FILE"

# Check if compose file exists
if [ ! -f "$COMPOSE_PATH" ]; then
    print_error "Compose file not found: $COMPOSE_PATH"
    exit 1
fi

print_info "Using Airflow version $VERSION ($AIRFLOW_VERSION)"
print_info "Compose file: $COMPOSE_FILE"
print_info "Dockerfile: $DOCKERFILE"
if [ -n "$DOCKER_ARGS" ]; then
    print_info "Additional docker args: $DOCKER_ARGS"
fi
echo ""

# Execute action
case "$ACTION" in
    up)
        create_env_file
        echo ""
        print_info "Building dbt manifest..."
        if [ -f "$SCRIPT_DIR/dags/build_manifest.sh" ]; then
            cd "$SCRIPT_DIR/dags"
            if uv run ./build_manifest.sh; then
                print_info "dbt manifest built successfully"
            else
                print_warning "Failed to build dbt manifest. Continuing with startup..."
            fi
        else
            print_warning "build_manifest.sh not found. Skipping manifest generation."
        fi
        echo ""
        print_info "Starting Airflow $VERSION services..."
        cd "$SCRIPT_DIR"
        docker compose -f "$COMPOSE_FILE" up -d $DOCKER_ARGS
        echo ""
        print_info "Airflow $VERSION is starting up!"
        if [ "$VERSION" = "2" ]; then
            print_info "Webserver will be available at: http://localhost:8080"
            print_info "Default credentials - Username: airflow, Password: airflow"
        else
            print_info "API Server will be available at: http://localhost:8080"
            print_info "Default credentials - Username: airflow, Password: airflow"
        fi
        print_warning "Services may take a few minutes to be fully ready. Use '$0 status $VERSION' to check."
        ;;

    down)
        print_info "Stopping Airflow $VERSION services..."
        cd "$SCRIPT_DIR"
        docker compose -f "$COMPOSE_FILE" down $DOCKER_ARGS
        echo ""
        print_info "Airflow $VERSION services stopped."
        ;;

    restart)
        create_env_file
        echo ""
        print_info "Restarting Airflow $VERSION services..."
        cd "$SCRIPT_DIR"
        docker compose -f "$COMPOSE_FILE" restart $DOCKER_ARGS
        echo ""
        print_info "Airflow $VERSION services restarted."
        ;;

    build)
        create_env_file
        echo ""
        print_info "Building Docker images for Airflow $VERSION..."
        cd "$SCRIPT_DIR"
        docker compose -f "$COMPOSE_FILE" build $DOCKER_ARGS
        echo ""
        print_info "Docker images built successfully."
        ;;

    logs)
        print_info "Showing logs for Airflow $VERSION services..."
        print_info "Press Ctrl+C to exit logs"
        echo ""
        cd "$SCRIPT_DIR"
        docker compose -f "$COMPOSE_FILE" logs -f $DOCKER_ARGS
        ;;

    status)
        print_info "Status of Airflow $VERSION services:"
        echo ""
        cd "$SCRIPT_DIR"
        docker compose -f "$COMPOSE_FILE" ps $DOCKER_ARGS
        ;;

    ps)
        print_info "Running containers for Airflow $VERSION:"
        echo ""
        cd "$SCRIPT_DIR"
        docker compose -f "$COMPOSE_FILE" ps $DOCKER_ARGS
        ;;

    *)
        print_error "Invalid action: $ACTION"
        echo ""
        show_usage
        exit 1
        ;;
esac

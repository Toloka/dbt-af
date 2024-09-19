#!/usr/bin/env bash
set -euoa pipefail

REPO_DIR="$(git rev-parse --show-toplevel)"
source "${REPO_DIR}/examples/.env"

DBT_PROJECT_DIR="${REPO_DIR}/examples/dags"
dbt clean --no-clean-project-files-only --project-dir "${DBT_PROJECT_DIR}" --profiles-dir "${DBT_PROJECT_DIR}" --target dev
dbt deps --debug --project-dir "${DBT_PROJECT_DIR}" --profiles-dir "${DBT_PROJECT_DIR}" --target dev
dbt parse --debug --project-dir "${DBT_PROJECT_DIR}" --profiles-dir "${DBT_PROJECT_DIR}" --target dev

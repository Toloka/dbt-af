#!/usr/bin/env bash
#dbt clean --no-clean-project-files-only --project-dir . --profiles-dir . --target dev
dbt deps --debug --project-dir . --profiles-dir . --target dev
dbt parse --debug --project-dir . --profiles-dir . --target dev

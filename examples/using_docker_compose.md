# Running Airflow with Docker
1. Install Docker and Docker Compose
2. Init `.env` (`source .env`) and build dbt (`cd examples/dags && ./build_manifest.sh`)
3. run `docker compose up --force-recreate -d --build`
4. enjoy
5. run `docker compose down --volumes --remove-orphans`
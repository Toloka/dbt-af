FROM apache/airflow:2.11.0-python3.12

LABEL maintainer="Nikita Yurasov <nikitayurasov@toloka.ai>"
SHELL ["/bin/bash", "-o", "pipefail", "-o", "errexit", "-o", "nounset", "-o", "xtrace", "-c"]

USER root

RUN apt-get update --allow-releaseinfo-change \
    && apt-get install --no-install-recommends -y \
        build-essential \
        libpq-dev


COPY ./dbt_af ${AIRFLOW_HOME}/dbt_af/dbt_af
COPY ./dbt_af_functional_tests ${AIRFLOW_HOME}/dbt_af/dbt_af_functional_tests
COPY ./scripts ${AIRFLOW_HOME}/dbt_af/scripts
COPY ./pyproject.toml ${AIRFLOW_HOME}/dbt_af/pyproject.toml
COPY ./uv.lock ${AIRFLOW_HOME}/dbt_af/uv.lock
COPY ./README.md ${AIRFLOW_HOME}/dbt_af/README.md

RUN chown -R airflow:0 ${AIRFLOW_HOME}/dbt_af

USER airflow

RUN pip install --upgrade uv

RUN uv pip install -e "${AIRFLOW_HOME}/dbt_af[all]"
RUN uv export --directory "${AIRFLOW_HOME}/dbt_af" --only-dev > dev-requirements.txt
RUN uv pip install -r dev-requirements.txt

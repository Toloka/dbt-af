FROM apache/airflow:2.7.3-python3.10

LABEL maintainer="Nikita Yurasov <nikitayurasov@toloka.ai>"
SHELL ["/bin/bash", "-o", "pipefail", "-o", "errexit", "-o", "nounset", "-o", "xtrace", "-c"]

USER root

COPY ./dbt_af ${AIRFLOW_HOME}/dbt_af/dbt_af
COPY ./dbt_af_functional_tests ${AIRFLOW_HOME}/dbt_af/dbt_af_functional_tests
COPY ./scripts ${AIRFLOW_HOME}/dbt_af/scripts
COPY ./pyproject.toml ${AIRFLOW_HOME}/dbt_af/pyproject.toml
COPY ./README.md ${AIRFLOW_HOME}/dbt_af/README.md

RUN chown -R airflow:0 ${AIRFLOW_HOME}/dbt_af

USER airflow

RUN pip install -e "${AIRFLOW_HOME}/dbt_af[tests,mcd,minidbt,examples]"

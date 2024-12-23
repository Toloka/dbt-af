# syntax=docker/dockerfile:1.4

# Labels:
# INTERMEDIATE: intermediate image
# MAIN:         main image
# CI:           continuous integration image


ARG AIRFLOW_VERSION=2.10.3
ARG PY_VERSION=3.10

# INTERMEDIATE: airflow image
FROM apache/airflow:${AIRFLOW_VERSION}-python${PY_VERSION} as base-airflow
LABEL maintainer="Nikita Yurasov <nikitayurasov@toloka.ai>"
SHELL ["/bin/bash", "-o", "pipefail", "-o", "errexit", "-o", "nounset", "-o", "xtrace", "-c"]
# for faster builds
ARG AIRFLOW_USE_UV="true"
USER root

RUN apt-get update --allow-releaseinfo-change \
    && apt-get install --no-install-recommends -y \
        build-essential \
        libpq-dev \
        curl

COPY ./dbt_af ${AIRFLOW_HOME}/dbt_af/dbt_af
COPY ./dbt_af_functional_tests ${AIRFLOW_HOME}/dbt_af/dbt_af_functional_tests
COPY ./scripts ${AIRFLOW_HOME}/dbt_af/scripts
COPY ./pyproject.toml ${AIRFLOW_HOME}/dbt_af/pyproject.toml
COPY ./README.md ${AIRFLOW_HOME}/dbt_af/README.md
RUN chown -R airflow:0 ${AIRFLOW_HOME}/dbt_af


# MAIN: airflow image
FROM base-airflow as airflow-dbt-af
ARG AIRFLOW_VERSION
ARG PY_VERSION

USER airflow
RUN if [ "${AIRFLOW_USE_UV}" = "true" && "${AIRFLOW_VERSION}" \> "2.9.0" ]; then \
      uv pip install "apache-airflow[uv]==${AIRFLOW_VERSION}" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY_VERSION}.txt \
      && uv pip install -e "${AIRFLOW_HOME}/dbt_af[all]"; \
    else \
      pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY_VERSION}.txt \
      && pip install -e "${AIRFLOW_HOME}/dbt_af[all]"; \
    fi

# CI: airflow image
FROM base-airflow as airflow-dbt-af-ci
ARG AIRFLOW_VERSION
ARG PY_VERSION

# install poetry
USER root
ARG POETRY_UID=65533
ARG POETRY_GID=65533
RUN groupadd -g $POETRY_GID -o poetry
RUN useradd -m -u $POETRY_UID -g $POETRY_GID -o -s /bin/bash poetry
# https://python-poetry.org/docs/configuration/#using-environment-variables
ENV POETRY_VERSION="1.8.5" \
    # make poetry install to this location \
    POETRY_HOME="/opt/poetry" \
    POETRY_CACHE_DIR="/opt/poetry/.cache" \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_PREFER_ACTIVE_PYTHON=true \
    # https://github.com/python-poetry/poetry/issues/6301 \
    POETRY_EXPERIMENTAL_NEW_INSTALLER=false \
    POETRY_UID=$POETRY_UID \
    POETRY_GID=$POETRY_GID

USER airflow
RUN pip install pipx \
    pipx ensurepath
# install poetry
RUN pipx install "poetry==${POETRY_VERSION}"
ENV PATH="$POETRY_HOME/bin:$PATH"

WORKDIR ${AIRFLOW_HOME}/dbt_af
COPY --chown=airflow:0 ./tests ${AIRFLOW_HOME}/dbt_af/tests
COPY --chown=airflow:0 ./poetry.lock ${AIRFLOW_HOME}/dbt_af/poetry.lock

USER root
# reinstall apache-airflow and resolve dependencies with exact version of apache-airflow
RUN poetry remove apache-airflow  \
    && poetry add apache-airflow==${AIRFLOW_VERSION} --extras cncf-kubernetes \
    && poetry export --with=dev --without-hashes --format=requirements.txt > requirements.txt
USER airflow
RUN pip install -e "${AIRFLOW_HOME}/dbt_af[all]" && pip install -r requirements.txt

RUN airflow db migrate


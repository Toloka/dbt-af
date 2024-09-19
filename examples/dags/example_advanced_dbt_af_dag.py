# LABELS: dag, airflow (it's required for airflow dag-processor)
from datetime import timedelta

import pendulum

from dbt_af.conf import Config, DbtDefaultTargetsConfig, DbtProjectConfig, RetriesConfig, RetryPolicy
from dbt_af.dags import compile_dbt_af_dags

# specify here all settings for your dbt project
config = Config(
    dbt_project=DbtProjectConfig(
        dbt_project_name='dbt_af_project',
        dbt_project_path='/opt/airflow/dags',
        dbt_models_path='/opt/airflow/dags/advanced_jaffle_shop/models',
        dbt_profiles_path='/opt/airflow/dags',
        dbt_target_path='/opt/airflow/dags/target',
        dbt_log_path='/opt/airflow/dags/logs',
        dbt_schema='my_dbt_schema',
    ),
    dbt_default_targets=DbtDefaultTargetsConfig(default_target='dev', default_for_tests_target='tests'),
    dag_start_date=pendulum.yesterday(),
    is_dev=False,  # set to True if you want to turn on dry-run mode
    retries_config=RetriesConfig(
        default_retry_policy=RetryPolicy(retries=3, retry_delay=timedelta(minutes=5)),
        dbt_run_retry_policy=RetryPolicy(retries=10, retry_delay=timedelta(minutes=1)),
        dbt_test_retry_policy=RetryPolicy(retries=0),
        sensor_retry_policy=RetryPolicy(retries=30, retry_delay=timedelta(minutes=30)),
        k8s_task_retry_policy=RetryPolicy(retry_delay=timedelta(minutes=5)),
        supplemental_task_retry_policy=RetryPolicy(retries=0),
    ),
)

dags = compile_dbt_af_dags(
    manifest_path='/opt/airflow/dags/target/manifest.json',
    config=config,
    etl_service_name='advanced_jaffle_shop',
)
for dag_name, dag in dags.items():
    globals()[dag_name] = dag

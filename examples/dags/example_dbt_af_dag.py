# LABELS: dag, airflow (it's required for airflow dag-processor)
import pendulum

from dbt_af.conf import Config, DbtDefaultTargetsConfig, DbtProjectConfig
from dbt_af.dags import compile_dbt_af_dags

# specify here all settings for your dbt project
config = Config(
    dbt_project=DbtProjectConfig(
        dbt_project_name='dbt_af_project',
        dbt_project_path='/opt/airflow/dags',
        dbt_models_path='/opt/airflow/dags/jaffle_shop/dbt/models',
        dbt_profiles_path='/opt/airflow/dags',
        dbt_target_path='/opt/airflow/dags/target',
        dbt_log_path='/opt/airflow/dags/logs',
        dbt_schema='my_dbt_schema',
    ),
    dbt_default_targets=DbtDefaultTargetsConfig(default_target='dev'),
    dag_start_date=pendulum.yesterday(),
    dry_run=False,  # set to True if you want to turn on dry-run mode
)

dags = compile_dbt_af_dags(
    manifest_path='/opt/airflow/dags/target/manifest.json',
    config=config,
    etl_service_name='jaffle_shop',
)
for dag_name, dag in dags.items():
    globals()[dag_name] = dag

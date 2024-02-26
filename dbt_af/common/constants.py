import datetime

DBT_SENSOR_POOL = 'dbt_sensor_pool'
DBT_COMPILE_POOL = 'dbt_compile_pool'

DBT_MODEL_DAG_PARAM = 'dbt_select_model'

DEFAULT_DAG_ARGS = {'owner': 'airflow', 'retries': 1, 'retry_delay': datetime.timedelta(minutes=1)}

# tag for DBT dags in airflow that have regular schedule (not @manual)
FRONTIER_TAG = 'frontier'
BACKFILL_TAG = 'backfill'
MAINTENANCE_TAG = 'dbt_maintenance'
LARGE_TESTS_TAG = 'dbt_large_tests'

DOMAIN_DAG_START_DATE_FMT = 'YYYY-MM-DDTHH:mm:ss'

# k8s specific constants
AZ_MI_BINDING_LABEL_NAME = 'aadpodidbinding'

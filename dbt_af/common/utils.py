import logging
import os
from enum import Enum
from pathlib import Path
from typing import Optional

from airflow.models import Variable
from airflow.utils.context import Context
from cachetools import TTLCache, cached

from dbt_af.conf import Config


class TestTag(Enum):
    """DBT tag that specifies a test scheduling strategy in an Airflow DAG."""

    small = '@small'
    medium = '@medium'
    large = '@large'


def get_variable(var_name):
    if var_name in os.environ:
        return os.environ.get(var_name)

    return Variable.get(var_name, f'{{{{ var.value.{var_name} }}}}')


@cached(cache=TTLCache(maxsize=1024, ttl=60 * 5))
def init_environment(config: Config):
    dbt_env = {
        'DBT_TARGET_PATH': str(config.dbt_project.dbt_target_path),
        'DBT_LOG_PATH': str(config.dbt_project.dbt_log_path),
        'DBT_SCHEMA': config.dbt_project.dbt_schema,
        'DBT_PROJECT_DIR': str(config.dbt_project.dbt_project_path),
        'DBT_DEPS_DIR': os.path.join(config.dbt_project.dbt_project_path, 'dbt_packages'),
        'DBT_PROFILES_DIR': str(config.dbt_project.dbt_profiles_path),
        **config.dbt_project.additional_dbt_env,
    }

    return dbt_env


def find_latest_log_file(context: 'Context', log_dir: Path) -> Optional[str]:
    log_pattern = f'dag_id={context["dag"].dag_id}/run_id={context["run_id"]}/task_id={context["task"].task_id}/*.log'
    try:
        return str(max(log_dir.glob(log_pattern)))
    except TypeError:
        logging.error(f'No log file found for dag_id={context["dag"].dag_id}, task_id={context["task"].task_id}')
    except Exception as ex:
        logging.error(f'Something went wrong while searching log file, {ex}')

    return None

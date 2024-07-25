import logging
from collections import defaultdict
from typing import Callable, Dict, Tuple, Optional, List

from dbt_af.conf import Config
from dbt_af.integrations.mcd import prepare_mcd_callbacks


def merge_dicts_by_key(*dicts: dict) -> Dict:
    result_dict = defaultdict(list)

    for d in dicts:
        for key, value in d.items():
            result_dict[key].append(value)
    return dict(result_dict)


def prepare_af_callbacks(config: Config) -> Tuple[Dict[str, Callable], Dict[str, Callable]]:
    if config.af_callbacks is None:
        logging.warning('No callback defined')
        return {}, {}

    airflow_task_callbacks = {
        'on_success_callback': config.af_callbacks.task_on_success_callback,
        'on_failure_callback': config.af_callbacks.task_on_failure_callback,
        'on_retry_callback': config.af_callbacks.task_on_retry_callback,
        'on_execute_callback': config.af_callbacks.task_on_execute_callback
    }

    airflow_dag_callbacks = {
        'on_failure_callback': config.af_callbacks.dag_on_failure_callback,
        'on_success_callback': config.af_callbacks.dag_on_success_callback,
        'sla_miss_callback': config.af_callbacks.dag_sla_miss_callback,
    }

    return airflow_dag_callbacks, airflow_task_callbacks


def prepare_callbacks(config: Config) -> Tuple[Dict[str, List[Optional[Callable]]], Dict[str, List[Optional[Callable]]]]:
    mcd_dag_callbacks, mcd_task_callbacks = prepare_mcd_callbacks(config)
    airflow_dag_callbacks, airflow_task_callbacks = prepare_af_callbacks(config)

    dag_callbacks = merge_dicts_by_key(mcd_dag_callbacks, airflow_dag_callbacks)
    task_callbacks = merge_dicts_by_key(mcd_task_callbacks, airflow_task_callbacks)

    return dict(dag_callbacks), dict(task_callbacks)

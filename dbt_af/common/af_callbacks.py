from collections.abc import Iterable
from typing import Optional

from dbt_af.conf import Config
from dbt_af.integrations.af_callbacks import prepare_custom_af_callbacks
from dbt_af.integrations.mcd import prepare_mcd_callbacks


def merge_dicts_by_key(*dicts: dict) -> dict:
    merged_dict = dict()

    for d in dicts:
        for key, value in d.items():
            merged_dict.setdefault(key, [])
            if isinstance(value, Iterable):
                merged_dict[key].extend(list(value))
            else:
                merged_dict[key].append(value)
    return merged_dict


def collect_af_custom_callbacks(
    config: Config,
) -> tuple[dict[str, list[Optional[callable]]], dict[str, list[Optional[callable]]]]:
    mcd_dag_callbacks, mcd_task_callbacks = prepare_mcd_callbacks(config)
    airflow_dag_callbacks, airflow_task_callbacks = prepare_custom_af_callbacks(config)

    dag_callbacks = merge_dicts_by_key(mcd_dag_callbacks, airflow_dag_callbacks)
    task_callbacks = merge_dicts_by_key(mcd_task_callbacks, airflow_task_callbacks)

    return dag_callbacks, task_callbacks

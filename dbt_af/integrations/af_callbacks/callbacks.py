import logging

from dbt_af.conf import Config


def prepare_custom_af_callbacks(config: Config) -> tuple[dict[str, callable], dict[str, callable]]:
    if config.af_callbacks is None:
        logging.warning('No callback were passed')
        return {}, {}

    airflow_task_callbacks = {
        'on_success_callback': config.af_callbacks.task_on_success_callback,
        'on_failure_callback': config.af_callbacks.task_on_failure_callback,
        'on_retry_callback': config.af_callbacks.task_on_retry_callback,
        'on_execute_callback': config.af_callbacks.task_on_execute_callback,
    }

    airflow_dag_callbacks = {
        'on_failure_callback': config.af_callbacks.dag_on_failure_callback,
        'on_success_callback': config.af_callbacks.dag_on_success_callback,
        'sla_miss_callback': config.af_callbacks.dag_sla_miss_callback,
    }

    return airflow_dag_callbacks, airflow_task_callbacks

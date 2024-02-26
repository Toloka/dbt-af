import logging
from typing import Callable, Dict, Tuple

from dbt_af.conf import Config


def prepare_mcd_callbacks(config: Config) -> Tuple[Dict[str, Callable], Dict[str, Callable]]:
    if config.mcd is None:
        logging.warning('No airflow variable for Monte Carlo Data settings')
        return {}, {}

    if not config.mcd.callbacks_enabled:
        logging.info('Monte Carlo Data callbacks are disabled')
        return {}, {}

    try:
        from airflow_mcd.callbacks import mcd_callbacks
    except ModuleNotFoundError:
        logging.error('Package airflow-mcd for MonteCarloData callbacks is not installed')
        return {}, {}

    mcd_dag_callbacks = mcd_callbacks.dag_callbacks if not config.is_dev else {}
    mcd_task_callbacks = mcd_callbacks.task_callbacks if not config.is_dev else {}

    return mcd_dag_callbacks, mcd_task_callbacks

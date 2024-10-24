import logging
import os
from datetime import datetime
from functools import cache, cached_property, partial
from typing import TYPE_CHECKING, Optional, Sequence, Union

from airflow.hooks.subprocess import SubprocessHook
from airflow.models.dag import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.python import PythonSensor
from airflow.utils.state import State
from croniter import croniter, croniter_range

from dbt_af.common.constants import DBT_SENSOR_POOL
from dbt_af.common.scheduling import BaseScheduleTag, ScheduleTag
from dbt_af.conf import Config
from dbt_af.parser.dbt_node_model import WaitPolicy

_DEFAULT_WAIT_TIMEOUT = 24 * 60 * 60
_DEFAULT_POKE_INTERVAL_SECONDS = 30 * 60
_RETRIES_COUNT = 30
_POKE_INTERVALS_SECONDS = {
    ScheduleTag.every15minutes.name: 30,
    ScheduleTag.hourly.name: 2 * 60,
    ScheduleTag.daily.name: 5 * 60,
    ScheduleTag.weekly.name: 30 * 60,
    ScheduleTag.monthly.name: 45 * 60,
}

if TYPE_CHECKING:
    from airflow.utils.task_group import TaskGroup


class _TaskScheduleMapping:
    def __init__(self, wait_policy: WaitPolicy):
        self.wait_policy = wait_policy

        self._mapping = {}

    def add(
        self,
        upstream_schedule_tag: ScheduleTag,
        downstream_schedule_tag: ScheduleTag,
        fn: Union[callable, list[callable]],
    ):
        if not isinstance(fn, list):
            fn = [fn]
        self._mapping[(upstream_schedule_tag.name, downstream_schedule_tag.name)] = fn
        return self

    @staticmethod
    def _update_upstream_cron_args(fn: partial, upstream: BaseScheduleTag, downstream: BaseScheduleTag):
        if fn is None:
            return

        fn.keywords.update({'self_schedule': downstream, 'upstream_schedule': upstream})

    def get(self, key: tuple[BaseScheduleTag, BaseScheduleTag], default=None) -> list[Optional[callable]]:
        """
        If the result function is None (or list of None),
        it means that there is no need to find a specific upstream task, wait for the last one.
        """
        if not isinstance(default, list):
            default = [default]
        stream_names = (key[0].name, key[1].name)
        fns = self._mapping.get(stream_names, default)
        for fn in fns:
            self._update_upstream_cron_args(fn, upstream=key[0], downstream=key[1])

        return fns


def calculate_task_to_wait_execution_date(
    execution_date: datetime,
    self_schedule: BaseScheduleTag,
    upstream_schedule: BaseScheduleTag,
    num_iter: int | None = None,
):
    """
    this function calculates the correct nearest execution date for the upstream task to wait for.

    :param num_iter: number of iterations to go back in time
        by default it's None, which means that the function will return the last possible execution date.
        this parameter is used for the 'all' wait policy to iterate over all possible execution dates
    """
    self_cron = self_schedule.cron_expression()
    upstream_cron = upstream_schedule.cron_expression()
    interval_stop_dttm: datetime = croniter(self_cron.raw_cron_expression, execution_date).get_next(datetime)

    if self_schedule < upstream_schedule:
        cron_iter = croniter(upstream_cron.raw_cron_expression, interval_stop_dttm)
        cron_iter.get_prev()
        return cron_iter.get_prev(datetime)

    all_dts = list(
        croniter_range(execution_date, interval_stop_dttm, upstream_cron.raw_cron_expression, ret_type=datetime)
    )

    if all_dts and all_dts[-1] == interval_stop_dttm:
        all_dts.pop()

    if num_iter is None:
        return all_dts[-1]

    return all_dts[num_iter]


@cache
def get_execution_date_fn_mapping(wait_policy: WaitPolicy) -> _TaskScheduleMapping:
    """
    This function returns mapping of functions to calculate the correct execution dates for sensors in runtime.
    It builds a whole matrix of possible combinations of upstream and downstream schedules and calculates functions.
    """
    _mapping = _TaskScheduleMapping(wait_policy)
    for upstream_schedule_tag in ScheduleTag:
        for downstream_schedule_tag in ScheduleTag:
            if upstream_schedule_tag == downstream_schedule_tag or ScheduleTag.manual in (
                upstream_schedule_tag,
                downstream_schedule_tag,
            ):
                continue
            match wait_policy:
                case WaitPolicy.last:
                    _mapping.add(
                        upstream_schedule_tag,
                        downstream_schedule_tag,
                        partial(calculate_task_to_wait_execution_date),
                    )
                case WaitPolicy.all:
                    embeddings_number = (
                        downstream_schedule_tag()
                        .cron_expression()
                        .embeddings_number(
                            upstream_schedule_tag().cron_expression(),
                            is_upstream_bigger=downstream_schedule_tag < upstream_schedule_tag,
                        )
                    )
                    _mapping.add(
                        upstream_schedule_tag,
                        downstream_schedule_tag,
                        (
                            [
                                partial(calculate_task_to_wait_execution_date, num_iter=i)
                                for i in range(embeddings_number)
                            ]
                            if embeddings_number
                            else [partial(calculate_task_to_wait_execution_date)]
                        ),
                    )
                case _:
                    raise TypeError(f'Unknown wait policy {wait_policy}')

    return _mapping


class AfExecutionDateFn:
    """
    This class is used to get execution dates for sensors.
    Each function operates with execution date (aka logical date, start date)
    and waits for execution date of an upstream task.

    Example:
        daily -> hourly. Daily starts at 00:00 and waits for hourly at 23:00 of the previous day.
        It works like this: for daily task at 2023-01-02T00:00:00 execution date is 2023-01-01T00:00:00, and it will
        wait for hourly task with execution date at 2023-01-01T23:00:00 (it's data interval start!!!)
    """

    def __init__(
        self,
        upstream_schedule_tag: BaseScheduleTag,
        downstream_schedule_tag: BaseScheduleTag,
        wait_policy: WaitPolicy,
    ):
        self.upstream_schedule_tag = upstream_schedule_tag
        self.downstream_schedule_tag = downstream_schedule_tag
        self.wait_policy = wait_policy

    def get_execution_dates(self) -> list[Optional[callable]]:
        return get_execution_date_fn_mapping(self.wait_policy).get(
            (self.upstream_schedule_tag, self.downstream_schedule_tag)
        )


class DbtExternalSensor(ExternalTaskSensor):
    def __init__(
        self,
        dbt_af_config: Config,
        task_id: str,
        task_group: 'Optional[TaskGroup]',
        external_dag_id: str,
        external_task_id: str,
        execution_date_fn: callable,
        dep_schedule: BaseScheduleTag,
        dag: 'DAG',
        **kwargs,
    ) -> None:
        retry_policy = dbt_af_config.retries_config.sensor_retry_policy.as_dict()
        retry_policy['retries'] = max(_RETRIES_COUNT, retry_policy['retries'])
        super().__init__(
            task_id=task_id,
            task_group=task_group,
            external_dag_id=external_dag_id,
            external_task_id=external_task_id,
            execution_date_fn=execution_date_fn,
            dag=dag,
            max_active_tis_per_dag=None,
            pool=DBT_SENSOR_POOL if dbt_af_config.use_dbt_target_specific_pools else None,
            mode='reschedule',
            skipped_states=[State.NONE, State.SKIPPED],
            failed_states=[State.FAILED, State.UPSTREAM_FAILED],
            timeout=6 * 60 * 60,
            poke_interval=_POKE_INTERVALS_SECONDS.get(dep_schedule.name, _DEFAULT_POKE_INTERVAL_SECONDS),
            exponential_backoff=False,
            **retry_policy,
            **kwargs,
        )


class DbtSourceFreshnessSensor(PythonSensor):
    """
    :param wait_timeout: maximum time (in seconds) to wait for the sensor to return True
    :param retries: number of retries that should be performed before failing the sensor.
    :param poke_interval: time (in seconds) that the sensor should wait in between each try
    """

    template_fields: Sequence[str] = ('templates_dict', 'op_args', 'op_kwargs', 'env')
    template_fields_renderers = {'env': 'json'}

    def __init__(
        self,
        task_id: str,
        dag: 'DAG',
        env: dict,
        source_name: str,
        source_identifier: str,
        dbt_af_config: Config,
        target_environment: str = None,
        **kwargs,
    ):
        self.env = env
        self.source_name = source_name
        self.source_identifier = source_identifier
        self.target_environment = target_environment or dbt_af_config.dbt_default_targets.default_for_tests_target
        self.dbt_af_config = dbt_af_config

        retry_policy = dbt_af_config.retries_config.dbt_source_freshness_retry_policy.as_dict()
        if retries := retry_policy.get('retries'):
            logging.debug(
                'For %s number of retries are dynamically calculated and passed value retries=%s will be ignored',
                self.__class__.__name__,
                retries,
            )

        _poke_interval = (
            retry_policy.get('retry_delay').total_seconds()
            if retry_policy.get('retry_delay')
            else _DEFAULT_POKE_INTERVAL_SECONDS
        )
        retry_policy['retries'] = _DEFAULT_WAIT_TIMEOUT // _poke_interval - 1
        retry_policy['poke_interval'] = _poke_interval
        retry_policy['timeout'] = _DEFAULT_WAIT_TIMEOUT

        super().__init__(
            task_id=task_id,
            dag=dag,
            pool=DBT_SENSOR_POOL if dbt_af_config.use_dbt_target_specific_pools else None,
            mode='reschedule',
            python_callable=self._check_freshness,
            **retry_policy,
            **kwargs,
        )

    @cached_property
    def subprocess_hook(self):
        """Returns hook for running the bash command."""
        return SubprocessHook()

    def _check_freshness(self):
        env = os.environ.copy()
        env.update(self.env)
        freshness_cmd = ' && '.join(
            [
                'cd $DBT_PROJECT_DIR',
                'cp -R ./target/* $DBT_TARGET_PATH',
                f'dbt source freshness {"-h" if self.dbt_af_config.is_dev else ""} '
                f'--profiles-dir $DBT_PROFILES_DIR --project-dir $DBT_PROJECT_DIR --target {self.target_environment} '
                f'--select source:{self.source_name}.{self.source_identifier}',
            ]
        )
        result = self.subprocess_hook.run_command(command=['bash', '-c', freshness_cmd], env=env)
        if result.exit_code:
            return False

        return True

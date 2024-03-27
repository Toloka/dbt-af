import datetime
import os
from datetime import timedelta
from functools import cached_property, partial
from typing import TYPE_CHECKING, Callable, Optional, Sequence, Tuple, Union

from airflow.hooks.subprocess import SubprocessHook
from airflow.models.dag import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.python import PythonSensor
from airflow.utils.state import State
from dateutil.relativedelta import relativedelta

from dbt_af.common.constants import DBT_SENSOR_POOL
from dbt_af.common.scheduling import BaseScheduleTag, CronExpression, ScheduleTag
from dbt_af.conf import Config
from dbt_af.parser.dbt_node_model import WaitPolicy

_DEFAULT_WAIT_TIMEOUT = 24 * 60 * 60
_DEFAULT_POKE_INTERVAL_SECONDS = 30 * 60
_RETRIES_COUNT = 30
_POKE_INTERVALS_SECONDS = {
    ScheduleTag.hourly.name: 2 * 60,
    ScheduleTag.daily.name: 5 * 60,
    ScheduleTag.weekly.name: 30 * 60,
    ScheduleTag.monthly.name: 45 * 60,
}

if TYPE_CHECKING:
    from airflow.utils.task_group import TaskGroup


def daily_on_hourly(
    execution_date: datetime.datetime,
    n_hours: int = 23,
    upstream_cron: CronExpression = ScheduleTag.hourly.default_cron_expression,
    **kwargs,
):
    """
    Daily task with data interval [2023-01-01T00:00:00, 2023-01-02T00:00:00] waits for hourly task with data interval
    [2023-01-01T23:00:00, 2023-01-02T00:00:00]

    Daily task with schedule Y X * * * waits for hourly task with schedule Z * * * *. Daily task will have example
    data interval [2023-07-13TXX:YY:00, 2023-07-14TXX:YY:00] and hourly task will have data interval
    [2023-07-13TXX:YY:00 + 23h + Z minutes, 2023-07-13TXX:YY:00 + 24h + Z minutes] for WaitPolicy.last (default) and
    [2023-07-13TXX:YY:00 + i hours + Z minutes, 2023-07-13TXX:YY:00 + (i+1) hours + Z minutes] (i=0..23)
    for WaitPolicy.all

    Execution date is always the start of the data interval.
    """
    return execution_date + timedelta(hours=n_hours, minutes=upstream_cron.minutes)


def hourly_on_daily(
    execution_date: datetime.datetime,
    upstream_cron: CronExpression = ScheduleTag.daily.default_cron_expression,
    **kwargs,
):
    """
    Hourly task with data interval [2023-01-02T05:00:00, 2023-01-02T06:00:00] waits for daily task with data interval
    [2023-01-01T00:00:00, 2023-01-02T00:00:00]
    [2023-01-10T23:00:00, 2023-01-11T00:00:00] --> [2023-01-09T00:00:00, 2023-01-10T00:00:00]

    Hourly task with schedule Z * * * * waits for daily task with schedule Y X * * *. Hourly task will have example
    data interval [2023-07-13TZZ:00:00, 2023-07-13TZZ:00:00 + 1h] and daily task will have data interval
    [2023-07-12TXX:YY:00, 2023-07-13TXX:YY:00] for WaitPolicy.<last, all>
    """
    return execution_date.replace(hour=upstream_cron.hours, minute=upstream_cron.minutes) - timedelta(days=1)


def monthly_on_hourly(
    execution_date: datetime.datetime,
    n_days: int = 0,
    n_hours: int = 0,
    upstream_cron: CronExpression = ScheduleTag.hourly.default_cron_expression,
    **kwargs,
):
    """
    Monthly task with data interval [2023-01-01T00:00:00, 2023-02-01T00:00:00] waits for hourly task with data interval
    [2023-01-31T23:00:00, 2023-02-01T00:00:00]

    Monthly task with schedule Y X D * * waits for hourly task with schedule Z * * * *. Monthly task will have example
    data interval [2023-07-DDTXX:YY:00, 2023-08-DDTXX:YY:00] and hourly task will have data interval
    [2023-07-DDTXX:YY:00 + n_days + n_hours + Z minutes, 2023-07-DDTXX:YY:00 + (n_days + 1) + Z minutes] for
    WaitPolicy.last and [2023-07-DDTXX:YY:00 + j days + i hours + Z minutes, 2023-07-DDTXX:YY:00 + j days + (i+1) hours
    + Z minutes] (i=0..23, j=0..n_days) for WaitPolicy.all

    days=0..days_in_month-1
    hours=0..23
    """
    logical_date = execution_date + relativedelta(months=1)
    upstream_execution_date = logical_date.replace(minute=upstream_cron.minutes) - timedelta(
        days=n_days, hours=n_hours + 1
    )
    if upstream_execution_date + timedelta(hours=1) > logical_date:
        upstream_execution_date -= timedelta(hours=1)
    return upstream_execution_date


def hourly_on_monthly(
    execution_date: datetime.datetime,
    upstream_cron: CronExpression = ScheduleTag.monthly.default_cron_expression,
    **kwargs,
):
    """
    Hourly task with data interval [2023-01-02T05:00:00, 2023-01-02T06:00:00] waits for monthly task with data interval
    [2023-01-01T00:00:00, 2023-02-01T00:00:00]
    [2023-01-10T23:00:00, 2023-01-11T00:00:00] --> [2023-01-01T00:00:00, 2023-02-01T00:00:00]

    Hourly task with schedule Z * * * * waits for monthly task with schedule Y X D * *. Hourly task will have example
    data interval [2023-07-DDTZZ:00:00, 2023-07-DDTZZ:00:00 + 1h] and monthly task will have data interval
    [2023-06-DDTXX:YY:00, 2023-07-DDTXX:YY:00] for WaitPolicy.<last, all>
    """
    months_to_shift = 1
    if (
        execution_date.replace(day=upstream_cron.days, hour=upstream_cron.hours, minute=upstream_cron.minutes)
        > execution_date
    ):
        months_to_shift = 2
    return execution_date.replace(
        day=upstream_cron.days, hour=upstream_cron.hours, minute=upstream_cron.minutes
    ) - relativedelta(months=months_to_shift)


def daily_on_monthly(
    execution_date: datetime.datetime,
    upstream_cron: CronExpression = ScheduleTag.monthly.default_cron_expression,
    **kwargs,
):
    """
    Daily task with data interval [2023-01-01T00:00:00, 2023-02-01T00:00:00] waits for monthly task with data interval
    [2023-01-01T00:00:00, 2023-02-01T00:00:00]
    """
    months_to_shift = 1
    if (
        execution_date.replace(day=upstream_cron.days, hour=upstream_cron.hours, minute=upstream_cron.minutes)
        > execution_date
    ):
        months_to_shift = 2
    return execution_date.replace(
        day=upstream_cron.days, hour=upstream_cron.hours, minute=upstream_cron.minutes
    ) - relativedelta(months=months_to_shift)


def monthly_on_daily(
    execution_date: datetime.datetime,
    n_days: int = 0,
    upstream_cron: CronExpression = ScheduleTag.daily.default_cron_expression,
    **kwargs,
):
    """
    Monthly task with data interval [2023-01-01T00:00:00, 2023-02-01T00:00:00] waits for daily task with data interval
    [2023-01-31T00:00:00, 2023-02-01T00:00:00]

    Monthly task with schedule Y X D * * waits for daily task with schedule Z X * * *. Monthly task will have example
    data interval [2023-07-DDTXX:YY:00, 2023-08-DDTXX:YY:00] and daily task will have data interval
    [2023-07-DDTXX:YY:00 + n_days, 2023-07-DDTXX:YY:00 + n_days + 1] for WaitPolicy.last and
    [2023-07-DDTXX:YY:00 + j days, 2023-07-DDTXX:YY:00 + (j+1) days] (j=0..n_days) for WaitPolicy.all

    """
    logical_date = execution_date + relativedelta(months=1)
    upstream_execution_date = logical_date.replace(hour=upstream_cron.hours, minute=upstream_cron.minutes) - timedelta(
        days=n_days + 1
    )
    if upstream_execution_date + timedelta(days=1) > logical_date:
        upstream_execution_date -= timedelta(days=1)
    return upstream_execution_date


def weekly_on_hourly(
    execution_date: datetime.datetime,
    n_days: int = 6,
    n_hours: int = 23,
    upstream_cron: CronExpression = ScheduleTag.hourly.default_cron_expression,
    **kwargs,
):
    """
    Weekly task with schedule Z Y * * X waits for hourly task with schedule T * * * *. Weekly task will have example
    data interval [2023-07-13TZZ:YY:00, 2023-07-20TZZ:YY:00] and hourly task will have data interval
    [2023-07-13TYY:00:00 + 6d + 23h + T mins, 2023-07-13TYY:00:00 + 7d + T mins] for WaitPolicy.last and
    [2023-07-13TYY:00:00 + j days + i hours + T mins, 2023-07-13TYY:00:00 + j days + (i+1) hours + T mins]
    (i=0..23, j=0..6) for WaitPolicy.all
    """
    return execution_date.replace(minute=0) + timedelta(days=n_days, hours=n_hours, minutes=upstream_cron.minutes)


def weekly_on_daily(
    execution_date: datetime.datetime,
    n_days: int = 6,
    upstream_cron: CronExpression = ScheduleTag.daily.default_cron_expression,
    **kwargs,
):
    """
    Weekly task with data interval [2023-05-10TXX:YY:00, 2023-05-17TXX:YY:00] waits for daily task with data interval
    [2023-05-16TZZ:TT:00, 2023-05-17TZZ:TT:00] for WaitPolicy.last and
    [2023-05-10TZZ:TT:00 + j days, 2023-05-11TZZ:TT:00 + j days] (j=0..6) for WaitPolicy.all
    """
    return execution_date.replace(hour=0, minute=0) + timedelta(
        days=n_days, hours=upstream_cron.hours, minutes=upstream_cron.minutes
    )


def daily_on_weekly(
    execution_date: datetime.datetime,
    upstream_cron: CronExpression = ScheduleTag.weekly.default_cron_expression,
    **kwargs,
):
    """
    Daily task with data interval [2023-01-10T00:00:00, 2023-01-11T00:00:00] waits for weekly task with data interval
    [2023-01-02T00:00:00, 2023-01-09T00:00:00]
    It calculates weekday of the execution date and returns Sunday of the previous week.

    Daily task with schedule Y X * * * waits for weekly task with schedule Z H * * D. Daily task will have example
    data interval [2023-07-13TXX:YY:00, 2023-07-14TXX:YY:00] and weekly task will have data interval
    [2023-07-09THH:ZZ:00, 2023-07-16THH:ZZ:00] for WaitPolicy.<last, all>
    """
    return execution_date.replace(hour=upstream_cron.hours, minute=upstream_cron.minutes) - timedelta(
        days=execution_date.weekday() + 8 - upstream_cron.weekdays
    )


def hourly_on_weekly(
    execution_date: datetime.datetime,
    upstream_cron: CronExpression = ScheduleTag.weekly.default_cron_expression,
    **kwargs,
):
    """
    Hourly task with data interval [2023-10-10T05:XX:00, 2023-10-10T06:XX:00] waits for weekly task with schedule
    Z Y * * D. Hourly task will have example data interval [2023-10-10T05:XX:00, 2023-10-10T06:XX:00] and weekly task
    will have data interval [2023-10-01TYY:ZZ:00 + D days, 2023-10-08TYY:ZZ:00 + D days] for WaitPolicy.<last, all>
    """
    return execution_date.replace(hour=upstream_cron.hours, minute=upstream_cron.minutes) - timedelta(
        days=execution_date.weekday() + 8 - upstream_cron.weekdays
    )


class _TaskScheduleMapping:
    def __init__(self, wait_policy: WaitPolicy):
        self.wait_policy = wait_policy

        self._mapping = {}

    def add(
        self,
        upstream_schedule_tag: ScheduleTag,
        downstream_schedule_tag: ScheduleTag,
        fn: Union[Callable, list[Callable]],
    ):
        if not isinstance(fn, list):
            fn = [fn]
        self._mapping[(upstream_schedule_tag.name, downstream_schedule_tag.name)] = fn
        return self

    @staticmethod
    def _update_upstream_cron_args(fn: partial, upstream: BaseScheduleTag):
        if fn is None:
            return

        upstream_cron = upstream.cron_expression()
        fn.keywords.update({'upstream_cron': upstream_cron})

    def get(self, key: Tuple[BaseScheduleTag, BaseScheduleTag], default=None) -> list[Optional[Callable]]:
        """
        If result function is None (or list of None), it means that there is no need to find specific upstream task,
        just wait for the last one.
        """
        if not isinstance(default, list):
            default = [default]
        stream_names = (key[0].name, key[1].name)
        fns = self._mapping.get(stream_names, default)
        for fn in fns:
            self._update_upstream_cron_args(fn, key[0])

        return fns


EXECUTION_DATE_FN = (
    _TaskScheduleMapping(WaitPolicy.last)
    .add(ScheduleTag.hourly, ScheduleTag.daily, partial(daily_on_hourly, n_hours=23))
    .add(ScheduleTag.daily, ScheduleTag.hourly, partial(hourly_on_daily, n_hours=24))
    .add(ScheduleTag.hourly, ScheduleTag.weekly, partial(weekly_on_hourly, n_days=6, n_hours=23))
    .add(ScheduleTag.daily, ScheduleTag.weekly, partial(weekly_on_daily, n_days=6))
    .add(ScheduleTag.weekly, ScheduleTag.daily, partial(daily_on_weekly, n_days=8))
    .add(ScheduleTag.weekly, ScheduleTag.hourly, partial(hourly_on_weekly, n_days=8))
    .add(ScheduleTag.monthly, ScheduleTag.hourly, partial(hourly_on_monthly))
    .add(ScheduleTag.hourly, ScheduleTag.monthly, partial(monthly_on_hourly))
    .add(ScheduleTag.monthly, ScheduleTag.daily, partial(daily_on_monthly))
    .add(ScheduleTag.daily, ScheduleTag.monthly, partial(monthly_on_daily))
)


EXECUTION_DATE_DEEP_FN = (
    _TaskScheduleMapping(WaitPolicy.all)
    .add(ScheduleTag.hourly, ScheduleTag.daily, [partial(daily_on_hourly, n_hours=i) for i in range(24)])
    .add(ScheduleTag.daily, ScheduleTag.hourly, partial(hourly_on_daily, n_hours=24))
    .add(
        ScheduleTag.hourly,
        ScheduleTag.weekly,
        [partial(weekly_on_hourly, n_days=d, n_hours=h) for d in range(7) for h in range(24)],
    )
    .add(ScheduleTag.daily, ScheduleTag.weekly, [partial(weekly_on_daily, n_days=d) for d in range(7)])
    .add(ScheduleTag.weekly, ScheduleTag.daily, partial(daily_on_weekly, n_days=8))
    .add(ScheduleTag.weekly, ScheduleTag.hourly, partial(hourly_on_weekly, n_days=8))
    .add(
        ScheduleTag.monthly,
        ScheduleTag.hourly,
        [partial(hourly_on_monthly, n_days=d, n_hours=h) for d in range(31) for h in range(24)],
    )
    .add(ScheduleTag.hourly, ScheduleTag.monthly, partial(monthly_on_hourly))
    .add(ScheduleTag.monthly, ScheduleTag.daily, partial(daily_on_monthly))
    .add(ScheduleTag.daily, ScheduleTag.monthly, [partial(monthly_on_daily, n_days=d) for d in range(31)])
)


class AfExecutionDateFn:
    """
    This class is used to get execution dates for sensors. Each funtion operates with execution date (aka logical date,
    start date) and waits for execution date of upstream task.

    Example:
        daily -> hourly. Daily starts at 00:00 and waits for hourly at 23:00 of the previous day.
        It works like this: for daily task at 2023-01-02T00:00:00 execution date is 2023-01-01T00:00:00, and it will
        wait for hourly task with execution date at 2023-01-01T23:00:00 (it's data interval start!!!)
    """

    def __init__(
        self,
        upstream_schedule_tag: BaseScheduleTag,
        downstream_schedule_tag: BaseScheduleTag,
        wait_policy,
    ):
        self.upstream_schedule_tag = upstream_schedule_tag
        self.downstream_schedule_tag = downstream_schedule_tag
        self.wait_policy = wait_policy

    def get_execution_dates(self) -> list[Optional[Callable]]:
        match self.wait_policy:
            case WaitPolicy.last:
                return EXECUTION_DATE_FN.get((self.upstream_schedule_tag, self.downstream_schedule_tag))
            case WaitPolicy.all:
                return EXECUTION_DATE_DEEP_FN.get((self.upstream_schedule_tag, self.downstream_schedule_tag))
            case _:
                raise TypeError(f'Unknown wait policy {self.wait_policy}')


class DbtExternalSensor(ExternalTaskSensor):
    def __init__(
        self,
        dbt_af_config: Config,
        task_id: str,
        task_group: 'Optional[TaskGroup]',
        external_dag_id: str,
        external_task_id: str,
        execution_date_fn: Callable,
        dep_schedule: BaseScheduleTag,
        dag: 'DAG',
        **kwargs,
    ) -> None:
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
            retries=max(_RETRIES_COUNT, kwargs.get('retries', 0)),
            failed_states=[State.NONE, State.SKIPPED, State.FAILED, State.UPSTREAM_FAILED],
            timeout=6 * 60 * 60,
            poke_interval=_POKE_INTERVALS_SECONDS.get(dep_schedule.name, _DEFAULT_POKE_INTERVAL_SECONDS),
            retry_delay=datetime.timedelta(seconds=60 * 30),
            exponential_backoff=False,
            **kwargs,
        )


class DbtSourceFreshnessSensor(PythonSensor):
    """
    :param wait_timeout: maximum time (in seconds) to wait for the sensor to return True
    :param retries: number of retries that should be performed before failing the sensor.
    :param poke_interval: time (in seconds) that the sensor should wait in between each tries
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
        wait_timeout: int = None,
        **kwargs,
    ):
        self.env = env
        self.source_name = source_name
        self.source_identifier = source_identifier
        self.target_environment = target_environment or dbt_af_config.dbt_default_targets.default_for_tests_target
        self.dbt_af_config = dbt_af_config

        if kwargs.get('retries') and wait_timeout:
            raise ValueError('You can not specify both wait_timeout and retries')
        if not kwargs.get('retries') and not wait_timeout:
            wait_timeout = _DEFAULT_WAIT_TIMEOUT

        _poke_interval = kwargs.get('poke_interval', _DEFAULT_POKE_INTERVAL_SECONDS)
        _retries = kwargs.get('retries', wait_timeout // _poke_interval - 1)
        _airflow_task_timeout = max(_DEFAULT_WAIT_TIMEOUT, wait_timeout)

        super().__init__(
            task_id=task_id,
            dag=dag,
            pool=DBT_SENSOR_POOL if dbt_af_config.use_dbt_target_specific_pools else None,
            poke_interval=_poke_interval,
            timeout=_airflow_task_timeout,
            retries=_retries,
            mode='reschedule',
            python_callable=self._check_freshness,
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

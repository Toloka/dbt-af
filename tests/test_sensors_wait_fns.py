import datetime

import pytest
from dateutil.relativedelta import relativedelta

from dbt_af.common.scheduling import _DailyScheduleTag, _HourlyScheduleTag, _MonthlyScheduleTag, _WeeklyScheduleTag
from dbt_af.operators.sensors import AfExecutionDateFn, CronExpression, calculate_task_to_wait_execution_date
from dbt_af.parser.dbt_node_model import WaitPolicy


@pytest.fixture
def execution_date_during_the_day():
    return datetime.datetime(2023, 10, 12, 16, 0, 0)


@pytest.fixture
def execution_date_during_the_night():
    return datetime.datetime(2023, 10, 12, 0, 0, 0)


def test_daily_on_hourly(execution_date_during_the_day, execution_date_during_the_night):
    assert calculate_task_to_wait_execution_date(
        execution_date_during_the_day,
        self_cron=CronExpression('0 16 * * *'),
        upstream_cron=_HourlyScheduleTag.default_cron_expression,
    ) == execution_date_during_the_day + datetime.timedelta(hours=23)
    assert calculate_task_to_wait_execution_date(
        execution_date_during_the_night,
        self_cron=_DailyScheduleTag.default_cron_expression,
        upstream_cron=_HourlyScheduleTag.default_cron_expression,
    ) == execution_date_during_the_night + datetime.timedelta(hours=23)

    for i in range(24):
        assert calculate_task_to_wait_execution_date(
            execution_date_during_the_day,
            self_cron=CronExpression('0 16 * * *'),
            upstream_cron=_HourlyScheduleTag.default_cron_expression,
            num_iter=i,
        ) == execution_date_during_the_day + datetime.timedelta(hours=i)
        assert calculate_task_to_wait_execution_date(
            execution_date_during_the_night,
            self_cron=_DailyScheduleTag.default_cron_expression,
            upstream_cron=_HourlyScheduleTag.default_cron_expression,
            num_iter=i,
        ) == execution_date_during_the_night + datetime.timedelta(hours=i)

    daily_shift = datetime.timedelta(hours=3, minutes=7)
    hourly_shift = datetime.timedelta(minutes=11)
    fn_set_last = AfExecutionDateFn(
        upstream_schedule_tag=_HourlyScheduleTag(hourly_shift),
        downstream_schedule_tag=_DailyScheduleTag(daily_shift),
        wait_policy=WaitPolicy.last,
    ).get_execution_dates()
    assert len(fn_set_last) == 1
    assert fn_set_last[0](
        execution_date_during_the_night.replace(hour=3, minute=7)
    ) == execution_date_during_the_night.replace(hour=3, minute=0) + datetime.timedelta(hours=23, minutes=11)

    fn_set_all = AfExecutionDateFn(
        upstream_schedule_tag=_HourlyScheduleTag(hourly_shift),
        downstream_schedule_tag=_DailyScheduleTag(daily_shift),
        wait_policy=WaitPolicy.all,
    ).get_execution_dates()
    assert len(fn_set_all) == 24
    for i in range(24):
        assert fn_set_all[i](
            execution_date_during_the_night.replace(hour=3, minute=7)
        ) == execution_date_during_the_night.replace(hour=3, minute=0) + datetime.timedelta(hours=i, minutes=11)


def test_hourly_on_daily(execution_date_during_the_day, execution_date_during_the_night):
    assert calculate_task_to_wait_execution_date(
        execution_date_during_the_day,
        self_cron=CronExpression('0 * * * *'),
        upstream_cron=_DailyScheduleTag.default_cron_expression,
    ) == datetime.datetime(2023, 10, 11, 0, 0, 0)
    assert calculate_task_to_wait_execution_date(
        execution_date_during_the_night,
        self_cron=CronExpression('0 * * * *'),
        upstream_cron=_DailyScheduleTag.default_cron_expression,
    ) == datetime.datetime(2023, 10, 11, 0, 0, 0)

    assert calculate_task_to_wait_execution_date(
        execution_date_during_the_day.replace(hour=9),
        self_cron=_HourlyScheduleTag(datetime.timedelta(minutes=20)).cron_expression(),
        upstream_cron=_DailyScheduleTag.default_cron_expression,
    ) == datetime.datetime(2023, 10, 11, 0, 0, 0)
    assert calculate_task_to_wait_execution_date(
        execution_date_during_the_night.replace(hour=7),
        self_cron=_HourlyScheduleTag(datetime.timedelta(minutes=30)).cron_expression(),
        upstream_cron=_DailyScheduleTag(datetime.timedelta(hours=3)).cron_expression(),
    ) == datetime.datetime(2023, 10, 11, 3, 0, 0)

    daily_shift = datetime.timedelta(hours=3, minutes=7)
    hourly_shift = datetime.timedelta(minutes=11)
    fn_set_last = AfExecutionDateFn(
        upstream_schedule_tag=_DailyScheduleTag(daily_shift),
        downstream_schedule_tag=_HourlyScheduleTag(hourly_shift),
        wait_policy=WaitPolicy.last,
    ).get_execution_dates()
    assert len(fn_set_last) == 1
    assert fn_set_last[0](
        execution_date_during_the_night.replace(minute=11)
    ) == execution_date_during_the_night.replace(hour=3, minute=7) - datetime.timedelta(days=2)

    fn_set_all = AfExecutionDateFn(
        upstream_schedule_tag=_DailyScheduleTag(daily_shift),
        downstream_schedule_tag=_HourlyScheduleTag(hourly_shift),
        wait_policy=WaitPolicy.all,
    ).get_execution_dates()
    assert len(fn_set_all) == 1
    assert fn_set_all[0](execution_date_during_the_night.replace(minute=11)) == execution_date_during_the_night.replace(
        hour=3, minute=7
    ) - datetime.timedelta(days=2)


def test_weekly_on_hourly(execution_date_during_the_day, execution_date_during_the_night):
    fn_set_last = AfExecutionDateFn(
        upstream_schedule_tag=_HourlyScheduleTag(),
        downstream_schedule_tag=_WeeklyScheduleTag(),
        wait_policy=WaitPolicy.last,
    ).get_execution_dates()
    assert len(fn_set_last) == 1
    assert fn_set_last[0](execution_date_during_the_night) == execution_date_during_the_night + datetime.timedelta(
        days=3
    ) - datetime.timedelta(hours=1)

    hourly_shift = datetime.timedelta(minutes=11)
    weekly_shift = datetime.timedelta(days=3, hours=7)
    fn_set_all = AfExecutionDateFn(
        upstream_schedule_tag=_HourlyScheduleTag(hourly_shift),
        downstream_schedule_tag=_WeeklyScheduleTag(weekly_shift),
        wait_policy=WaitPolicy.all,
    ).get_execution_dates()
    assert len(fn_set_all) == 168


def test_weekly_on_daily(execution_date_during_the_day, execution_date_during_the_night):
    execution_date_during_the_night = execution_date_during_the_night.replace(day=15)  # end of the week
    assert calculate_task_to_wait_execution_date(
        execution_date_during_the_night,
        self_cron=_WeeklyScheduleTag.default_cron_expression,
        upstream_cron=_DailyScheduleTag.default_cron_expression,
    ) == datetime.datetime(2023, 10, 21)  # 15 + 7 - 1

    daily_shift = datetime.timedelta(hours=3, minutes=7)
    weekly_shift = datetime.timedelta(days=6, hours=23)
    fn_set_last = AfExecutionDateFn(
        upstream_schedule_tag=_DailyScheduleTag(daily_shift),
        downstream_schedule_tag=_WeeklyScheduleTag(weekly_shift),
        wait_policy=WaitPolicy.last,
    ).get_execution_dates()
    assert len(fn_set_last) == 1
    assert fn_set_last[0](execution_date_during_the_night.replace(hour=23)) == datetime.datetime(2023, 10, 21, 3, 7, 0)

    fn_set_all = AfExecutionDateFn(
        upstream_schedule_tag=_DailyScheduleTag(daily_shift),
        downstream_schedule_tag=_WeeklyScheduleTag(weekly_shift),
        wait_policy=WaitPolicy.all,
    ).get_execution_dates()
    assert len(fn_set_all) == 7
    for i in range(7):
        assert fn_set_all[i](execution_date_during_the_night.replace(day=14, hour=23)) == datetime.datetime(
            2023, 10, 15, 3, 7, 0
        ) + datetime.timedelta(days=i)


def test_daily_on_weekly(execution_date_during_the_day, execution_date_during_the_night):
    # [2023-10-12T05:15:00, 2023-10-13T05:15:00] will wait weekly task with cron expression: 45 7 * * 4 at
    # data interval [2023-10-05T07:45:00, 2023-10-12T07:45:00]
    assert calculate_task_to_wait_execution_date(
        datetime.datetime(2023, 10, 12, 5, 15, 0),
        self_cron=CronExpression('15 5 * * *'),
        upstream_cron=CronExpression('45 7 * * 4'),
    ) == datetime.datetime(2023, 10, 5, 7, 45, 0)

    fn_set_last = AfExecutionDateFn(
        upstream_schedule_tag=_WeeklyScheduleTag(),
        downstream_schedule_tag=_DailyScheduleTag(),
        wait_policy=WaitPolicy.last,
    ).get_execution_dates()
    assert len(fn_set_last) == 1
    assert fn_set_last[0](execution_date_during_the_night) == datetime.datetime(2023, 10, 1, 0, 0, 0)
    assert fn_set_last[0](execution_date_during_the_night + datetime.timedelta(days=3)) == datetime.datetime(
        2023, 10, 8, 0, 0, 0
    )
    assert fn_set_last[0](execution_date_during_the_night + datetime.timedelta(days=6)) == datetime.datetime(
        2023, 10, 1, 0, 0, 0
    ) + datetime.timedelta(days=7)

    fn_set_all = AfExecutionDateFn(
        upstream_schedule_tag=_WeeklyScheduleTag(),
        downstream_schedule_tag=_DailyScheduleTag(),
        wait_policy=WaitPolicy.all,
    ).get_execution_dates()
    assert len(fn_set_all) == 1
    assert fn_set_all[0](execution_date_during_the_night) == datetime.datetime(2023, 10, 1, 0, 0, 0)
    assert fn_set_last[0](execution_date_during_the_night + datetime.timedelta(days=6)) == datetime.datetime(
        2023, 10, 1, 0, 0, 0
    ) + datetime.timedelta(days=7)


def test_hourly_on_weekly(execution_date_during_the_day, execution_date_during_the_night):
    # [2023-10-10T05:15:00, 2023-10-10T06:15:00] will wait weekly task with cron expression: 45 7 * * 4 at
    # data interval [2023-10-05T07:45:00, 2023-10-12T07:45:00]
    assert calculate_task_to_wait_execution_date(
        datetime.datetime(2023, 10, 10, 5, 15, 0),
        self_cron=CronExpression('15 * * * *'),
        upstream_cron=CronExpression('45 7 * * 4'),
    ) == datetime.datetime(2023, 9, 28, 7, 45, 0)
    fn_set_last = AfExecutionDateFn(
        upstream_schedule_tag=_WeeklyScheduleTag(),
        downstream_schedule_tag=_HourlyScheduleTag(),
        wait_policy=WaitPolicy.last,
    ).get_execution_dates()
    assert len(fn_set_last) == 1
    assert fn_set_last[0](execution_date_during_the_night) == datetime.datetime(2023, 10, 1, 0, 0, 0)
    assert fn_set_last[0](execution_date_during_the_night + datetime.timedelta(days=3)) == datetime.datetime(
        2023, 10, 8, 0, 0, 0
    )
    assert fn_set_last[0](execution_date_during_the_night + datetime.timedelta(days=6)) == datetime.datetime(
        2023, 10, 1, 0, 0, 0
    ) + datetime.timedelta(days=7)

    weekly_shift = datetime.timedelta(days=3, hours=7)
    hourly_shift = datetime.timedelta(minutes=11)
    fn_set_all = AfExecutionDateFn(
        upstream_schedule_tag=_WeeklyScheduleTag(weekly_shift),
        downstream_schedule_tag=_HourlyScheduleTag(hourly_shift),
        wait_policy=WaitPolicy.all,
    ).get_execution_dates()
    assert len(fn_set_all) == 1
    assert fn_set_all[0](execution_date_during_the_night) == datetime.datetime(
        2023, 10, 1, 7, 0, 0
    ) + datetime.timedelta(days=3)
    assert fn_set_all[0](execution_date_during_the_night + datetime.timedelta(days=6)) == datetime.datetime(
        2023, 10, 4, 7, 0, 0
    )


def test_hourly_on_monthly(execution_date_during_the_day, execution_date_during_the_night):
    assert calculate_task_to_wait_execution_date(
        execution_date_during_the_day,
        self_cron=_HourlyScheduleTag.default_cron_expression,
        upstream_cron=_MonthlyScheduleTag.default_cron_expression,
    ) == datetime.datetime(2023, 9, 1, 0, 0, 0)
    assert calculate_task_to_wait_execution_date(
        execution_date_during_the_night,
        self_cron=_HourlyScheduleTag.default_cron_expression,
        upstream_cron=_MonthlyScheduleTag.default_cron_expression,
    ) == datetime.datetime(2023, 9, 1, 0, 0, 0)

    assert calculate_task_to_wait_execution_date(
        execution_date_during_the_day,
        self_cron=_HourlyScheduleTag.default_cron_expression,
        upstream_cron=_MonthlyScheduleTag(datetime.timedelta(hours=9)).cron_expression(),
    ) == datetime.datetime(2023, 9, 1, 9, 0, 0)
    assert calculate_task_to_wait_execution_date(
        execution_date_during_the_night,
        self_cron=_HourlyScheduleTag.default_cron_expression,
        upstream_cron=_MonthlyScheduleTag(datetime.timedelta(hours=7)).cron_expression(),
    ) == datetime.datetime(2023, 9, 1, 7, 0, 0)

    monthly_shift = datetime.timedelta(days=3, hours=7)
    hourly_shift = datetime.timedelta(minutes=11)
    fn_set_last = AfExecutionDateFn(
        upstream_schedule_tag=_MonthlyScheduleTag(monthly_shift),
        downstream_schedule_tag=_HourlyScheduleTag(hourly_shift),
        wait_policy=WaitPolicy.last,
    ).get_execution_dates()
    assert len(fn_set_last) == 1
    assert fn_set_last[0](
        execution_date_during_the_night.replace(minute=11)
    ) == execution_date_during_the_night.replace(day=3, hour=7) - relativedelta(months=1)

    fn_set_all = AfExecutionDateFn(
        upstream_schedule_tag=_MonthlyScheduleTag(monthly_shift),
        downstream_schedule_tag=_HourlyScheduleTag(hourly_shift),
        wait_policy=WaitPolicy.all,
    ).get_execution_dates()
    assert len(fn_set_all) == 1
    assert fn_set_all[0](execution_date_during_the_night.replace(minute=11)) == execution_date_during_the_night.replace(
        day=3, hour=7
    ) - relativedelta(months=1)


def test_monthly_on_hourly(execution_date_during_the_day, execution_date_during_the_night):
    assert calculate_task_to_wait_execution_date(
        datetime.datetime(2023, 10, 1, 5, 15, 0),
        self_cron=CronExpression('15 5 1 * *'),
        upstream_cron=CronExpression('45 * * * *'),
    ) == datetime.datetime(2023, 11, 1, 4, 45, 0)
    fn_set_last = AfExecutionDateFn(
        upstream_schedule_tag=_HourlyScheduleTag(),
        downstream_schedule_tag=_MonthlyScheduleTag(),
        wait_policy=WaitPolicy.last,
    ).get_execution_dates()
    assert len(fn_set_last) == 1
    assert fn_set_last[0](execution_date_during_the_night) == datetime.datetime(2023, 10, 31, 23, 0, 0)
    assert fn_set_last[0](execution_date_during_the_night + datetime.timedelta(days=3)) == datetime.datetime(
        2023, 10, 28, 23, 0, 0
    ) + datetime.timedelta(days=3)
    assert fn_set_last[0](execution_date_during_the_night + datetime.timedelta(days=6)) == datetime.datetime(
        2023, 10, 25, 23, 0, 0
    ) + datetime.timedelta(days=6)

    fn_set_all = AfExecutionDateFn(
        upstream_schedule_tag=_HourlyScheduleTag(),
        downstream_schedule_tag=_MonthlyScheduleTag(),
        wait_policy=WaitPolicy.all,
    ).get_execution_dates()
    assert len(fn_set_all) == 720


def test_monthly_on_daily(execution_date_during_the_day, execution_date_during_the_night):
    assert calculate_task_to_wait_execution_date(
        datetime.datetime(2023, 10, 12, 5, 15, 0),
        self_cron=CronExpression('15 5 12 * *'),
        upstream_cron=CronExpression('45 7 * * *'),
    ) == datetime.datetime(2023, 11, 11, 7, 45, 0)
    fn_set_last = AfExecutionDateFn(
        upstream_schedule_tag=_DailyScheduleTag(),
        downstream_schedule_tag=_MonthlyScheduleTag(),
        wait_policy=WaitPolicy.last,
    ).get_execution_dates()
    assert len(fn_set_last) == 1
    assert fn_set_last[0](execution_date_during_the_night) == datetime.datetime(2023, 10, 31)
    assert fn_set_last[0](execution_date_during_the_night + datetime.timedelta(days=3)) == datetime.datetime(
        2023, 10, 28, 0, 0, 0
    ) + datetime.timedelta(days=3)
    assert fn_set_last[0](execution_date_during_the_night + datetime.timedelta(days=6)) == datetime.datetime(
        2023, 10, 25
    ) + datetime.timedelta(days=6)

    fn_set_all = AfExecutionDateFn(
        upstream_schedule_tag=_DailyScheduleTag(),
        downstream_schedule_tag=_MonthlyScheduleTag(),
        wait_policy=WaitPolicy.all,
    ).get_execution_dates()
    assert len(fn_set_all) == 30
    assert fn_set_all[0](execution_date_during_the_night.replace(day=1)) == datetime.datetime(2023, 10, 1)
    assert fn_set_all[-1](execution_date_during_the_night.replace(day=1)) == datetime.datetime(2023, 10, 30)


def test_daily_on_monthly(execution_date_during_the_day, execution_date_during_the_night):
    assert calculate_task_to_wait_execution_date(
        datetime.datetime(2023, 10, 12, 5, 15, 0),
        self_cron=CronExpression('15 5 * * *'),
        upstream_cron=CronExpression('45 7 13 * *'),
    ) == datetime.datetime(2023, 8, 13, 7, 45, 0)
    fn_set_last = AfExecutionDateFn(
        upstream_schedule_tag=_MonthlyScheduleTag(),
        downstream_schedule_tag=_DailyScheduleTag(),
        wait_policy=WaitPolicy.last,
    ).get_execution_dates()
    assert len(fn_set_last) == 1
    assert fn_set_last[0](execution_date_during_the_night) == datetime.datetime(2023, 9, 1, 0, 0, 0)
    assert fn_set_last[0](execution_date_during_the_night + datetime.timedelta(days=3)) == datetime.datetime(
        2023, 9, 1, 0, 0, 0
    )
    assert fn_set_last[0](execution_date_during_the_night + datetime.timedelta(days=6)) == datetime.datetime(
        2023, 9, 1, 0, 0, 0
    )

    fn_set_all = AfExecutionDateFn(
        upstream_schedule_tag=_MonthlyScheduleTag(),
        downstream_schedule_tag=_DailyScheduleTag(),
        wait_policy=WaitPolicy.all,
    ).get_execution_dates()
    assert len(fn_set_all) == 1
    assert fn_set_all[0](execution_date_during_the_night) == datetime.datetime(2023, 9, 1, 0, 0, 0)

    assert fn_set_last[0](execution_date_during_the_night + datetime.timedelta(days=6)) == datetime.datetime(
        2023, 9, 1, 0, 0, 0
    )

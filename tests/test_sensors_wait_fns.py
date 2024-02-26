import datetime

import pytest
from dateutil.relativedelta import relativedelta

from dbt_af.common.scheduling import _DailyScheduleTag, _HourlyScheduleTag, _MonthlyScheduleTag, _WeeklyScheduleTag
from dbt_af.operators.sensors import (
    AfExecutionDateFn,
    CronExpression,
    daily_on_hourly,
    daily_on_monthly,
    daily_on_weekly,
    hourly_on_daily,
    hourly_on_monthly,
    hourly_on_weekly,
    monthly_on_daily,
    monthly_on_hourly,
    weekly_on_daily,
    weekly_on_hourly,
)
from dbt_af.parser.dbt_node_model import WaitPolicy


@pytest.fixture
def now_dttm():
    return datetime.datetime.now().replace(second=0, microsecond=0)


@pytest.fixture
def execution_date_during_the_day():
    return datetime.datetime(2023, 10, 12, 16, 0, 0)


@pytest.fixture
def execution_date_during_the_night():
    return datetime.datetime(2023, 10, 12, 0, 0, 0)


def test_daily_on_hourly(now_dttm, execution_date_during_the_day, execution_date_during_the_night):
    # daily task with execution date: 2023-03-17T07:32:00 waits for hourly task with cron expression: 13 * * * *
    assert daily_on_hourly(now_dttm, upstream_cron=CronExpression('13 * * * *')) == now_dttm + datetime.timedelta(
        hours=23, minutes=13
    )

    assert daily_on_hourly(execution_date_during_the_day) == execution_date_during_the_day + datetime.timedelta(
        hours=23
    )
    assert daily_on_hourly(execution_date_during_the_night) == execution_date_during_the_night + datetime.timedelta(
        hours=23
    )

    for i in range(24):
        assert daily_on_hourly(
            execution_date_during_the_day, n_hours=i
        ) == execution_date_during_the_day + datetime.timedelta(hours=i)
        assert daily_on_hourly(
            execution_date_during_the_night, n_hours=i
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
    ) == execution_date_during_the_night.replace(hour=3, minute=7) + datetime.timedelta(hours=23, minutes=11)

    fn_set_all = AfExecutionDateFn(
        upstream_schedule_tag=_HourlyScheduleTag(hourly_shift),
        downstream_schedule_tag=_DailyScheduleTag(daily_shift),
        wait_policy=WaitPolicy.all,
    ).get_execution_dates()
    assert len(fn_set_all) == 24
    for i in range(24):
        assert fn_set_all[i](
            execution_date_during_the_night.replace(hour=3, minute=7)
        ) == execution_date_during_the_night.replace(hour=3, minute=7) + datetime.timedelta(hours=i, minutes=11)


def test_hourly_on_daily(now_dttm, execution_date_during_the_day, execution_date_during_the_night):
    # hourly task with execution date: 2023-03-17T07:32:00 waits for daily task with cron expression: 13 7 * * *
    assert hourly_on_daily(now_dttm, upstream_cron=CronExpression('13 7 * * *')) == now_dttm.replace(
        hour=7, minute=13
    ) - datetime.timedelta(days=1)

    assert hourly_on_daily(execution_date_during_the_day) == datetime.datetime(2023, 10, 11, 0, 0, 0)
    assert hourly_on_daily(execution_date_during_the_night) == datetime.datetime(2023, 10, 11, 0, 0, 0)

    assert hourly_on_daily(
        execution_date_during_the_day, _DailyScheduleTag(datetime.timedelta(hours=9)).cron_expression()
    ) == datetime.datetime(2023, 10, 11, 9, 0, 0)
    assert hourly_on_daily(
        execution_date_during_the_night, _DailyScheduleTag(datetime.timedelta(hours=7)).cron_expression()
    ) == datetime.datetime(2023, 10, 11, 7, 0, 0)

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
    ) == execution_date_during_the_night.replace(hour=3, minute=7) - datetime.timedelta(days=1)

    fn_set_all = AfExecutionDateFn(
        upstream_schedule_tag=_DailyScheduleTag(daily_shift),
        downstream_schedule_tag=_HourlyScheduleTag(hourly_shift),
        wait_policy=WaitPolicy.all,
    ).get_execution_dates()
    assert len(fn_set_all) == 1
    assert fn_set_all[0](execution_date_during_the_night.replace(minute=11)) == execution_date_during_the_night.replace(
        hour=3, minute=7
    ) - datetime.timedelta(days=1)


def test_weekly_on_hourly(now_dttm, execution_date_during_the_day, execution_date_during_the_night):
    # weekly task with execution date: 2023-03-17T07:32:00 waits for hourly task with cron expression: 13 7 * * *
    assert weekly_on_hourly(now_dttm, upstream_cron=CronExpression('13 7 * * *')) == now_dttm.replace(
        minute=0
    ) + datetime.timedelta(days=6, hours=23, minutes=13)
    fn_set_last = AfExecutionDateFn(
        upstream_schedule_tag=_HourlyScheduleTag(),
        downstream_schedule_tag=_WeeklyScheduleTag(),
        wait_policy=WaitPolicy.last,
    ).get_execution_dates()
    assert len(fn_set_last) == 1
    assert fn_set_last[0](execution_date_during_the_night) == execution_date_during_the_night + datetime.timedelta(
        days=7
    ) - datetime.timedelta(hours=1)

    hourly_shift = datetime.timedelta(minutes=11)
    weekly_shift = datetime.timedelta(days=3, hours=7)
    fn_set_all = AfExecutionDateFn(
        upstream_schedule_tag=_HourlyScheduleTag(hourly_shift),
        downstream_schedule_tag=_WeeklyScheduleTag(weekly_shift),
        wait_policy=WaitPolicy.all,
    ).get_execution_dates()
    assert len(fn_set_all) == 168
    for d in range(7):
        for h in range(24):
            assert fn_set_all[d * 24 + h](execution_date_during_the_night) == execution_date_during_the_night.replace(
                minute=0
            ) + datetime.timedelta(days=d, hours=h, minutes=11)


def test_weekly_on_daily(now_dttm, execution_date_during_the_day, execution_date_during_the_night):
    # weekly task with execution date: 2023-03-17T07:32:00 waits for daily task with cron expression: 13 7 * * *
    assert weekly_on_daily(now_dttm, upstream_cron=CronExpression('13 7 * * *')) == now_dttm.replace(
        hour=0, minute=0
    ) + datetime.timedelta(days=6, hours=7, minutes=13)

    assert weekly_on_daily(execution_date_during_the_night) == datetime.datetime(2023, 10, 18, 0, 0, 0)

    daily_shift = datetime.timedelta(hours=3, minutes=7)
    weekly_shift = datetime.timedelta(days=6, hours=23)
    fn_set_last = AfExecutionDateFn(
        upstream_schedule_tag=_DailyScheduleTag(daily_shift),
        downstream_schedule_tag=_WeeklyScheduleTag(weekly_shift),
        wait_policy=WaitPolicy.last,
    ).get_execution_dates()
    assert len(fn_set_last) == 1
    assert fn_set_last[0](execution_date_during_the_night.replace(hour=23)) == datetime.datetime(2023, 10, 18, 3, 7, 0)

    fn_set_all = AfExecutionDateFn(
        upstream_schedule_tag=_DailyScheduleTag(daily_shift),
        downstream_schedule_tag=_WeeklyScheduleTag(weekly_shift),
        wait_policy=WaitPolicy.all,
    ).get_execution_dates()
    assert len(fn_set_all) == 7
    for i in range(7):
        assert fn_set_all[i](execution_date_during_the_night.replace(hour=23)) == datetime.datetime(
            2023, 10, 12, 3, 7, 0
        ) + datetime.timedelta(days=i)


def test_daily_on_weekly(now_dttm, execution_date_during_the_day, execution_date_during_the_night):
    # [2023-10-12T05:15:00, 2023-10-13T05:15:00] will wait weekly task with cron expression: 45 7 * * 4 at
    # data interval [2023-10-05T07:45:00, 2023-10-12T07:45:00]
    assert daily_on_weekly(
        datetime.datetime(2023, 10, 12, 5, 15, 0), upstream_cron=CronExpression('45 7 * * 4')
    ) == datetime.datetime(2023, 10, 5, 7, 45, 0)

    fn_set_last = AfExecutionDateFn(
        upstream_schedule_tag=_WeeklyScheduleTag(),
        downstream_schedule_tag=_DailyScheduleTag(),
        wait_policy=WaitPolicy.last,
    ).get_execution_dates()
    assert len(fn_set_last) == 1
    assert fn_set_last[0](execution_date_during_the_night) == datetime.datetime(2023, 10, 1, 0, 0, 0)
    assert fn_set_last[0](execution_date_during_the_night + datetime.timedelta(days=3)) == datetime.datetime(
        2023, 10, 1, 0, 0, 0
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


def test_hourly_on_weekly(now_dttm, execution_date_during_the_day, execution_date_during_the_night):
    # [2023-10-10T05:15:00, 2023-10-10T06:15:00] will wait weekly task with cron expression: 45 7 * * 4 at
    # data interval [2023-10-05T07:45:00, 2023-10-12T07:45:00]
    assert hourly_on_weekly(
        datetime.datetime(2023, 10, 10, 5, 15, 0), upstream_cron=CronExpression('45 7 * * 4')
    ) == datetime.datetime(2023, 10, 5, 7, 45, 0)
    fn_set_last = AfExecutionDateFn(
        upstream_schedule_tag=_WeeklyScheduleTag(),
        downstream_schedule_tag=_HourlyScheduleTag(),
        wait_policy=WaitPolicy.last,
    ).get_execution_dates()
    assert len(fn_set_last) == 1
    assert fn_set_last[0](execution_date_during_the_night) == datetime.datetime(2023, 10, 1, 0, 0, 0)
    assert fn_set_last[0](execution_date_during_the_night + datetime.timedelta(days=3)) == datetime.datetime(
        2023, 10, 1, 0, 0, 0
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
        2023, 10, 1, 7, 0, 0
    ) + datetime.timedelta(days=7 + 3)


def test_hourly_on_monthly(now_dttm, execution_date_during_the_day, execution_date_during_the_night):
    # monthly task with cron expression: 13 7 15 * *
    months_to_shift = 1
    if now_dttm.replace(day=15, hour=7, minute=13) > now_dttm:
        months_to_shift = 2
    assert hourly_on_monthly(now_dttm, upstream_cron=CronExpression('13 7 15 * *')) == now_dttm.replace(
        day=15, hour=7, minute=13
    ) - relativedelta(months=months_to_shift)

    assert hourly_on_monthly(execution_date_during_the_day) == datetime.datetime(2023, 9, 1, 0, 0, 0)
    assert hourly_on_monthly(execution_date_during_the_night) == datetime.datetime(2023, 9, 1, 0, 0, 0)

    assert hourly_on_monthly(
        execution_date_during_the_day, _MonthlyScheduleTag(datetime.timedelta(hours=9)).cron_expression()
    ) == datetime.datetime(2023, 9, 1, 9, 0, 0)
    assert hourly_on_monthly(
        execution_date_during_the_night, _MonthlyScheduleTag(datetime.timedelta(hours=7)).cron_expression()
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
    assert len(fn_set_all) == 744
    assert fn_set_all[0](execution_date_during_the_night.replace(minute=11)) == execution_date_during_the_night.replace(
        day=3, hour=7
    ) - relativedelta(months=1)


def test_monthly_on_hourly(now_dttm, execution_date_during_the_day, execution_date_during_the_night):
    assert monthly_on_hourly(
        datetime.datetime(2023, 10, 12, 5, 15, 0), upstream_cron=CronExpression('45 * * * *')
    ) == datetime.datetime(2023, 11, 12, 3, 45, 0)
    fn_set_last = AfExecutionDateFn(
        upstream_schedule_tag=_HourlyScheduleTag(),
        downstream_schedule_tag=_MonthlyScheduleTag(),
        wait_policy=WaitPolicy.last,
    ).get_execution_dates()
    assert len(fn_set_last) == 1
    assert fn_set_last[0](execution_date_during_the_night) == datetime.datetime(2023, 11, 11, 23, 0, 0)
    assert fn_set_last[0](execution_date_during_the_night + datetime.timedelta(days=3)) == datetime.datetime(
        2023, 11, 11, 23, 0, 0
    ) + datetime.timedelta(days=3)
    assert fn_set_last[0](execution_date_during_the_night + datetime.timedelta(days=6)) == datetime.datetime(
        2023, 11, 11, 23, 0, 0
    ) + datetime.timedelta(days=6)

    fn_set_all = AfExecutionDateFn(
        upstream_schedule_tag=_HourlyScheduleTag(),
        downstream_schedule_tag=_MonthlyScheduleTag(),
        wait_policy=WaitPolicy.all,
    ).get_execution_dates()
    assert len(fn_set_all) == 1
    assert fn_set_all[0](execution_date_during_the_night) == datetime.datetime(2023, 11, 11, 23, 0, 0)
    assert fn_set_last[0](execution_date_during_the_night + datetime.timedelta(days=6)) == datetime.datetime(
        2023, 11, 11, 23, 0, 0
    ) + datetime.timedelta(days=6)


def test_monthly_on_daily(now_dttm, execution_date_during_the_day, execution_date_during_the_night):
    assert monthly_on_daily(
        datetime.datetime(2023, 10, 12, 5, 15, 0), upstream_cron=CronExpression('45 7 * * *')
    ) == datetime.datetime(2023, 11, 10, 7, 45, 0)
    fn_set_last = AfExecutionDateFn(
        upstream_schedule_tag=_DailyScheduleTag(),
        downstream_schedule_tag=_MonthlyScheduleTag(),
        wait_policy=WaitPolicy.last,
    ).get_execution_dates()
    assert len(fn_set_last) == 1
    assert fn_set_last[0](execution_date_during_the_night) == datetime.datetime(2023, 11, 11, 0, 0, 0)
    assert fn_set_last[0](execution_date_during_the_night + datetime.timedelta(days=3)) == datetime.datetime(
        2023, 11, 11, 0, 0, 0
    ) + datetime.timedelta(days=3)
    assert fn_set_last[0](execution_date_during_the_night + datetime.timedelta(days=6)) == datetime.datetime(
        2023, 11, 11, 0, 0, 0
    ) + datetime.timedelta(days=6)

    fn_set_all = AfExecutionDateFn(
        upstream_schedule_tag=_DailyScheduleTag(),
        downstream_schedule_tag=_MonthlyScheduleTag(),
        wait_policy=WaitPolicy.all,
    ).get_execution_dates()
    assert len(fn_set_all) == 31
    assert fn_set_all[0](execution_date_during_the_night) == datetime.datetime(2023, 11, 11, 0, 0, 0)
    assert fn_set_all[-1](execution_date_during_the_night) == datetime.datetime(2023, 10, 12, 0, 0, 0)

    assert fn_set_last[0](execution_date_during_the_night + datetime.timedelta(days=6)) == datetime.datetime(
        2023, 11, 11, 0, 0, 0
    ) + datetime.timedelta(days=6)


def test_daily_on_monthly(now_dttm, execution_date_during_the_day, execution_date_during_the_night):
    assert daily_on_monthly(
        datetime.datetime(2023, 10, 12, 5, 15, 0), upstream_cron=CronExpression('45 7 13 * *')
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

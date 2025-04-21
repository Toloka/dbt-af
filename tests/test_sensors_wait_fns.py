from datetime import datetime, timedelta

import pytest
from dateutil.relativedelta import relativedelta

from dbt_af.common.scheduling import (
    _DailyScheduleTag,
    _HourlyScheduleTag,
    _MonthlyScheduleTag,
    _WeeklyScheduleTag,
)
from dbt_af.operators.sensors import AfExecutionDateFn, calculate_task_to_wait_execution_date
from dbt_af.parser.dbt_node_model import WaitPolicy


@pytest.fixture
def execution_date_during_the_day():
    return datetime(2023, 10, 12, 16, 0, 0)


@pytest.fixture
def execution_date_during_the_night():
    return datetime(2023, 10, 12, 0, 0, 0)


def test_daily_on_hourly(execution_date_during_the_day, execution_date_during_the_night):
    assert calculate_task_to_wait_execution_date(
        execution_date_during_the_day,
        self_schedule=_DailyScheduleTag(timedelta(hours=16)),
        upstream_schedule=_HourlyScheduleTag(),
    ) == execution_date_during_the_day + timedelta(hours=23)
    assert calculate_task_to_wait_execution_date(
        execution_date_during_the_night,
        self_schedule=_DailyScheduleTag(),
        upstream_schedule=_HourlyScheduleTag(),
    ) == execution_date_during_the_night + timedelta(hours=23)

    for i in range(24):
        assert calculate_task_to_wait_execution_date(
            execution_date_during_the_day,
            self_schedule=_DailyScheduleTag(timedelta(hours=16)),
            upstream_schedule=_HourlyScheduleTag(),
            num_iter=i,
        ) == execution_date_during_the_day + timedelta(hours=i)
        assert calculate_task_to_wait_execution_date(
            execution_date_during_the_night,
            self_schedule=_DailyScheduleTag(),
            upstream_schedule=_HourlyScheduleTag(),
            num_iter=i,
        ) == execution_date_during_the_night + timedelta(hours=i)

    daily_shift = timedelta(hours=3, minutes=7)
    hourly_shift = timedelta(minutes=11)
    fn_set_last = AfExecutionDateFn(
        upstream_schedule_tag=_HourlyScheduleTag(hourly_shift),
        downstream_schedule_tag=_DailyScheduleTag(daily_shift),
        wait_policy=WaitPolicy.last,
    ).get_execution_dates()
    assert len(fn_set_last) == 1
    assert fn_set_last[0](
        execution_date_during_the_night.replace(hour=3, minute=7)
    ) == execution_date_during_the_night.replace(hour=3, minute=0) + timedelta(hours=23, minutes=11)

    fn_set_all = AfExecutionDateFn(
        upstream_schedule_tag=_HourlyScheduleTag(hourly_shift),
        downstream_schedule_tag=_DailyScheduleTag(daily_shift),
        wait_policy=WaitPolicy.all,
    ).get_execution_dates()
    assert len(fn_set_all) == 24
    for i in range(24):
        assert fn_set_all[i](
            execution_date_during_the_night.replace(hour=3, minute=7)
        ) == execution_date_during_the_night.replace(hour=3, minute=0) + timedelta(hours=i, minutes=11)


def test_hourly_on_daily(execution_date_during_the_day, execution_date_during_the_night):
    assert calculate_task_to_wait_execution_date(
        execution_date_during_the_day,
        self_schedule=_HourlyScheduleTag(),
        upstream_schedule=_DailyScheduleTag(),
    ) == datetime(2023, 10, 11, 0, 0, 0)
    assert calculate_task_to_wait_execution_date(
        execution_date_during_the_night,
        self_schedule=_HourlyScheduleTag(),
        upstream_schedule=_DailyScheduleTag(),
    ) == datetime(2023, 10, 11, 0, 0, 0)

    assert calculate_task_to_wait_execution_date(
        execution_date_during_the_day.replace(hour=9),
        self_schedule=_HourlyScheduleTag(timedelta(minutes=20)),
        upstream_schedule=_DailyScheduleTag(),
    ) == datetime(2023, 10, 11, 0, 0, 0)
    assert calculate_task_to_wait_execution_date(
        execution_date_during_the_night.replace(hour=7),
        self_schedule=_HourlyScheduleTag(timedelta(minutes=30)),
        upstream_schedule=_DailyScheduleTag(timedelta(hours=3)),
    ) == datetime(2023, 10, 11, 3, 0, 0)

    daily_shift = timedelta(hours=3, minutes=7)
    hourly_shift = timedelta(minutes=11)
    fn_set_last = AfExecutionDateFn(
        upstream_schedule_tag=_DailyScheduleTag(daily_shift),
        downstream_schedule_tag=_HourlyScheduleTag(hourly_shift),
        wait_policy=WaitPolicy.last,
    ).get_execution_dates()
    assert len(fn_set_last) == 1
    assert fn_set_last[0](
        execution_date_during_the_night.replace(minute=11)
    ) == execution_date_during_the_night.replace(hour=3, minute=7) - timedelta(days=2)

    fn_set_all = AfExecutionDateFn(
        upstream_schedule_tag=_DailyScheduleTag(daily_shift),
        downstream_schedule_tag=_HourlyScheduleTag(hourly_shift),
        wait_policy=WaitPolicy.all,
    ).get_execution_dates()
    assert len(fn_set_all) == 1
    assert fn_set_all[0](execution_date_during_the_night.replace(minute=11)) == execution_date_during_the_night.replace(
        hour=3, minute=7
    ) - timedelta(days=2)


def test_weekly_on_hourly(execution_date_during_the_day, execution_date_during_the_night):
    fn_set_last = AfExecutionDateFn(
        upstream_schedule_tag=_HourlyScheduleTag(),
        downstream_schedule_tag=_WeeklyScheduleTag(),
        wait_policy=WaitPolicy.last,
    ).get_execution_dates()
    assert len(fn_set_last) == 1
    assert fn_set_last[0](execution_date_during_the_night) == execution_date_during_the_night + timedelta(
        days=3
    ) - timedelta(hours=1)

    hourly_shift = timedelta(minutes=11)
    weekly_shift = timedelta(days=3, hours=7)
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
        self_schedule=_WeeklyScheduleTag(),
        upstream_schedule=_DailyScheduleTag(),
    ) == datetime(2023, 10, 21)  # 15 + 7 - 1

    daily_shift = timedelta(hours=3, minutes=7)
    weekly_shift = timedelta(days=6, hours=23)
    fn_set_last = AfExecutionDateFn(
        upstream_schedule_tag=_DailyScheduleTag(daily_shift),
        downstream_schedule_tag=_WeeklyScheduleTag(weekly_shift),
        wait_policy=WaitPolicy.last,
    ).get_execution_dates()
    assert len(fn_set_last) == 1
    assert fn_set_last[0](execution_date_during_the_night.replace(hour=23)) == datetime(2023, 10, 21, 3, 7, 0)

    fn_set_all = AfExecutionDateFn(
        upstream_schedule_tag=_DailyScheduleTag(daily_shift),
        downstream_schedule_tag=_WeeklyScheduleTag(weekly_shift),
        wait_policy=WaitPolicy.all,
    ).get_execution_dates()
    assert len(fn_set_all) == 7
    for i in range(7):
        assert fn_set_all[i](execution_date_during_the_night.replace(day=14, hour=23)) == datetime(
            2023, 10, 15, 3, 7, 0
        ) + timedelta(days=i)


def test_daily_on_weekly(execution_date_during_the_day, execution_date_during_the_night):
    # [2023-10-12T05:15:00, 2023-10-13T05:15:00] will wait weekly task with cron expression: 45 7 * * 4 at
    # data interval [2023-10-05T07:45:00, 2023-10-12T07:45:00]
    assert calculate_task_to_wait_execution_date(
        datetime(2023, 10, 12, 5, 15, 0),
        self_schedule=_DailyScheduleTag(timedelta(hours=5, minutes=15)),
        upstream_schedule=_WeeklyScheduleTag(timedelta(hours=7, minutes=45, days=4)),
    ) == datetime(2023, 10, 5, 7, 45, 0)

    fn_set_last = AfExecutionDateFn(
        upstream_schedule_tag=_WeeklyScheduleTag(),
        downstream_schedule_tag=_DailyScheduleTag(),
        wait_policy=WaitPolicy.last,
    ).get_execution_dates()
    assert len(fn_set_last) == 1
    assert fn_set_last[0](execution_date_during_the_night) == datetime(2023, 10, 1, 0, 0, 0)
    assert fn_set_last[0](execution_date_during_the_night + timedelta(days=3)) == datetime(2023, 10, 8, 0, 0, 0)
    assert fn_set_last[0](execution_date_during_the_night + timedelta(days=6)) == datetime(
        2023, 10, 1, 0, 0, 0
    ) + timedelta(days=7)

    fn_set_all = AfExecutionDateFn(
        upstream_schedule_tag=_WeeklyScheduleTag(),
        downstream_schedule_tag=_DailyScheduleTag(),
        wait_policy=WaitPolicy.all,
    ).get_execution_dates()
    assert len(fn_set_all) == 1
    assert fn_set_all[0](execution_date_during_the_night) == datetime(2023, 10, 1, 0, 0, 0)
    assert fn_set_last[0](execution_date_during_the_night + timedelta(days=6)) == datetime(
        2023, 10, 1, 0, 0, 0
    ) + timedelta(days=7)


def test_hourly_on_weekly(execution_date_during_the_day, execution_date_during_the_night):
    # [2023-10-10T05:15:00, 2023-10-10T06:15:00] will wait weekly task with cron expression: 45 7 * * 4 at
    # data interval [2023-10-05T07:45:00, 2023-10-12T07:45:00]
    assert calculate_task_to_wait_execution_date(
        datetime(2023, 10, 10, 5, 15, 0),
        self_schedule=_HourlyScheduleTag(timedelta(minutes=15)),
        upstream_schedule=_WeeklyScheduleTag(timedelta(hours=7, minutes=45, days=4)),
    ) == datetime(2023, 9, 28, 7, 45, 0)
    fn_set_last = AfExecutionDateFn(
        upstream_schedule_tag=_WeeklyScheduleTag(),
        downstream_schedule_tag=_HourlyScheduleTag(),
        wait_policy=WaitPolicy.last,
    ).get_execution_dates()
    assert len(fn_set_last) == 1
    assert fn_set_last[0](execution_date_during_the_night) == datetime(2023, 10, 1, 0, 0, 0)
    assert fn_set_last[0](execution_date_during_the_night + timedelta(days=3)) == datetime(2023, 10, 8, 0, 0, 0)
    assert fn_set_last[0](execution_date_during_the_night + timedelta(days=6)) == datetime(
        2023, 10, 1, 0, 0, 0
    ) + timedelta(days=7)

    weekly_shift = timedelta(days=3, hours=7)
    hourly_shift = timedelta(minutes=11)
    fn_set_all = AfExecutionDateFn(
        upstream_schedule_tag=_WeeklyScheduleTag(weekly_shift),
        downstream_schedule_tag=_HourlyScheduleTag(hourly_shift),
        wait_policy=WaitPolicy.all,
    ).get_execution_dates()
    assert len(fn_set_all) == 1
    assert fn_set_all[0](execution_date_during_the_night) == datetime(2023, 10, 1, 7, 0, 0) + timedelta(days=3)
    assert fn_set_all[0](execution_date_during_the_night + timedelta(days=6)) == datetime(2023, 10, 4, 7, 0, 0)


def test_hourly_on_monthly(execution_date_during_the_day, execution_date_during_the_night):
    assert calculate_task_to_wait_execution_date(
        execution_date_during_the_day,
        self_schedule=_HourlyScheduleTag(),
        upstream_schedule=_MonthlyScheduleTag(),
    ) == datetime(2023, 9, 1, 0, 0, 0)
    assert calculate_task_to_wait_execution_date(
        execution_date_during_the_night,
        self_schedule=_HourlyScheduleTag(),
        upstream_schedule=_MonthlyScheduleTag(),
    ) == datetime(2023, 9, 1, 0, 0, 0)

    assert calculate_task_to_wait_execution_date(
        execution_date_during_the_day,
        self_schedule=_HourlyScheduleTag(),
        upstream_schedule=_MonthlyScheduleTag(timedelta(hours=9)),
    ) == datetime(2023, 9, 1, 9, 0, 0)
    assert calculate_task_to_wait_execution_date(
        execution_date_during_the_night,
        self_schedule=_HourlyScheduleTag(),
        upstream_schedule=_MonthlyScheduleTag(timedelta(hours=7)),
    ) == datetime(2023, 9, 1, 7, 0, 0)

    monthly_shift = timedelta(days=3, hours=7)
    hourly_shift = timedelta(minutes=11)
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
        datetime(2023, 10, 1, 5, 15, 0),
        self_schedule=_MonthlyScheduleTag(timedelta(hours=5, minutes=15, days=1)),
        upstream_schedule=_HourlyScheduleTag(timedelta(minutes=45)),
    ) == datetime(2023, 11, 1, 4, 45, 0)
    fn_set_last = AfExecutionDateFn(
        upstream_schedule_tag=_HourlyScheduleTag(),
        downstream_schedule_tag=_MonthlyScheduleTag(),
        wait_policy=WaitPolicy.last,
    ).get_execution_dates()
    assert len(fn_set_last) == 1
    assert fn_set_last[0](execution_date_during_the_night) == datetime(2023, 10, 31, 23, 0, 0)
    assert fn_set_last[0](execution_date_during_the_night + timedelta(days=3)) == datetime(
        2023, 10, 28, 23, 0, 0
    ) + timedelta(days=3)
    assert fn_set_last[0](execution_date_during_the_night + timedelta(days=6)) == datetime(
        2023, 10, 25, 23, 0, 0
    ) + timedelta(days=6)

    fn_set_all = AfExecutionDateFn(
        upstream_schedule_tag=_HourlyScheduleTag(),
        downstream_schedule_tag=_MonthlyScheduleTag(),
        wait_policy=WaitPolicy.all,
    ).get_execution_dates()
    assert 28 * 24 <= len(fn_set_all) == 31 * 24


def test_monthly_on_daily(execution_date_during_the_day, execution_date_during_the_night):
    assert calculate_task_to_wait_execution_date(
        datetime(2023, 10, 12, 5, 15, 0),
        self_schedule=_MonthlyScheduleTag(timedelta(hours=5, minutes=15, days=12)),
        upstream_schedule=_DailyScheduleTag(timedelta(hours=7, minutes=45)),
    ) == datetime(2023, 11, 11, 7, 45, 0)
    fn_set_last = AfExecutionDateFn(
        upstream_schedule_tag=_DailyScheduleTag(),
        downstream_schedule_tag=_MonthlyScheduleTag(),
        wait_policy=WaitPolicy.last,
    ).get_execution_dates()
    assert len(fn_set_last) == 1
    assert fn_set_last[0](execution_date_during_the_night) == datetime(2023, 10, 31)
    assert fn_set_last[0](execution_date_during_the_night + timedelta(days=3)) == datetime(
        2023, 10, 28, 0, 0, 0
    ) + timedelta(days=3)
    assert fn_set_last[0](execution_date_during_the_night + timedelta(days=6)) == datetime(2023, 10, 25) + timedelta(
        days=6
    )

    fn_set_all = AfExecutionDateFn(
        upstream_schedule_tag=_DailyScheduleTag(),
        downstream_schedule_tag=_MonthlyScheduleTag(),
        wait_policy=WaitPolicy.all,
    ).get_execution_dates()
    assert 28 <= len(fn_set_all) <= 31
    assert fn_set_all[0](execution_date_during_the_night.replace(day=1)) == datetime(2023, 10, 1)
    assert fn_set_all[-1](execution_date_during_the_night.replace(day=1)) == datetime(2023, 10, 31)


def test_daily_on_monthly(execution_date_during_the_day, execution_date_during_the_night):
    assert calculate_task_to_wait_execution_date(
        datetime(2023, 10, 12, 5, 15, 0),
        self_schedule=_DailyScheduleTag(timedelta(hours=5, minutes=15)),
        upstream_schedule=_MonthlyScheduleTag(timedelta(hours=7, minutes=45, days=13)),
    ) == datetime(2023, 8, 13, 7, 45, 0)
    fn_set_last = AfExecutionDateFn(
        upstream_schedule_tag=_MonthlyScheduleTag(),
        downstream_schedule_tag=_DailyScheduleTag(),
        wait_policy=WaitPolicy.last,
    ).get_execution_dates()
    assert len(fn_set_last) == 1
    assert fn_set_last[0](execution_date_during_the_night) == datetime(2023, 9, 1, 0, 0, 0)
    assert fn_set_last[0](execution_date_during_the_night + timedelta(days=3)) == datetime(2023, 9, 1, 0, 0, 0)
    assert fn_set_last[0](execution_date_during_the_night + timedelta(days=6)) == datetime(2023, 9, 1, 0, 0, 0)

    fn_set_all = AfExecutionDateFn(
        upstream_schedule_tag=_MonthlyScheduleTag(),
        downstream_schedule_tag=_DailyScheduleTag(),
        wait_policy=WaitPolicy.all,
    ).get_execution_dates()
    assert len(fn_set_all) == 1
    assert fn_set_all[0](execution_date_during_the_night) == datetime(2023, 9, 1, 0, 0, 0)

    assert fn_set_last[0](execution_date_during_the_night + timedelta(days=6)) == datetime(2023, 9, 1, 0, 0, 0)


def test_hourly_with_shift_15_m_on_hourly_with_shift_30_m():
    """
    @hourly_shift_15_minutes with an interval [16:15, 17:15]
    waits for @hourly_shift_30_minutes with an interval [15:30, 16:30]
    """
    assert calculate_task_to_wait_execution_date(
        datetime(2023, 10, 12, 16, 15, 0),
        self_schedule=_HourlyScheduleTag(timedelta(minutes=15)),
        upstream_schedule=_HourlyScheduleTag(timedelta(minutes=30)),
    ) == datetime(2023, 10, 12, 15, 30, 0)


def test_hourly_with_shift_30_m_on_hourly_with_shift_15_m():
    """
    @hourly_shift_30_minutes with an interval [16:30, 17:30]
    waits for @hourly_shift_15_minutes with an interval [16:15, 17:15]
    """
    assert calculate_task_to_wait_execution_date(
        datetime(2023, 10, 12, 16, 30, 0),
        self_schedule=_HourlyScheduleTag(timedelta(minutes=30)),
        upstream_schedule=_HourlyScheduleTag(timedelta(minutes=15)),
    ) == datetime(2023, 10, 12, 16, 15, 0)


@pytest.mark.parametrize(
    'upstream_schedule, downstream_schedule, execution_date, expected_wait_execution_date',
    [
        # hourly
        (
            _HourlyScheduleTag(),
            _HourlyScheduleTag(),
            datetime(2023, 10, 12, 16, 0, 0),
            datetime(2023, 10, 12, 16, 0, 0),
        ),
        (
            _HourlyScheduleTag(timedelta(minutes=30)),
            _HourlyScheduleTag(),
            datetime(2023, 10, 12, 16, 0, 0),
            datetime(2023, 10, 12, 15, 30, 0),
        ),
        (
            _HourlyScheduleTag(),
            _HourlyScheduleTag(timedelta(minutes=30)),
            datetime(2023, 10, 12, 16, 30, 0),
            datetime(2023, 10, 12, 16, 0, 0),
        ),
        # daily
        (
            _DailyScheduleTag(),
            _DailyScheduleTag(),
            datetime(2023, 10, 12, 0, 0, 0),
            datetime(2023, 10, 12, 0, 0, 0),
        ),
        (
            _DailyScheduleTag(timedelta(hours=3)),
            _DailyScheduleTag(),
            datetime(2023, 10, 12, 0, 0, 0),
            datetime(2023, 10, 11, 3, 0, 0),
        ),
        (
            _DailyScheduleTag(),
            _DailyScheduleTag(timedelta(hours=3)),
            datetime(2023, 10, 12, 3, 0, 0),
            datetime(2023, 10, 12, 0, 0, 0),
        ),
        # monthly
        (
            _MonthlyScheduleTag(),
            _MonthlyScheduleTag(),
            datetime(2023, 10, 1, 0, 0, 0),
            datetime(2023, 10, 1, 0, 0, 0),
        ),
        (
            _MonthlyScheduleTag(timedelta(days=7)),
            _MonthlyScheduleTag(),
            datetime(2023, 10, 1, 0, 0, 0),
            datetime(2023, 9, 7, 0, 0, 0),
        ),
        (
            _MonthlyScheduleTag(),
            _MonthlyScheduleTag(timedelta(days=7)),
            datetime(2023, 10, 7, 0, 0, 0),
            datetime(2023, 10, 1, 0, 0, 0),
        ),
    ],
)
def test_schedule_on_same_schedule(
    upstream_schedule, downstream_schedule, execution_date, expected_wait_execution_date
):
    assert (
        calculate_task_to_wait_execution_date(
            execution_date,
            self_schedule=downstream_schedule,
            upstream_schedule=upstream_schedule,
        )
        == expected_wait_execution_date
    )

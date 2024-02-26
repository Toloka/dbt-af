import datetime

import pytest

from dbt_af.common.scheduling import (
    _DailyScheduleTag,
    _HourlyScheduleTag,
    _ManualScheduleTag,
    _MonthlyScheduleTag,
    _WeeklyScheduleTag,
)


def test_manual_schedule_tag():
    assert _ManualScheduleTag().af_repr() is None
    assert _ManualScheduleTag().name == '@manual'
    assert _ManualScheduleTag().safe_name == 'dbt_manual'
    assert _ManualScheduleTag().timeshift is None
    assert _ManualScheduleTag(datetime.timedelta(days=1)).timeshift is None


def test_hourly_schedule_tag():
    assert _HourlyScheduleTag().af_repr() == '@hourly'
    assert _HourlyScheduleTag().name == '@hourly'
    assert _HourlyScheduleTag().safe_name == 'dbt_hourly'
    assert _HourlyScheduleTag().timeshift == _HourlyScheduleTag.default_timeshift
    assert _HourlyScheduleTag(datetime.timedelta(minutes=30)).timeshift == datetime.timedelta(minutes=30)
    assert _HourlyScheduleTag(datetime.timedelta(minutes=30)).af_repr() == '30 * * * *'

    assert _HourlyScheduleTag(datetime.timedelta(minutes=59)).timeshift == datetime.timedelta(minutes=59)
    assert _HourlyScheduleTag(datetime.timedelta(minutes=59)).af_repr() == '59 * * * *'

    bad_timeshifts = [
        datetime.timedelta(minutes=60),
        datetime.timedelta(minutes=61),
        datetime.timedelta(minutes=100),
        datetime.timedelta(hours=1),
        datetime.timedelta(hours=2),
        datetime.timedelta(days=1),
        datetime.timedelta(days=2),
    ]

    for shift in bad_timeshifts:
        with pytest.raises(ValueError):
            _HourlyScheduleTag(shift).af_repr()


def test_daily_schedule_tag():
    assert _DailyScheduleTag().af_repr() == '@daily'
    assert _DailyScheduleTag().name == '@daily'
    assert _DailyScheduleTag().safe_name == 'dbt_daily'
    assert _DailyScheduleTag().timeshift == _DailyScheduleTag.default_timeshift
    assert _DailyScheduleTag(datetime.timedelta(hours=1)).timeshift == datetime.timedelta(hours=1)
    assert _DailyScheduleTag(datetime.timedelta(hours=1)).af_repr() == '0 1 * * *'

    assert _DailyScheduleTag(datetime.timedelta(hours=23)).timeshift == datetime.timedelta(hours=23)
    assert _DailyScheduleTag(datetime.timedelta(hours=23)).af_repr() == '0 23 * * *'

    bad_timeshifts = [
        datetime.timedelta(hours=24),
        datetime.timedelta(hours=25),
        datetime.timedelta(hours=100),
        datetime.timedelta(days=1),
        datetime.timedelta(days=2),
    ]

    for shift in bad_timeshifts:
        with pytest.raises(ValueError):
            _DailyScheduleTag(shift).af_repr()


def test_weekly_schedule_tag():
    assert _WeeklyScheduleTag().af_repr() == '@weekly'
    assert _WeeklyScheduleTag().name == '@weekly'
    assert _WeeklyScheduleTag().safe_name == 'dbt_weekly'
    assert _WeeklyScheduleTag().timeshift == _WeeklyScheduleTag.default_timeshift
    assert _WeeklyScheduleTag(datetime.timedelta(days=1)).timeshift == datetime.timedelta(days=1)
    assert _WeeklyScheduleTag(datetime.timedelta(days=1)).af_repr() == '0 0 * * 1'

    assert _WeeklyScheduleTag(datetime.timedelta(days=6)).timeshift == datetime.timedelta(days=6)
    assert _WeeklyScheduleTag(datetime.timedelta(days=6)).af_repr() == '0 0 * * 6'

    assert _WeeklyScheduleTag(datetime.timedelta(minutes=22, hours=5, days=3)).af_repr() == '22 5 * * 3'

    bad_timeshifts = [
        datetime.timedelta(days=7),
        datetime.timedelta(days=8),
        datetime.timedelta(days=100),
    ]

    for shift in bad_timeshifts:
        with pytest.raises(ValueError):
            _WeeklyScheduleTag(shift).af_repr()


def test_monthly_schedule_tag():
    assert _MonthlyScheduleTag().af_repr() == '@monthly'
    assert _MonthlyScheduleTag().name == '@monthly'
    assert _MonthlyScheduleTag().safe_name == 'dbt_monthly'
    assert _MonthlyScheduleTag().timeshift is _MonthlyScheduleTag.default_timeshift
    assert _MonthlyScheduleTag(datetime.timedelta(days=1)).timeshift == datetime.timedelta(days=1)
    assert _MonthlyScheduleTag(datetime.timedelta(days=1)).af_repr() == '0 0 1 * *'

    assert _MonthlyScheduleTag(datetime.timedelta(days=30)).timeshift == datetime.timedelta(days=30)
    assert _MonthlyScheduleTag(datetime.timedelta(days=30)).af_repr() == '0 0 30 * *'

    assert _MonthlyScheduleTag(datetime.timedelta(minutes=22, hours=5, days=3)).af_repr() == '22 5 3 * *'

    bad_timeshifts = [
        datetime.timedelta(days=32),
        datetime.timedelta(minutes=60, hours=23, days=31),
        datetime.timedelta(days=100),
    ]

    for shift in bad_timeshifts:
        with pytest.raises(ValueError):
            _MonthlyScheduleTag(shift).af_repr()

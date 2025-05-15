import datetime

import pytest
from freezegun import freeze_time

from dbt_af.common.scheduling import (
    BaseScheduleTag,
    EScheduleTag,
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
    with pytest.raises(ValueError):
        _ManualScheduleTag(datetime.timedelta(days=1))


def test_hourly_schedule_tag():
    assert _HourlyScheduleTag().af_repr() == '0 * * * *'
    assert _HourlyScheduleTag().name == '@hourly'
    assert _HourlyScheduleTag().safe_name == 'dbt_hourly'
    assert _HourlyScheduleTag().timeshift == _HourlyScheduleTag.default_timeshift
    assert _HourlyScheduleTag(datetime.timedelta(minutes=30)).timeshift == datetime.timedelta(minutes=30)
    assert _HourlyScheduleTag(datetime.timedelta(minutes=30)).af_repr() == '30 * * * *'

    assert _HourlyScheduleTag(datetime.timedelta(minutes=59)).timeshift == datetime.timedelta(minutes=59)
    assert _HourlyScheduleTag(datetime.timedelta(minutes=59)).af_repr() == '59 * * * *'

    assert _HourlyScheduleTag(datetime.timedelta(minutes=22)).name == '@hourly_shift_22_minutes'

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
        assert _HourlyScheduleTag(shift).af_repr() == '59 * * * *'
        assert _HourlyScheduleTag(shift).name == '@hourly_shift_59_minutes'
        assert _HourlyScheduleTag(shift).timeshift == datetime.timedelta(minutes=59)


def test_daily_schedule_tag():
    assert _DailyScheduleTag().af_repr() == '0 0 * * *'
    assert _DailyScheduleTag().name == '@daily'
    assert _DailyScheduleTag().safe_name == 'dbt_daily'
    assert _DailyScheduleTag().timeshift == _DailyScheduleTag.default_timeshift
    assert _DailyScheduleTag(datetime.timedelta(hours=1)).timeshift == datetime.timedelta(hours=1)
    assert _DailyScheduleTag(datetime.timedelta(hours=1)).af_repr() == '0 1 * * *'

    assert _DailyScheduleTag(datetime.timedelta(hours=23)).timeshift == datetime.timedelta(hours=23)
    assert _DailyScheduleTag(datetime.timedelta(hours=23)).af_repr() == '0 23 * * *'

    assert _DailyScheduleTag(datetime.timedelta(minutes=22)).name == '@daily_shift_22_minutes'
    assert _DailyScheduleTag(datetime.timedelta(hours=5)).name == '@daily_shift_5_hours'
    assert _DailyScheduleTag(datetime.timedelta(minutes=22, hours=5)).name == '@daily_shift_5_hours_22_minutes'

    bad_timeshifts = [
        datetime.timedelta(hours=24),
        datetime.timedelta(hours=25),
        datetime.timedelta(hours=100),
        datetime.timedelta(days=1),
        datetime.timedelta(days=2),
    ]

    for shift in bad_timeshifts:
        assert _DailyScheduleTag(shift).af_repr() == '59 23 * * *'
        assert _DailyScheduleTag(shift).name == '@daily_shift_23_hours_59_minutes'
        assert _DailyScheduleTag(shift).timeshift == datetime.timedelta(hours=23, minutes=59)


def test_weekly_schedule_tag():
    assert _WeeklyScheduleTag().af_repr() == '0 0 * * 0'
    assert _WeeklyScheduleTag().name == '@weekly'
    assert _WeeklyScheduleTag().safe_name == 'dbt_weekly'
    assert _WeeklyScheduleTag().timeshift == _WeeklyScheduleTag.default_timeshift
    assert _WeeklyScheduleTag(datetime.timedelta(days=1)).timeshift == datetime.timedelta(days=1)
    assert _WeeklyScheduleTag(datetime.timedelta(days=1)).af_repr() == '0 0 * * 1'

    assert _WeeklyScheduleTag(datetime.timedelta(days=6)).timeshift == datetime.timedelta(days=6)
    assert _WeeklyScheduleTag(datetime.timedelta(days=6)).af_repr() == '0 0 * * 6'

    assert _WeeklyScheduleTag(datetime.timedelta(minutes=22, hours=5, days=3)).af_repr() == '22 5 * * 3'

    assert _WeeklyScheduleTag(datetime.timedelta(minutes=22)).name == '@weekly_shift_22_minutes'
    assert _WeeklyScheduleTag(datetime.timedelta(hours=5)).name == '@weekly_shift_5_hours'
    assert _WeeklyScheduleTag(datetime.timedelta(days=3)).name == '@weekly_shift_3_days'
    assert (
        _WeeklyScheduleTag(datetime.timedelta(minutes=22, hours=5, days=3)).name
        == '@weekly_shift_3_days_5_hours_22_minutes'
    )

    bad_timeshifts = [
        datetime.timedelta(days=7),
        datetime.timedelta(days=8),
        datetime.timedelta(days=100),
    ]

    for shift in bad_timeshifts:
        assert _WeeklyScheduleTag(shift).af_repr() == '59 23 * * 6'
        assert _WeeklyScheduleTag(shift).name == '@weekly_shift_6_days_23_hours_59_minutes'
        assert _WeeklyScheduleTag(shift).timeshift == datetime.timedelta(days=6, hours=23, minutes=59)


@freeze_time(datetime.datetime(2023, 11, 1))
def test_monthly_schedule_tag():
    assert _MonthlyScheduleTag().af_repr() == '0 0 1 * *'
    assert _MonthlyScheduleTag().name == '@monthly'
    assert _MonthlyScheduleTag().safe_name == 'dbt_monthly'
    assert _MonthlyScheduleTag().timeshift is _MonthlyScheduleTag.default_timeshift
    assert _MonthlyScheduleTag(datetime.timedelta(days=1)).timeshift == datetime.timedelta(days=1)
    assert _MonthlyScheduleTag(datetime.timedelta(days=1)).af_repr() == '0 0 1 * *'

    assert _MonthlyScheduleTag(datetime.timedelta(days=30)).timeshift == datetime.timedelta(days=30)
    assert _MonthlyScheduleTag(datetime.timedelta(days=30)).af_repr() == '0 0 30 * *'

    assert _MonthlyScheduleTag(datetime.timedelta(minutes=22, hours=5, days=3)).af_repr() == '22 5 3 * *'

    assert _MonthlyScheduleTag(datetime.timedelta(minutes=22)).name == '@monthly_shift_22_minutes'
    assert _MonthlyScheduleTag(datetime.timedelta(hours=5)).name == '@monthly_shift_5_hours'
    assert _MonthlyScheduleTag(datetime.timedelta(days=3)).name == '@monthly_shift_3_days'
    assert (
        _MonthlyScheduleTag(datetime.timedelta(minutes=22, hours=5, days=3)).name
        == '@monthly_shift_3_days_5_hours_22_minutes'
    )

    bad_timeshifts = [
        datetime.timedelta(days=32),
        datetime.timedelta(minutes=60, hours=23, days=31),
        datetime.timedelta(days=100),
    ]

    for shift in bad_timeshifts:
        assert _MonthlyScheduleTag(shift).af_repr() == '59 23 30 * *'
        assert _MonthlyScheduleTag(shift).name == '@monthly_shift_30_days_23_hours_59_minutes'
        assert _MonthlyScheduleTag(shift).timeshift == datetime.timedelta(days=30, hours=23, minutes=59)


def test_scheduling_tag_levels_all_unique():
    all_levels = [tag().level for tag in EScheduleTag]
    assert len(all_levels) == len(set(all_levels))


def test_scheduling_tag_correct_comparison():
    assert (
        EScheduleTag.manual()
        < EScheduleTag.every15minutes()
        < EScheduleTag.hourly()
        < EScheduleTag.daily()
        < EScheduleTag.weekly()
        < EScheduleTag.monthly()
    )


@pytest.mark.parametrize(
    'schedule_tag, shift, base_name, full_name',
    [
        ('manual', None, '@manual', '@manual'),
        ('every15minutes', None, '@every15minutes', '@every15minutes'),
        (
            'every15minutes',
            datetime.timedelta(minutes=14),
            '@every15minutes',
            '@every15minutes_shift_14_minutes',
        ),
        ('hourly', None, '@hourly', '@hourly'),
        ('hourly', datetime.timedelta(minutes=22), '@hourly', '@hourly_shift_22_minutes'),
        ('daily', None, '@daily', '@daily'),
        ('daily', datetime.timedelta(minutes=22), '@daily', '@daily_shift_22_minutes'),
        ('weekly', None, '@weekly', '@weekly'),
        ('weekly', datetime.timedelta(minutes=22), '@weekly', '@weekly_shift_22_minutes'),
        ('monthly', None, '@monthly', '@monthly'),
        ('monthly', datetime.timedelta(minutes=22), '@monthly', '@monthly_shift_22_minutes'),
    ],
)
def test_base_schedule_name(schedule_tag, shift, base_name, full_name):
    schedule: BaseScheduleTag = EScheduleTag[schedule_tag](shift)  # noqa
    assert schedule.base_name == base_name
    assert schedule.name == full_name

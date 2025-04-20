import datetime

import pytest

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
        with pytest.raises(ValueError):
            _HourlyScheduleTag(shift).af_repr()


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
        with pytest.raises(ValueError):
            _DailyScheduleTag(shift).af_repr()


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
        with pytest.raises(ValueError):
            _WeeklyScheduleTag(shift).af_repr()


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
        with pytest.raises(ValueError):
            _MonthlyScheduleTag(shift).af_repr()


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
    'schedule_tag, shift, base_name, full_name, expected_error_raise',
    [
        ('manual', None, '@manual', '@manual', False),
        ('manual', datetime.timedelta(minutes=22), '@manual', '', True),
        ('every15minutes', None, '@every15minutes', '@every15minutes', False),
        (
            'every15minutes',
            datetime.timedelta(minutes=14),
            '@every15minutes',
            '@every15minutes_shift_14_minutes',
            False,
        ),
        ('every15minutes', datetime.timedelta(minutes=16), '@every15minutes', '', True),
        ('hourly', None, '@hourly', '@hourly', False),
        ('hourly', datetime.timedelta(minutes=22), '@hourly', '@hourly_shift_22_minutes', False),
        ('hourly', datetime.timedelta(minutes=61), '@hourly', '', True),
        ('daily', None, '@daily', '@daily', False),
        ('daily', datetime.timedelta(minutes=22), '@daily', '@daily_shift_22_minutes', False),
        ('daily', datetime.timedelta(hours=25), '@daily', '', True),
        ('weekly', None, '@weekly', '@weekly', False),
        ('weekly', datetime.timedelta(minutes=22), '@weekly', '@weekly_shift_22_minutes', False),
        ('weekly', datetime.timedelta(days=8), '@weekly', '', True),
        ('monthly', None, '@monthly', '@monthly', False),
        ('monthly', datetime.timedelta(minutes=22), '@monthly', '@monthly_shift_22_minutes', False),
        ('monthly', datetime.timedelta(days=50), '@monthly', '', True),
    ],
)
def test_base_schedule_name(schedule_tag, shift, base_name, full_name, expected_error_raise):
    if expected_error_raise:
        with pytest.raises(ValueError):
            EScheduleTag[schedule_tag](shift)
    else:
        schedule: BaseScheduleTag = EScheduleTag[schedule_tag](shift)  # noqa
        assert schedule.base_name == base_name
        assert schedule.name == full_name

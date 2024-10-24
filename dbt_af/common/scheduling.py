import datetime
import re
from abc import ABC, abstractmethod
from enum import Enum
from functools import cache
from typing import List, Optional, Union

from attrs import define, field
from croniter import croniter, croniter_range


@define(order=False)
class CronExpression:
    raw_cron_expression: str = field(validator=lambda _, __, val: croniter.is_valid(val))

    def __eq__(self, other):
        if not isinstance(other, (CronExpression, str)):
            return NotImplemented
        if isinstance(other, str):
            return self.raw_cron_expression == other
        return self.raw_cron_expression == other.raw_cron_expression

    def embeddings_number(self, other: 'CronExpression | None', is_upstream_bigger: bool) -> int:
        """
        Calculate how many times this cron expression can be embedded into another one over a given period.
        """
        if other is None:
            return 0
        if not isinstance(other, CronExpression):
            raise ValueError(f'Comparison must be with another CronExpression, got {other}')
        if is_upstream_bigger:
            return 0

        now = datetime.datetime.now()
        self_cron_iter = croniter(self.raw_cron_expression, now)
        start_interval = self_cron_iter.get_next(datetime.datetime)
        end_interval = self_cron_iter.get_next(datetime.datetime)

        dts = list(
            croniter_range(
                start_interval,
                end_interval,
                other.raw_cron_expression,
                ret_type=datetime.datetime,
                expand_from_start_time=True,
            )
        )
        if dts and dts[-1] == end_interval:
            # both ends are inclusive, so we need to remove the last element if it's equal to the end_interval
            dts.pop()

        return len(dts)

    def _split_cron_expression(self) -> List[str]:
        return self.raw_cron_expression.split()

    @property
    def minutes(self) -> int:
        minutes_part = self._split_cron_expression()[0]
        if minutes_part == '*':
            return 0
        return int(minutes_part)

    @property
    def hours(self) -> int:
        hours_part = self._split_cron_expression()[1]
        if hours_part == '*':
            return 0
        return int(hours_part)

    @property
    def days(self) -> int:
        days_part = self._split_cron_expression()[2]
        if days_part == '*':
            return 1
        return int(days_part)

    @property
    def months(self) -> int:
        months_part = self._split_cron_expression()[3]
        if months_part == '*':
            return 1
        return int(months_part)

    @property
    def weekdays(self) -> int:
        weekdays_part = self._split_cron_expression()[4]
        if weekdays_part == '*':
            return 0
        return int(weekdays_part)


class BaseScheduleTag(ABC):
    timeshift: Optional[datetime.timedelta]
    default_timeshift = datetime.timedelta()

    @abstractmethod
    def __init__(self, timeshift: Optional[datetime.timedelta] = None):
        raise NotImplementedError()

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError()

    @property
    @abstractmethod
    def level(self) -> int:
        """
        Is used for comparison of schedule tags.
        Must be unique for each schedule tag.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def default_cron_expression(self) -> CronExpression | None:
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def _af_repr_impl(full_days_shift: int, full_hours_shift: int, rest_minutes_shift: int) -> str | None:
        raise NotImplementedError()

    def __str__(self):
        return self.name

    def __hash__(self):
        return hash(self.name)

    def __lt__(self, other):
        if not isinstance(other, BaseScheduleTag):
            return NotImplemented
        return self.level < other.level

    def __eq__(self, other: Union[str, 'BaseScheduleTag']):
        if isinstance(other, str):
            return self.name == other
        if isinstance(other, BaseScheduleTag):
            return self.name == other.name and self.timeshift == other.timeshift
        raise TypeError(f'Cannot compare {self} with {other}')

    @property
    def safe_name(self):
        return re.sub(r'^@', 'dbt_', self.name)

    def split_timeshift(self) -> tuple[int, int, int]:
        full_days_shift = self.timeshift.days
        full_hours_shift = self.timeshift.seconds // 3600
        rest_minutes_shift = (self.timeshift.seconds % 3600) // 60

        return full_days_shift, full_hours_shift, rest_minutes_shift

    @cache
    def af_repr(self) -> str | None:
        """
        Returns airflow-like schedule representation.
        Could be cron expression or None if there's no schedule.
        """
        full_days_shift, full_hours_shift, rest_minutes_shift = self.split_timeshift()
        return self._af_repr_impl(full_days_shift, full_hours_shift, rest_minutes_shift)

    def cron_expression(self) -> CronExpression | None:
        af_schedule = self.af_repr()
        if af_schedule is None:
            return None
        return CronExpression(af_schedule)


class _MonthlyScheduleTag(BaseScheduleTag):
    name = '@monthly'
    default_cron_expression = CronExpression('0 0 1 * *')
    level = 5

    def __init__(self, timeshift: Optional[datetime.timedelta] = None):
        self.timeshift = timeshift or self.default_timeshift

    def _af_repr_impl(self, full_days_shift: int, full_hours_shift: int, rest_minutes_shift: int):
        """
        base cron expression: 0 0 1 * * (or @monthly)
        """

        if full_days_shift > 31:
            raise ValueError(f'Monthly schedule tag supports only shifts between 1 and 31 days, got {self.timeshift}')
        if full_hours_shift > 23:
            raise ValueError(f'Monthly schedule tag supports only shifts between 0 and 23 hours, got {self.timeshift}')
        if rest_minutes_shift > 59:
            raise ValueError(
                f'Monthly schedule tag supports only shifts between 0 and 59 minutes, got {self.timeshift}'
            )
        if full_days_shift == 0:
            full_days_shift = '1'

        return f'{rest_minutes_shift} {full_hours_shift} {full_days_shift} * *'


class _WeeklyScheduleTag(BaseScheduleTag):
    name = '@weekly'
    default_cron_expression = CronExpression('0 0 * * 0')
    level = 4

    def __init__(self, timeshift: Optional[datetime.timedelta] = None):
        self.timeshift = timeshift or self.default_timeshift

    def _af_repr_impl(self, full_days_shift: int, full_hours_shift: int, rest_minutes_shift: int):
        """
        base cron expression: 0 0 * * 0 (or @weekly)
        """

        if full_days_shift > 6:
            raise ValueError(f'Weekly schedule tag supports only shifts between 0 and 6 days, got {self.timeshift}')

        return f'{rest_minutes_shift} {full_hours_shift} * * {full_days_shift}'


class _DailyScheduleTag(BaseScheduleTag):
    name = '@daily'
    default_cron_expression = CronExpression('0 0 * * *')
    level = 3

    def __init__(self, timeshift: Optional[datetime.timedelta] = None):
        self.timeshift = timeshift or self.default_timeshift

    def _af_repr_impl(self, full_days_shift: int, full_hours_shift: int, rest_minutes_shift: int):
        """
        base cron expression: 0 0 * * * (or @daily)
        """

        if full_days_shift:
            raise ValueError(f'Daily schedule tag supports only shifts between 0 and 23 hours, got {self.timeshift}')

        return f'{rest_minutes_shift} {full_hours_shift} * * *'


class _HourlyScheduleTag(BaseScheduleTag):
    name = '@hourly'
    default_cron_expression = CronExpression('0 * * * *')
    level = 2

    def __init__(self, timeshift: Optional[datetime.timedelta] = None):
        self.timeshift = timeshift or self.default_timeshift

    def _af_repr_impl(self, full_days_shift: int, full_hours_shift: int, rest_minutes_shift: int):
        """
        base cron expression: 0 * * * * (or @hourly)
        """

        if full_days_shift or full_hours_shift:
            raise ValueError(f'Hourly schedule tag supports only shifts between 0 and 59 minutes, got {self.timeshift}')

        return f'{rest_minutes_shift} * * * *'


class _Every15MinutesScheduleTag(BaseScheduleTag):
    name = '@every15minutes'
    default_cron_expression = CronExpression('*/15 * * * *')
    level = 1

    def __init__(self, timeshift: Optional[datetime.timedelta] = None):
        self.timeshift = timeshift or self.default_timeshift

    def _af_repr_impl(self, full_days_shift: int, full_hours_shift: int, rest_minutes_shift: int):
        """
        base cron expression: */15 * * * * (or @15minutes)
        """

        if full_days_shift or full_hours_shift or rest_minutes_shift > 59:
            raise ValueError(
                f'Every 15 minutes schedule tag supports only shifts between 0 and 59 minutes, got {self.timeshift}'
            )
        return f'{rest_minutes_shift}-59/15 * * * *'


class _ManualScheduleTag(BaseScheduleTag):
    name = '@manual'
    default_cron_expression = None
    level = 0

    def __init__(self, timeshift: Optional[datetime.timedelta] = None):
        self.timeshift = None

    @staticmethod
    def _af_repr_impl(full_days_shift: int, full_hours_shift: int, rest_minutes_shift: int):
        return None

    def af_repr(self):
        # there's no schedule for manual DAGs
        return None


class ScheduleTag(Enum):
    """
    Schedule tags for dbt models.
    Only these tags are supported.

    Enum's key must be equal to the ScheduleTag name.
    """

    monthly = _MonthlyScheduleTag
    weekly = _WeeklyScheduleTag
    daily = _DailyScheduleTag
    hourly = _HourlyScheduleTag
    manual = _ManualScheduleTag
    every15minutes = _Every15MinutesScheduleTag

    @property
    def name(self):
        return self.value.name

    @property
    def level(self):
        return self.value.level

    @property
    def default_cron_expression(self):
        return self.value.default_cron_expression

    def __hash__(self):
        return hash(self.name)

    def __str__(self):
        return self.value.name

    def __lt__(self, other):
        if not isinstance(other, ScheduleTag):
            return NotImplemented
        return self.value.level < other.value.level

    def __eq__(self, other):
        if isinstance(other, ScheduleTag):
            return self.value.name == other.value.name
        if isinstance(other, str):
            return self.value.name == other
        raise TypeError(f'Cannot compare {self} with {other}')

    def __call__(self, *args, **kwargs):
        return self.value(*args, **kwargs)

import datetime
import re
from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Optional, Tuple, Union

from attrs import define


@define
class CronExpression:
    """
    Represents cron expression as a shift from the base expression
    """

    raw_cron_expression: str

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
    name: Optional[str] = None
    default_cron_expression: Optional[CronExpression] = None
    default_timeshift = datetime.timedelta()

    def __str__(self):
        return self.name

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other: Union[str, 'BaseScheduleTag']):
        if isinstance(other, str):
            return self.name == other
        if isinstance(other, BaseScheduleTag):
            return self.name == other.name and self.timeshift == other.timeshift
        raise TypeError(f'Cannot compare {self} with {other}')

    @property
    def safe_name(self):
        return re.sub(r'^@', 'dbt_', self.name)

    @abstractmethod
    def __init__(self, timeshift: Optional[datetime.timedelta] = None):
        raise NotImplementedError()
        self.timeshift = timeshift  # noqa (for IDE)

    @staticmethod
    @abstractmethod
    def _af_repr_impl(full_days_shift: int, full_hours_shift: int, rest_minutes_shift: int):
        raise NotImplementedError()

    def cron_expression(self) -> Union[str, CronExpression]:
        af_schedule = self.af_repr()
        if af_schedule.startswith('@'):
            return self.default_cron_expression
        return CronExpression(af_schedule)

    def af_repr(self) -> str:
        if not self.timeshift:
            return self.name
        full_days_shift, full_hours_shift, rest_minutes_shift = self.split_timeshift()
        return self._af_repr_impl(full_days_shift, full_hours_shift, rest_minutes_shift)

    def split_timeshift(self) -> Tuple[int, int, int]:
        full_days_shift = self.timeshift.days
        full_hours_shift = self.timeshift.seconds // 3600
        rest_minutes_shift = (self.timeshift.seconds % 3600) // 60

        return full_days_shift, full_hours_shift, rest_minutes_shift


class _MonthlyScheduleTag(BaseScheduleTag):
    name = '@monthly'
    default_cron_expression = CronExpression('0 0 1 * *')

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
            full_days_shift = '*'

        return f'{rest_minutes_shift} {full_hours_shift} {full_days_shift} * *'


class _WeeklyScheduleTag(BaseScheduleTag):
    name = '@weekly'
    default_cron_expression = CronExpression('0 0 * * 0')

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

    def __init__(self, timeshift: Optional[datetime.timedelta] = None):
        self.timeshift = timeshift or self.default_timeshift

    def _af_repr_impl(self, full_days_shift: int, full_hours_shift: int, rest_minutes_shift: int):
        """
        base cron expression: 0 * * * * (or @hourly)
        """

        if full_days_shift or full_hours_shift:
            raise ValueError(f'Hourly schedule tag supports only shifts between 0 and 59 minutes, got {self.timeshift}')

        return f'{rest_minutes_shift} * * * *'


class _ManualScheduleTag(BaseScheduleTag):
    name = '@manual'

    def __init__(self, timeshift: Optional[datetime.timedelta] = None):
        self.timeshift = None

    @staticmethod
    def _af_repr_impl(full_days_shift: int, full_hours_shift: int, rest_minutes_shift: int):
        return None

    def af_repr(self):
        # there's no schedule for manual DAGs
        return None


class ScheduleTag(Enum):
    """DBT tag that specifies an Airflow model launch frequency"""

    monthly = _MonthlyScheduleTag
    weekly = _WeeklyScheduleTag
    daily = _DailyScheduleTag
    hourly = _HourlyScheduleTag
    manual = _ManualScheduleTag

    @property
    def name(self):
        return self.value.name

    @property
    def default_cron_expression(self):
        return self.value.default_cron_expression

    def __hash__(self):
        return hash(self.name)

    def __str__(self):
        return self.value.name

    def __eq__(self, other):
        if isinstance(other, ScheduleTag):
            return self.value.name == other.value.name
        if isinstance(other, str):
            return self.value.name == other
        raise TypeError(f'Cannot compare {self} with {other}')

    def __call__(self, *args, **kwargs):
        return self.value(*args, **kwargs)

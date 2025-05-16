import datetime
import re
from abc import ABC, abstractmethod
from enum import Enum
from functools import cache

from dbt_af.common.cron import CronExpression


class BaseScheduleTag(ABC):
    default_timeshift = datetime.timedelta()

    def __init__(self, timeshift: datetime.timedelta | None = None):
        if timeshift is not None:
            if self.default_cron_expression is None:
                raise ValueError('Cannot set timeshift for manual schedule tag')

            # if timeshift is greater than max timeshift, set it to max timeshift - 1 minute
            max_timeshift = self.default_cron_expression.distance_between_two_runs()
            timeshift = min(timeshift, max_timeshift - datetime.timedelta(minutes=1))
        self.timeshift = timeshift or self.default_timeshift

    @property
    @abstractmethod
    def base_name(self) -> str:
        raise NotImplementedError()

    @property
    def name(self) -> str:
        if self.timeshift == self.default_timeshift or self.timeshift is None:
            return self.base_name

        full_days_shift, full_hours_shift, rest_minutes_shift = self.split_timeshift()
        shifts = [
            ('day', full_days_shift),
            ('hour', full_hours_shift),
            ('minute', rest_minutes_shift),
        ]
        shift_parts = [f'{value}_{unit}s' for unit, value in shifts if value > 0]
        if shift_parts:
            return f'{self.base_name}_shift_{"_".join(shift_parts)}'

        return self.base_name

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
        if self.level == other.level:
            return self.timeshift < other.timeshift
        return self.level < other.level

    def __eq__(self, other: 'str | BaseScheduleTag'):
        if isinstance(other, str):
            return self.base_name == other
        if isinstance(other, BaseScheduleTag):
            return self.base_name == other.base_name
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
    base_name = '@monthly'
    default_cron_expression = CronExpression('0 0 1 * *')
    level = 5

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
    base_name = '@weekly'
    default_cron_expression = CronExpression('0 0 * * 0')
    level = 4

    def _af_repr_impl(self, full_days_shift: int, full_hours_shift: int, rest_minutes_shift: int):
        """
        base cron expression: 0 0 * * 0 (or @weekly)
        """

        if full_days_shift > 6:
            raise ValueError(f'Weekly schedule tag supports only shifts between 0 and 6 days, got {self.timeshift}')

        return f'{rest_minutes_shift} {full_hours_shift} * * {full_days_shift}'


class _DailyScheduleTag(BaseScheduleTag):
    base_name = '@daily'
    default_cron_expression = CronExpression('0 0 * * *')
    level = 3

    def _af_repr_impl(self, full_days_shift: int, full_hours_shift: int, rest_minutes_shift: int):
        """
        base cron expression: 0 0 * * * (or @daily)
        """

        if full_days_shift:
            raise ValueError(f'Daily schedule tag supports only shifts between 0 and 23 hours, got {self.timeshift}')

        return f'{rest_minutes_shift} {full_hours_shift} * * *'


class _HourlyScheduleTag(BaseScheduleTag):
    base_name = '@hourly'
    default_cron_expression = CronExpression('0 * * * *')
    level = 2

    def _af_repr_impl(self, full_days_shift: int, full_hours_shift: int, rest_minutes_shift: int):
        """
        base cron expression: 0 * * * * (or @hourly)
        """

        if full_days_shift or full_hours_shift:
            raise ValueError(f'Hourly schedule tag supports only shifts between 0 and 59 minutes, got {self.timeshift}')

        return f'{rest_minutes_shift} * * * *'


class _Every15MinutesScheduleTag(BaseScheduleTag):
    base_name = '@every15minutes'
    default_cron_expression = CronExpression('*/15 * * * *')
    level = 1

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
    base_name = '@manual'
    default_cron_expression = None
    level = 0

    def __init__(self, timeshift: datetime.timedelta | None = None):
        super().__init__(timeshift)
        self.timeshift = None

    @staticmethod
    def _af_repr_impl(full_days_shift: int, full_hours_shift: int, rest_minutes_shift: int):
        return None

    def af_repr(self):
        # there's no schedule for manual DAGs
        return None


class EScheduleTag(Enum):
    """
    Schedule tags for dbt models.
    Only these tags are supported.

    Enum's key must be equal to the EScheduleTag name.
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
    def base_name(self):
        return self.value.base_name

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
        if not isinstance(other, EScheduleTag):
            return NotImplemented
        return self.value.level < other.value.level

    def __eq__(self, other):
        if isinstance(other, EScheduleTag):
            return self.value.name == other.value.name
        if isinstance(other, str):
            return self.value.name == other
        raise TypeError(f'Cannot compare {self} with {other}')

    def __call__(self, *args, **kwargs):
        return self.value(*args, **kwargs)

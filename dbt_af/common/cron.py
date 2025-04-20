import datetime

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

    def distance_between_two_runs(self) -> datetime.timedelta:
        """
        Calculate the time difference between two consecutive runs of the cron expression.
        """
        now = datetime.datetime.now()
        cron_iter = croniter(self.raw_cron_expression, now)
        next_run = cron_iter.get_next(datetime.datetime)
        after_next_run = cron_iter.get_next(datetime.datetime)
        return after_next_run - next_run

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

    def _split_cron_expression(self) -> list[str]:
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

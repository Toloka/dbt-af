from collections import defaultdict
from enum import Enum
from typing import Optional

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

from dbt_af.builder.task_dependencies import RegistryDomainDependencies
from dbt_af.common import constants
from dbt_af.common.scheduling import BaseScheduleTag, ScheduleTag
from dbt_af.conf import Config


class DomainDagType(Enum):
    SCHEDULED = 'scheduled'
    BACKFILL = 'backfill'
    MAINTENANCE = 'maintenance'
    LARGE_TESTS = 'large_tests'


class DomainDag:
    def __init__(
        self,
        domain_name: str,
        schedule: BaseScheduleTag = None,
        config: Optional[Config] = None,
        additional_tags: Optional[list[str]] = None,
        catchup: bool = True,
    ):
        self.domain_name = domain_name
        self._schedule = schedule or ScheduleTag.daily()
        self.config: Config = config or Config()

        self.additional_tags: list[str] = additional_tags or []
        self.catchup = catchup

        self.registered_domains_dependencies: dict[DomainDag, RegistryDomainDependencies] = defaultdict(
            RegistryDomainDependencies
        )

        self.af_dag = None

    @property
    def _base_tags(self) -> Optional[list[str]]:
        return None

    @property
    def dag_name(self) -> str:
        return f'{self.domain_name}{self._schedule}'.replace('@', '__')

    @property
    def schedule(self) -> BaseScheduleTag:
        return self._schedule

    @schedule.setter
    def schedule(self, value: BaseScheduleTag):
        self._schedule = value

    @property
    def tags(self) -> list[str]:
        pure_domain_name = self.domain_name.split('__')[0]
        merged_tags = (self._base_tags or []) + [pure_domain_name, self.schedule.safe_name] + self.additional_tags
        if self.schedule != ScheduleTag.manual() and constants.BACKFILL_TAG not in merged_tags:
            merged_tags.append(constants.FRONTIER_TAG)

        return merged_tags

    def __hash__(self) -> int:
        return hash(self.dag_name)

    def __eq__(self, other) -> bool:
        if isinstance(other, DomainDag):
            return self.dag_name == other.dag_name
        raise TypeError(f'Cannot compare {self.__class__.__name__} with {other.__class__.__name__}')

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.domain_name}, {self._schedule})'


class BackfillDomainDag(DomainDag):
    def __init__(
        self,
        domain_name: str,
        config: Optional[Config] = None,
        additional_tags: list[str] = None,
        catchup: bool = True,
    ):
        super().__init__(domain_name, self.schedule, config, additional_tags, catchup)

        self.start_endpoint: Optional[EmptyOperator] = None

    @property
    def _base_tags(self) -> Optional[list[str]]:
        return [constants.BACKFILL_TAG]

    @property
    def dag_name(self) -> str:
        return f'{self.domain_name}__backfill'.replace('@', '__')

    @property
    def schedule(self) -> BaseScheduleTag:
        return ScheduleTag.daily()

    def wrap_dag_with_endpoints(self):
        """
        Each backfill dag should have start with branch operator and end with empty operator.
        For the first scheduled run branch operator will return 'do_nothing' task and all dbt tasks will be skipped.
        If airflow dag is triggered again after scheduled run, it will trigger all downstream dbt tasks.
        """
        self.start_endpoint = EmptyOperator(task_id='start_work', dag=self.af_dag)
        do_nothing = EmptyOperator(task_id='do_nothing', dag=self.af_dag)

        def decide_which_path(**kwargs):
            if kwargs['task_instance'].try_number > 1:
                return 'start_work'
            return 'do_nothing'

        brancher = BranchPythonOperator(task_id='branch', python_callable=decide_which_path, dag=self.af_dag)
        brancher >> [self.start_endpoint, do_nothing]


class MaintenanceDomainDag(DomainDag):
    def __init__(
        self,
        domain_name: str,
        config: Optional[Config] = None,
        additional_tags: list[str] = None,
        catchup: bool = False,
    ):
        super().__init__(domain_name, self.schedule, config, additional_tags, catchup)

    @property
    def _base_tags(self) -> Optional[list[str]]:
        return [constants.MAINTENANCE_TAG]

    @property
    def dag_name(self) -> str:
        return f'{self.domain_name}__maintenance'.replace('@', '__')

    @property
    def schedule(self) -> BaseScheduleTag:
        return ScheduleTag.daily()


class LargeTestsDomainDag(DomainDag):
    def __init__(
        self,
        domain_name: str,
        config: Optional[Config] = None,
        additional_tags: list[str] = None,
        catchup: bool = False,
    ):
        super().__init__(domain_name, self.schedule, config, additional_tags, catchup)

    @property
    def _base_tags(self) -> Optional[list[str]]:
        return [constants.LARGE_TESTS_TAG]

    @property
    def dag_name(self) -> str:
        return f'{self.domain_name}__large_tests{self.schedule}'.replace('@', '__')

    @property
    def schedule(self) -> BaseScheduleTag:
        return ScheduleTag.daily()


class DomainDagFactory:
    @staticmethod
    def create(dag_type: DomainDagType, domain_name: str, schedule: BaseScheduleTag, config: Config) -> DomainDag:
        if dag_type == DomainDagType.SCHEDULED:
            return DomainDag(domain_name, schedule, config)
        if dag_type == DomainDagType.BACKFILL:
            return BackfillDomainDag(domain_name, config)
        if dag_type == DomainDagType.MAINTENANCE:
            return MaintenanceDomainDag(domain_name, config)
        if dag_type == DomainDagType.LARGE_TESTS:
            return LargeTestsDomainDag(domain_name, config)

        raise TypeError(f'Unknown dag type: {dag_type}')

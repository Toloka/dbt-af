import enum
from collections import defaultdict
from pathlib import Path
from typing import Any, DefaultDict, Dict, List, Optional, Union

import pendulum

try:
    import pydantic.v1 as pydantic
except ModuleNotFoundError:
    import pydantic

from dbt_af.common.constants import DOMAIN_DAG_START_DATE_FMT
from dbt_af.common.scheduling import BaseScheduleTag, ScheduleTag
from dbt_af.common.utils import TestTag
from dbt_af.conf import DbtDefaultTargetsConfig
from dbt_af.parser.dbt_profiles import KubernetesTarget, Profile, Target


class DbtModelMaintenanceType(enum.Enum):
    PERSIST_DOCS = 'persist_docs'
    OPTIMIZE_TABLES = 'optimize_table'
    VACUUM_TABLE = 'vacuum_table'
    DEDUPLICATE_TABLE = 'deduplicate_table'
    SET_TTL_ON_TABLE = 'set_ttl_on_table'


class WaitPolicy(enum.Enum):
    last = 'last'
    all = 'all'


class DependencyConfig(pydantic.BaseModel):
    skip: bool = pydantic.Field(default=False)
    wait_policy: WaitPolicy = WaitPolicy.last


class TTLConfig(pydantic.BaseModel):
    key: str = pydantic.Field(description='Table timestamp-like column name')
    expiration_timeout: int = pydantic.Field(description='Table expiration timeout in days')
    additional_predicate: Optional[str] = pydantic.Field(
        description='Additional predicate for filtering expired data',
        default='',
    )
    force_predicate: Optional[str] = pydantic.Field(
        description='Force predicate for filtering expired data. It will override base predicate',
        default='',
    )

    @pydantic.root_validator(pre=True)
    def validate_ttl_config(cls, values: dict[str, Any]) -> dict[str, Any]:
        # if force_predicate is set, then key and expiration_timeout are not required
        if values.get('force_predicate'):
            values['key'] = values.get('key', '')
            values['expiration_timeout'] = values.get('expiration_timeout', 0)

        return values


class DbtAFMaintenanceConfig(pydantic.BaseModel):
    ttl: Optional[TTLConfig] = pydantic.Field(default=None)
    persist_docs: Optional[bool] = pydantic.Field(default=False)
    optimize_table: Optional[bool] = pydantic.Field(default=False)
    vacuum_table: Optional[bool] = pydantic.Field(default=False)
    deduplicate_table: Optional[bool] = pydantic.Field(default=False)

    def get_required_maintenance_types(self) -> list[DbtModelMaintenanceType]:
        maintenance_types = []
        if self.ttl is not None:
            maintenance_types.append(DbtModelMaintenanceType.SET_TTL_ON_TABLE)
        if self.persist_docs:
            maintenance_types.append(DbtModelMaintenanceType.PERSIST_DOCS)
        if self.optimize_table:
            maintenance_types.append(DbtModelMaintenanceType.OPTIMIZE_TABLES)
        if self.vacuum_table:
            maintenance_types.append(DbtModelMaintenanceType.VACUUM_TABLE)
        if self.deduplicate_table:
            maintenance_types.append(DbtModelMaintenanceType.DEDUPLICATE_TABLE)

        return maintenance_types


class TableauRefreshResourceType(str, enum.Enum):
    workbook = 'workbook'
    datasource = 'datasource'


class TableauRefreshTaskConfig(pydantic.BaseModel):
    resource_name: str
    project_name: str
    resource_type: TableauRefreshResourceType


class DbtNodeConfig(pydantic.BaseModel):
    enabled: bool
    alias: Optional[str]
    node_schema: Optional[str] = pydantic.Field(..., alias='schema')
    database: Optional[str]
    tags: List[str]
    meta: Dict[str, Any]
    group: Optional[str]
    materialized: Optional[str]
    incremental_strategy: Optional[str]
    persist_docs: Optional[Dict[str, Any]]
    quoting: Optional[Dict[str, Any]]
    column_types: Optional[Dict[str, Any]]
    full_refresh: Optional[bool]
    unique_key: Optional[Union[str, List[str]]]
    on_schema_change: Optional[str]
    grants: Optional[Dict]
    packages: Optional[List]
    docs: Optional[Dict[str, Any]]
    contract: Optional[Dict[str, Any]]
    file_format: Optional[str]
    tblproperties: Optional[Dict[str, Any]]
    partition_by: Optional[Union[str, List[str]]]
    incremental_predicates: Optional[List[str]]
    post_hook: Optional[List[Dict[str, Any]]] = pydantic.Field(alias='post-hook', default_factory=list)
    pre_hook: Optional[List[Dict[str, Any]]] = pydantic.Field(alias='pre-hook', default_factory=list)

    schedule: Optional[BaseScheduleTag] = pydantic.Field(default_factory=ScheduleTag.daily)

    dependencies: Optional[DefaultDict[str, DependencyConfig]] = pydantic.Field(
        default_factory=lambda: defaultdict(DependencyConfig)
    )

    py_cluster: Optional[str]
    sql_cluster: Optional[str]
    daily_sql_cluster: Optional[str]
    bf_cluster: Optional[str]

    enable_from_dttm: Optional[str] = pydantic.Field(default='')
    disable_from_dttm: Optional[str] = pydantic.Field(default='')

    airflow_parallelism: int = pydantic.Field(default=1)
    domain_start_date: Optional[str] = pydantic.Field(default='')

    dbt_target: Optional[str] = pydantic.Field(default='')

    maintenance: DbtAFMaintenanceConfig = pydantic.Field(default_factory=DbtAFMaintenanceConfig)

    tableau_refresh_tasks: Optional[List[TableauRefreshTaskConfig]] = pydantic.Field(default_factory=list)

    class Config:
        arbitrary_types_allowed = True

    @pydantic.root_validator(pre=True)
    def validate_clusters(cls, values):
        if values['materialized'] not in ('test', 'seed'):
            if not all(
                [
                    values['py_cluster'],
                    values['sql_cluster'],
                    values['daily_sql_cluster'],
                    values['bf_cluster'],
                ]
            ):
                raise ValueError('py_cluster, sql_cluster and daily_sql_cluster must be set')
        return values

    @pydantic.root_validator(pre=True)
    def validate_airflow_parallelism(cls, values):
        if 'airflow_parallelism' in values and isinstance(values['airflow_parallelism'], str):
            values['airflow_parallelism'] = int(values['airflow_parallelism'])
        return values

    @pydantic.validator('schedule', pre=True)
    def validate_schedule(cls, v):
        if v not in [item.value().name for item in ScheduleTag]:
            return ScheduleTag.daily()
        return ScheduleTag[v.lstrip('@')]()

    @pydantic.validator('domain_start_date', pre=True)
    def validate_domain_start_date(cls, v):
        if v == '':
            return v
        pendulum.from_format(v, DOMAIN_DAG_START_DATE_FMT)
        return v


class DbtNodeColumn(pydantic.BaseModel):
    name: str
    description: Optional[str]
    meta: Optional[Dict[str, Any]]
    data_type: Optional[str]
    constraints: Optional[List]
    quote: Optional[bool]
    tags: Optional[List[str]]


class DbtNode(pydantic.BaseModel):
    database: str
    node_schema: str = pydantic.Field(..., alias='schema')
    name: str
    resource_type: str
    package_name: str
    path: str
    original_file_path: str
    unique_id: str
    fqn: List[str]
    alias: Optional[str]
    checksum: Dict[str, str]
    config: DbtNodeConfig
    tags: List[str]
    description: Optional[str]
    columns: Optional[Dict[str, DbtNodeColumn]]
    meta: Optional[Dict[str, Any]]
    group: Optional[str]
    docs: Optional[Dict[str, Any]]
    patch_path: Optional[str]
    build_path: Optional[str]
    deferred: Optional[bool]
    unrendered_config: Optional[Dict[str, Any]]
    created_at: float
    relation_name: Optional[str]
    raw_code: str
    language: Optional[str]
    refs: Optional[List[Dict[str, Any]]]
    sources: Optional[List[Any]] = pydantic.Field(default_factory=list)
    metrics: Optional[List[Dict[str, Any]]] = pydantic.Field(default_factory=list)
    node_depends_on: Optional[Dict[str, Any]] = pydantic.Field(alias='depends_on', default_factory=dict)
    compiled_path: Optional[str]
    contract: Optional[Dict[str, Any]] = pydantic.Field(default_factory=dict)
    access: Optional[str]
    constraints: Optional[List[Dict[str, Any]]] = pydantic.Field(default_factory=list)
    version: Optional[str]
    latest_version: Optional[str]

    target_details: Optional[Target | KubernetesTarget] = pydantic.Field(default=None)

    def __hash__(self):
        return hash(self.unique_id)

    def __eq__(self, other):
        if isinstance(other, DbtNode):
            return self.unique_id == other.unique_id
        if isinstance(other, str):
            return self.unique_id == other
        raise TypeError(f'Cannot compare {self.__class__.__name__} with {other.__class__.__name__}')

    @property
    def domain(self):
        """Gets domain name from a dbt resource fqn

        The function assumes that the layout of project looks something like:
            `[<project>, models, <domain>, <layer>, <model>, <model>]`

        This results in the fqn that looks like:
            `[<project>, <domain>, <layer>, <model>, <model>]` for models
            `[<project>, dbt, models, <domain>, <layer>, <model>, <model>]` for tests
        """
        if self.is_test():
            return self.fqn[3]
        return self.fqn[1]

    @property
    def resource_name(self):
        """Gets resource name from dbt's node_id

        test.dtt.not_null_task_w_test__ods__one_id.1417bf4cb1
        model.dtt.task_w_test__ods__one

        """
        if self.unique_id.startswith('test.'):
            return self.unique_id.split('.', maxsplit=2)[2].rsplit('.', maxsplit=1)[0]

        return self.unique_id.split('.', maxsplit=2)[2]

    @property
    def depends_on(self):
        return [
            dep for dep in self.node_depends_on['nodes'] if dep.startswith(('test.', 'model.', 'snapshot.', 'seed.'))
        ]

    @property
    def depends_on_sources(self):
        return [dep for dep in self.node_depends_on['nodes'] if dep.startswith('source.')]

    @property
    def materialized(self):
        return self.config.materialized if self.resource_type == 'model' else ''

    @property
    def test_type(self):
        # TODO refactor this
        if not self.is_test():
            return None

        test_tags = {f.value for f in TestTag} & set(self.tags)
        if len(test_tags) > 1:
            raise ValueError(f'Several test tags for {self.unique_id} are not allowed')
        if not test_tags:
            return TestTag.small.value

        return test_tags.pop()

    @property
    def model_type(self):
        if self.is_py_model():
            return 'py'
        if self.is_sql_model():
            return 'sql'
        return None

    def target_environment(self, default_dbt_targets: DbtDefaultTargetsConfig) -> str:
        if self.config.dbt_target:
            return self.config.dbt_target

        if self.is_test():
            return default_dbt_targets.default_for_tests_target

        if len(self.config.pre_hook) == 0 and self.model_type == 'sql':
            if self.config.schedule in (ScheduleTag.daily(), ScheduleTag.weekly()):
                return self.config.daily_sql_cluster

            return self.config.sql_cluster

        return self.config.py_cluster

    @property
    def original_file_path_dirname(self) -> str:
        return str(Path(self.original_file_path).parent)

    @property
    def original_file_path_without_extension(self):
        return str(Path(self.original_file_path).with_suffix(''))

    def is_model(self) -> bool:
        return self.resource_type == 'model'

    def is_view(self) -> bool:
        return self.materialized == 'view'

    def is_snapshot(self) -> bool:
        return self.resource_type == 'snapshot'

    def is_test(self) -> bool:
        return self.resource_type == 'test'

    def is_small_test(self) -> bool:
        return self.resource_type == 'test' and self.test_type == TestTag.small.value

    def is_medium_test(self) -> bool:
        return self.resource_type == 'test' and self.test_type == TestTag.medium.value

    def is_large_test(self) -> bool:
        return self.resource_type == 'test' and self.test_type == TestTag.large.value

    def is_seed(self) -> bool:
        return self.resource_type == 'seed'

    def is_sql_model(self) -> bool:
        return self.path.endswith('.sql')

    def is_py_model(self) -> bool:
        return self.path.endswith('.py')

    def is_at_etl_service(self, etl_service_name) -> bool:
        return etl_service_name in self.original_file_path

    def has_dt_partition(self) -> bool:
        # TODO: refactor this
        whitelisted_dt_partitions = {'timestamp'}

        if self.config.partition_by is None:
            return False
        if isinstance(self.config.partition_by, str):
            return '_dt' in self.config.partition_by or self.config.partition_by in whitelisted_dt_partitions
        if isinstance(self.config.partition_by, list):
            return '_dt' in ''.join(self.config.partition_by) or not whitelisted_dt_partitions.isdisjoint(
                self.config.partition_by
            )

        return False

    def get_airflow_parallelism(self) -> int:
        if self.materialized != 'incremental' or not self.has_dt_partition():
            return 1

        return self.config.airflow_parallelism

    def get_required_maintenance_types(self) -> list[DbtModelMaintenanceType]:
        return self.config.maintenance.get_required_maintenance_types()

    def set_target_details(self, profile: Profile, default_dbt_targets: DbtDefaultTargetsConfig):
        self.target_details = profile.outputs[self.target_environment(default_dbt_targets=default_dbt_targets)]

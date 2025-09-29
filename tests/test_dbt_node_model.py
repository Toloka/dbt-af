import datetime as dt
from unittest.mock import Mock

import pytest

from dbt_af.common.scheduling import EScheduleTag
from dbt_af.common.utils import TestTag
from dbt_af.conf import DbtDefaultTargetsConfig
from dbt_af.parser.dbt_node_model import (
    DbtAFMaintenanceConfig,
    DbtModelMaintenanceType,
    DbtNode,
    DbtNodeColumn,
    DbtNodeConfig,
    DependencyConfig,
    TableauRefreshResourceType,
    TableauRefreshTaskConfig,
    TTLConfig,
    WaitPolicy,
)
from dbt_af.parser.dbt_profiles import Profile, VenvTarget


@pytest.fixture
def dependency_config():
    return DependencyConfig(skip=True, wait_policy=WaitPolicy.all)


@pytest.fixture
def ttl_config():
    return TTLConfig(key='created_at', expiration_timeout=30, additional_predicate="status = 'archived'")


@pytest.fixture
def ttl_config_with_force():
    return TTLConfig(force_predicate='created_at < current_date() - 90')


@pytest.fixture
def maintenance_config():
    return DbtAFMaintenanceConfig(
        ttl=TTLConfig(key='dt', expiration_timeout=7),
        persist_docs=True,
        optimize_table=True,
        vacuum_table=False,
        deduplicate_table=True,
    )


@pytest.fixture
def tableau_refresh_task():
    return TableauRefreshTaskConfig(
        resource_name='sales_dashboard', project_name='analytics', resource_type=TableauRefreshResourceType.workbook
    )


@pytest.fixture
def node_config_data():
    return {
        'enabled': True,
        'alias': 'my_table_alias',
        'schema': 'analytics',
        'database': 'prod_db',
        'tags': ['daily', 'critical'],
        'meta': {'owner': 'data_team'},
        'group': 'core',
        'materialized': 'table',
        'incremental_strategy': 'merge',
        'persist_docs': {'relation': True, 'columns': True},
        'quoting': {'database': False, 'schema': False},
        'column_types': {'id': 'bigint'},
        'full_refresh': False,
        'unique_key': 'id',
        'on_schema_change': 'fail',
        'grants': {'select': ['analyst']},
        'packages': ['utils'],
        'docs': {'show': True},
        'contract': {'enforced': False},
        'file_format': 'parquet',
        'tblproperties': {'delta.autoOptimize.optimizeWrite': 'true'},
        'partition_by': 'dt',
        'incremental_predicates': ['dt >= current_date() - 7'],
        'post-hook': [{'sql': 'ANALYZE TABLE {{ this }}'}],
        'pre-hook': [{'sql': 'DROP TABLE IF EXISTS {{ this }}_temp'}],
        'schedule': '@daily',
        'schedule_shift': 30,
        'schedule_shift_unit': 'minute',
        'dependencies': {},
        'env': {'DBT_ENV': 'production'},
        'py_cluster': 'cluster1',
        'sql_cluster': 'cluster2',
        'daily_sql_cluster': 'cluster3',
        'bf_cluster': 'cluster4',
        'enable_from_dttm': '2024-01-01 00:00:00',
        'disable_from_dttm': '2025-01-01 00:00:00',
        'airflow_parallelism': 4,
        'domain_start_date': '2024-01-01T00:00:00',
        'dbt_target': 'prod',
        'maintenance': {'ttl': {'key': 'dt', 'expiration_timeout': 30}},
        'tableau_refresh_tasks': [{'resource_name': 'dashboard', 'project_name': 'main', 'resource_type': 'workbook'}],
    }


@pytest.fixture
def node_column():
    return DbtNodeColumn(
        name='user_id',
        description='Unique user identifier',
        meta={'pii': True},
        data_type='bigint',
        constraints=['not_null', 'unique'],
        quote=False,
        tags=['primary_key'],
    )


@pytest.fixture
def dbt_node_data():
    return {
        'database': 'analytics',
        'schema': 'marts',
        'name': 'fact_orders',
        'resource_type': 'model',
        'package_name': 'my_project',
        'path': 'models/marts/fact_orders.sql',
        'original_file_path': 'models/domain/marts/fact_orders.sql',
        'unique_id': 'model.my_project.fact_orders',
        'fqn': ['my_project', 'domain', 'marts', 'fact_orders'],
        'alias': 'fact_orders_v2',
        'checksum': {'name': 'sha256', 'checksum': 'abc123'},
        'config': {
            'enabled': True,
            'schema': 'marts',
            'database': 'analytics',
            'tags': ['daily'],
            'meta': {},
            'materialized': 'incremental',
            'py_cluster': 'cluster1',
            'sql_cluster': 'cluster2',
            'daily_sql_cluster': 'cluster3',
            'bf_cluster': 'cluster4',
        },
        'tags': ['critical'],
        'description': 'Orders fact table',
        'columns': {'order_id': {'name': 'order_id', 'description': 'Order ID', 'data_type': 'bigint'}},
        'meta': {'refresh_frequency': 'daily'},
        'group': 'core',
        'docs': {'show': True},
        'patch_path': 'models/schema.yml',
        'build_path': 'target/models/fact_orders.sql',
        'deferred': False,
        'unrendered_config': {},
        'created_at': 1234567890.0,
        'relation_name': 'analytics.marts.fact_orders',
        'raw_code': 'SELECT * FROM staging.orders',
        'language': 'sql',
        'refs': [{'name': 'stg_orders', 'package': None, 'version': None}],
        'sources': [],
        'metrics': [],
        'depends_on': {'nodes': ['model.my_project.stg_orders', 'test.my_project.test_orders'], 'macros': []},
        'compiled_path': 'target/compiled/models/fact_orders.sql',
        'contract': {'enforced': False},
        'access': 'protected',
        'constraints': [],
        'version': '1',
        'latest_version': '2',
    }


@pytest.fixture
def dbt_node(dbt_node_data):
    return DbtNode(**dbt_node_data)


@pytest.fixture
def dbt_test_node_data(dbt_node_data):
    test_data = dbt_node_data.copy()
    test_data.update(
        {
            'resource_type': 'test',
            'unique_id': 'test.my_project.not_null_fact_orders_order_id',
            'fqn': ['my_project', 'dbt', 'models', 'domain', 'marts', 'test_fact_orders'],
            'tags': ['small'],
            'config': {
                'enabled': True,
                'schema': 'marts',
                'database': 'analytics',
                'tags': ['small'],
                'meta': {},
                'materialized': 'test',
                'py_cluster': 'cluster1',
                'sql_cluster': 'cluster2',
                'daily_sql_cluster': 'cluster3',
                'bf_cluster': 'cluster4',
            },
        }
    )
    return test_data


@pytest.fixture
def default_dbt_targets():
    config = Mock(spec=DbtDefaultTargetsConfig)
    config.default_for_tests_target = 'test_target'
    return config


@pytest.fixture
def profile_mock():
    profile = Mock(spec=Profile)
    profile.outputs = {
        'cluster1': VenvTarget(type='venv', system_site_packages=False),
        'cluster2': VenvTarget(type='venv', system_site_packages=False),
        'cluster3': VenvTarget(type='venv', system_site_packages=False),
        'test_target': VenvTarget(type='venv', system_site_packages=False),
    }
    return profile


def test_wait_policy_enum():
    assert WaitPolicy.last.value == 'last'
    assert WaitPolicy.all.value == 'all'


def test_dependency_config(dependency_config):
    assert dependency_config.skip is True
    assert dependency_config.wait_policy == WaitPolicy.all


def test_dependency_config_defaults():
    config = DependencyConfig()
    assert config.skip is False
    assert config.wait_policy == WaitPolicy.last


def test_ttl_config(ttl_config):
    assert ttl_config.key == 'created_at'
    assert ttl_config.expiration_timeout == 30
    assert ttl_config.additional_predicate == "status = 'archived'"
    assert ttl_config.force_predicate == ''


def test_ttl_config_with_force_predicate(ttl_config_with_force):
    assert ttl_config_with_force.force_predicate == 'created_at < current_date() - 90'
    assert ttl_config_with_force.key == ''
    assert ttl_config_with_force.expiration_timeout == 0


def test_maintenance_config(maintenance_config):
    assert maintenance_config.ttl.key == 'dt'
    assert maintenance_config.persist_docs is True
    assert maintenance_config.optimize_table is True
    assert maintenance_config.vacuum_table is False
    assert maintenance_config.deduplicate_table is True


def test_maintenance_config_get_required_types(maintenance_config):
    types = maintenance_config.get_required_maintenance_types()
    assert DbtModelMaintenanceType.SET_TTL_ON_TABLE in types
    assert DbtModelMaintenanceType.PERSIST_DOCS in types
    assert DbtModelMaintenanceType.OPTIMIZE_TABLES in types
    assert DbtModelMaintenanceType.DEDUPLICATE_TABLE in types
    assert DbtModelMaintenanceType.VACUUM_TABLE not in types


def test_tableau_refresh_task(tableau_refresh_task):
    assert tableau_refresh_task.resource_name == 'sales_dashboard'
    assert tableau_refresh_task.project_name == 'analytics'
    assert tableau_refresh_task.resource_type == TableauRefreshResourceType.workbook


def test_node_config_creation(node_config_data):
    config = DbtNodeConfig(**node_config_data)
    assert config.enabled is True
    assert config.alias == 'my_table_alias'
    assert config.node_schema == 'analytics'
    assert config.database == 'prod_db'
    assert config.materialized == 'table'
    assert config.airflow_parallelism == 4


def test_node_config_schedule_validation():
    config_data = {
        'enabled': True,
        'schema': 'test_schema',
        'tags': [],
        'meta': {},
        'materialized': 'table',
        'schedule': '@weekly',
        'schedule_shift': 60,
        'schedule_shift_unit': 'minute',
        'py_cluster': 'c1',
        'sql_cluster': 'c2',
        'daily_sql_cluster': 'c3',
        'bf_cluster': 'c4',
    }
    config = DbtNodeConfig(**config_data)
    assert config.schedule == EScheduleTag.weekly(timeshift=dt.timedelta(seconds=60 * 60))


def test_node_column(node_column):
    assert node_column.name == 'user_id'
    assert node_column.description == 'Unique user identifier'
    assert node_column.data_type == 'bigint'
    assert 'primary_key' in node_column.tags


def test_dbt_node_creation(dbt_node):
    assert dbt_node.name == 'fact_orders'
    assert dbt_node.resource_type == 'model'
    assert dbt_node.unique_id == 'model.my_project.fact_orders'
    assert dbt_node.database == 'analytics'
    assert dbt_node.node_schema == 'marts'


def test_dbt_node_hash(dbt_node):
    assert hash(dbt_node) == hash('model.my_project.fact_orders')


def test_dbt_node_equality(dbt_node, dbt_node_data):
    other_node = DbtNode(**dbt_node_data)
    assert dbt_node == other_node
    assert dbt_node == 'model.my_project.fact_orders'
    with pytest.raises(TypeError):
        _ = dbt_node == 123


def test_dbt_node_domain(dbt_node):
    assert dbt_node.domain == 'domain'


def test_dbt_node_test_domain(dbt_test_node_data):
    test_node = DbtNode(**dbt_test_node_data)
    assert test_node.domain == 'domain'


def test_dbt_node_resource_name(dbt_node):
    assert dbt_node.resource_name == 'fact_orders'


def test_dbt_node_test_resource_name():
    test_id = 'test.project.not_null_table__column.abc123'
    node_data = {
        'unique_id': test_id,
        'database': 'db',
        'schema': 'schema',
        'name': 'test',
        'resource_type': 'test',
        'package_name': 'project',
        'path': 'test.sql',
        'original_file_path': 'test.sql',
        'fqn': [],
        'checksum': {},
        'config': {
            'schema': 'schema',
            'enabled': True,
            'tags': [],
            'meta': {},
            'materialized': 'test',
            'py_cluster': 'c1',
            'sql_cluster': 'c2',
            'daily_sql_cluster': 'c3',
            'bf_cluster': 'c4',
        },
        'tags': [],
        'raw_code': '',
        'created_at': 0,
    }
    node = DbtNode(**node_data)
    assert node.resource_name == 'not_null_table__column'


def test_dbt_node_depends_on(dbt_node):
    deps = dbt_node.depends_on
    assert 'model.my_project.stg_orders' in deps
    assert 'test.my_project.test_orders' in deps


def test_dbt_node_depends_on_sources(dbt_node_data):
    dbt_node_data['depends_on']['nodes'].append('source.my_project.raw.orders')
    node = DbtNode(**dbt_node_data)
    assert 'source.my_project.raw.orders' in node.depends_on_sources


def test_dbt_node_materialized(dbt_node):
    assert dbt_node.materialized == 'incremental'


def test_dbt_node_test_type(dbt_test_node_data):
    test_node = DbtNode(**dbt_test_node_data)
    assert test_node.test_type == TestTag.small.value


def test_dbt_node_test_type_default(dbt_test_node_data):
    dbt_test_node_data['tags'] = []
    test_node = DbtNode(**dbt_test_node_data)
    assert test_node.test_type == TestTag.small.value


def test_dbt_node_model_type_sql(dbt_node):
    assert dbt_node.model_type == 'sql'


def test_dbt_node_model_type_py(dbt_node_data):
    dbt_node_data['path'] = 'models/marts/fact_orders.py'
    node = DbtNode(**dbt_node_data)
    assert node.model_type == 'py'


def test_dbt_node_target_environment(dbt_node, default_dbt_targets):
    assert dbt_node.target_environment(default_dbt_targets) == 'cluster3'


def test_dbt_node_target_environment_custom(dbt_node_data, default_dbt_targets):
    dbt_node_data['config']['dbt_target'] = 'custom_target'
    node = DbtNode(**dbt_node_data)
    assert node.target_environment(default_dbt_targets) == 'custom_target'


def test_dbt_node_is_model(dbt_node):
    assert dbt_node.is_model() is True


def test_dbt_node_is_view(dbt_node_data):
    dbt_node_data['config']['materialized'] = 'view'
    node = DbtNode(**dbt_node_data)
    assert node.is_view() is True


def test_dbt_node_is_snapshot(dbt_node_data):
    dbt_node_data['resource_type'] = 'snapshot'
    node = DbtNode(**dbt_node_data)
    assert node.is_snapshot() is True


def test_dbt_node_is_test(dbt_test_node_data):
    node = DbtNode(**dbt_test_node_data)
    assert node.is_test() is True


def test_dbt_node_is_seed(dbt_node_data):
    dbt_node_data['resource_type'] = 'seed'
    node = DbtNode(**dbt_node_data)
    assert node.is_seed() is True


def test_dbt_node_is_sql_model(dbt_node):
    assert dbt_node.is_sql_model() is True


def test_dbt_node_is_py_model(dbt_node_data):
    dbt_node_data['path'] = 'models/transform.py'
    node = DbtNode(**dbt_node_data)
    assert node.is_py_model() is True


def test_dbt_node_is_at_etl_service(dbt_node):
    assert dbt_node.is_at_etl_service('domain') is True
    assert dbt_node.is_at_etl_service('other') is False


def test_dbt_node_has_dt_partition(dbt_node_data):
    dbt_node_data['config']['partition_by'] = 'month_dt'
    node = DbtNode(**dbt_node_data)
    assert node.has_dt_partition() is True


def test_dbt_node_has_dt_partition_list(dbt_node_data):
    dbt_node_data['config']['partition_by'] = ['year', 'month_dt']
    node = DbtNode(**dbt_node_data)
    assert node.has_dt_partition() is True


def test_dbt_node_has_dt_partition_timestamp(dbt_node_data):
    dbt_node_data['config']['partition_by'] = 'timestamp'
    node = DbtNode(**dbt_node_data)
    assert node.has_dt_partition() is True


def test_dbt_node_get_airflow_parallelism_default(dbt_node):
    assert dbt_node.get_airflow_parallelism() == 1


def test_dbt_node_get_airflow_parallelism_incremental(dbt_node_data):
    dbt_node_data['config']['materialized'] = 'incremental'
    dbt_node_data['config']['partition_by'] = 'month_dt'
    dbt_node_data['config']['airflow_parallelism'] = 8
    node = DbtNode(**dbt_node_data)
    assert node.get_airflow_parallelism() == 8


def test_dbt_node_set_target_details(dbt_node, profile_mock, default_dbt_targets):
    dbt_node.set_target_details(profile_mock, default_dbt_targets)
    assert dbt_node.target_details is not None


def test_dbt_node_original_file_path_dirname(dbt_node):
    assert dbt_node.original_file_path_dirname == 'models/domain/marts'


def test_dbt_node_original_file_path_without_extension(dbt_node):
    assert dbt_node.original_file_path_without_extension == 'models/domain/marts/fact_orders'


def test_dbt_node_test_types(dbt_test_node_data):
    node = DbtNode(**dbt_test_node_data)
    assert node.is_small_test() is True
    assert node.is_medium_test() is False
    assert node.is_large_test() is False

    dbt_test_node_data['tags'] = ['@medium']
    dbt_test_node_data['config']['tags'] = ['@medium']
    node = DbtNode(**dbt_test_node_data)
    assert node.is_small_test() is False
    assert node.is_medium_test() is True
    assert node.is_large_test() is False

    dbt_test_node_data['tags'] = ['@large']
    dbt_test_node_data['config']['tags'] = ['@large']
    node = DbtNode(**dbt_test_node_data)
    assert node.is_small_test() is False
    assert node.is_medium_test() is False
    assert node.is_large_test() is True


def test_dbt_node_config_validation_error():
    invalid_config = {
        'enabled': True,
        'tags': [],
        'meta': {},
        'materialized': 'table',
        'py_cluster': None,
        'sql_cluster': None,
        'daily_sql_cluster': None,
        'bf_cluster': None,
    }
    with pytest.raises(ValueError):
        DbtNodeConfig(**invalid_config)

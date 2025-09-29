from typing import Any, Dict

import pytest

from dbt_af.parser.dbt_source_model import (
    DbtSource,
    DbtSourceConfig,
    DbtSourceFreshness,
    _FreshnessAfterModel,
)


@pytest.fixture
def freshness_after_model_empty():
    return _FreshnessAfterModel(count=None, period=None)


@pytest.fixture
def freshness_after_model_with_data():
    return _FreshnessAfterModel(count=5, period='hour')


@pytest.fixture
def source_freshness():
    return DbtSourceFreshness(
        warn_after=_FreshnessAfterModel(count=5, period='hour'),
        error_after=_FreshnessAfterModel(count=10, period='hour'),
        filter="created_at > '2024-01-01'",
    )


@pytest.fixture
def source_config_enabled():
    return DbtSourceConfig(enabled=True)


@pytest.fixture
def source_config_disabled():
    return DbtSourceConfig(enabled=False)


@pytest.fixture
def sample_source_data() -> Dict[str, Any]:
    return {
        'database': 'analytics_db',
        'schema': 'raw_data',
        'name': 'users_table',
        'resource_type': 'source',
        'package_name': 'my_project',
        'path': 'models/staging/sources.yml',
        'original_file_path': 'services/etl_service/models/staging/sources.yml',
        'unique_id': 'source.my_project.raw_data.users_table',
        'fqn': ['my_project', 'domain_name', 'staging', 'raw_data', 'users_table'],
        'source_name': 'raw_data',
        'source_description': 'Raw data from upstream systems',
        'loader': 'airflow',
        'identifier': 'users',
        'quoting': {'database': False, 'schema': False, 'identifier': False},
        'loaded_at_field': 'loaded_timestamp',
        'freshness': {'warn_after': {'count': 5, 'period': 'hour'}, 'error_after': {'count': 10, 'period': 'hour'}},
        'description': 'Users table from production database',
        'columns': {
            'id': {'name': 'id', 'description': 'User ID'},
            'email': {'name': 'email', 'description': 'User email'},
        },
        'meta': {'owner': 'data_team'},
        'source_meta': {'update_frequency': 'hourly'},
        'tags': ['pii', 'critical'],
        'config': {'enabled': True},
        'unrendered_config': {},
        'relation_name': 'analytics_db.raw_data.users',
        'created_at': 1234567890.0,
    }


@pytest.fixture
def dbt_source(sample_source_data):
    return DbtSource(**sample_source_data)


def test_freshness_after_model_empty(freshness_after_model_empty):
    assert freshness_after_model_empty.empty() is True


def test_freshness_after_model_not_empty_with_count():
    model = _FreshnessAfterModel(count=5, period=None)
    assert model.empty() is False


def test_freshness_after_model_not_empty_with_period():
    model = _FreshnessAfterModel(count=None, period='day')
    assert model.empty() is False


def test_freshness_after_model_not_empty_with_both(freshness_after_model_with_data):
    assert freshness_after_model_with_data.empty() is False


def test_source_freshness_creation(source_freshness):
    assert source_freshness.warn_after.count == 5
    assert source_freshness.warn_after.period == 'hour'
    assert source_freshness.error_after.count == 10
    assert source_freshness.filter == "created_at > '2024-01-01'"


def test_source_freshness_without_filter():
    freshness = DbtSourceFreshness(
        warn_after=_FreshnessAfterModel(count=5, period='hour'),
        error_after=_FreshnessAfterModel(count=10, period='hour'),
    )
    assert freshness.filter is None


def test_source_config_enabled(source_config_enabled):
    assert source_config_enabled.enabled is True


def test_source_config_disabled(source_config_disabled):
    assert source_config_disabled.enabled is False


def test_dbt_source_creation(dbt_source):
    assert dbt_source.database == 'analytics_db'
    assert dbt_source.node_schema == 'raw_data'
    assert dbt_source.name == 'users_table'
    assert dbt_source.unique_id == 'source.my_project.raw_data.users_table'
    assert dbt_source.loaded_at_field == 'loaded_timestamp'


def test_dbt_source_hash(dbt_source):
    assert hash(dbt_source) == hash('source.my_project.raw_data.users_table')


def test_dbt_source_is_at_etl_service(dbt_source):
    assert dbt_source.is_at_etl_service('etl_service') is True
    assert dbt_source.is_at_etl_service('other_service') is False


def test_dbt_source_original_file_path_dirname(dbt_source):
    assert dbt_source.original_file_path_dirname == 'services/etl_service/models/staging'


def test_dbt_source_domain_property(dbt_source):
    assert dbt_source.domain == 'domain_name'


def test_dbt_source_need_to_check_freshness_enabled(dbt_source):
    assert dbt_source.need_to_check_freshness() is True


def test_dbt_source_need_to_check_freshness_disabled(sample_source_data):
    sample_source_data['config']['enabled'] = False
    source = DbtSource(**sample_source_data)
    assert source.need_to_check_freshness() is False


def test_dbt_source_need_to_check_freshness_no_settings(sample_source_data):
    sample_source_data['freshness'] = {
        'warn_after': {'count': None, 'period': None},
        'error_after': {'count': None, 'period': None},
    }
    source = DbtSource(**sample_source_data)
    assert source.need_to_check_freshness() is False


def test_dbt_source_with_external(sample_source_data):
    sample_source_data['external'] = {'location': 's3://bucket/path'}
    source = DbtSource(**sample_source_data)
    assert source.external == {'location': 's3://bucket/path'}


def test_dbt_source_with_patch_path(sample_source_data):
    sample_source_data['patch_path'] = 'models/staging/schema.yml'
    source = DbtSource(**sample_source_data)
    assert source.patch_path == 'models/staging/schema.yml'


def test_dbt_source_without_optional_fields(sample_source_data):
    sample_source_data['external'] = None
    sample_source_data['patch_path'] = None
    sample_source_data['loaded_at_field'] = None
    source = DbtSource(**sample_source_data)
    assert source.external is None
    assert source.patch_path is None
    assert source.loaded_at_field is None


def test_dbt_source_equality_by_unique_id(sample_source_data):
    source1 = DbtSource(**sample_source_data)
    source2 = DbtSource(**sample_source_data)
    assert hash(source1) == hash(source2)


def test_dbt_source_with_empty_tags(sample_source_data):
    sample_source_data['tags'] = []
    source = DbtSource(**sample_source_data)
    assert source.tags == []


def test_dbt_source_freshness_filter(sample_source_data):
    sample_source_data['freshness']['filter'] = 'dt > current_date() - 7'
    source = DbtSource(**sample_source_data)
    assert source.freshness.filter == 'dt > current_date() - 7'

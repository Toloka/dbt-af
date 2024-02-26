import json
from pathlib import Path

import pytest
import yaml

from dbt_af.builder.dbt_af_builder import DbtAfGraph
from dbt_af.conf import Config, DbtDefaultTargetsConfig, DbtProjectConfig


def pytest_addoption(parser):
    parser.addoption('--manifest_path', action='store', default='.', help='Path to manifest.json')
    parser.addoption('--profiles_path', action='store', default='.', help='Path to profiles.yml')
    parser.addoption('--dbt_project_path', action='store', default='.', help='Path to dbt_project.yml')
    parser.addoption('--etl_name', action='store', default=None, help='List of ETLs to test')
    parser.addoption('--target', action='store', default='dev', help='Default target to use')


@pytest.fixture(scope='session')
def get_manifest_path(pytestconfig):
    return pytestconfig.getoption('manifest_path')


@pytest.fixture(scope='session')
def get_profiles_path(pytestconfig):
    return pytestconfig.getoption('profiles_path')


@pytest.fixture(scope='session')
def get_dbt_project_path(pytestconfig):
    return pytestconfig.getoption('dbt_project_path')


@pytest.fixture(scope='session')
def get_etl_name(pytestconfig):
    return pytestconfig.getoption('etl_name')


@pytest.fixture(scope='session')
def target(pytestconfig):
    return pytestconfig.getoption('target')


@pytest.fixture
def config(target):
    return Config(
        dbt_project=DbtProjectConfig(
            dbt_project_name='dwh',
            dbt_models_path='.',
            dbt_project_path='.',
            dbt_profiles_path='.',
            dbt_target_path='.',
            dbt_log_path='.',
            dbt_schema='.',
        ),
        dbt_default_targets=DbtDefaultTargetsConfig(default_target=target),
    )


@pytest.fixture
def manifest(get_manifest_path) -> dict:
    manifest_path = (
        get_manifest_path if get_manifest_path.endswith('manifest.json') else Path(get_manifest_path) / 'manifest.json'
    )
    with open(manifest_path, 'r') as f:
        return json.load(f)


@pytest.fixture
def profiles(get_profiles_path) -> dict:
    profiles_path = (
        get_profiles_path if get_profiles_path.endswith('profiles.yml') else Path(get_profiles_path) / 'profiles.yml'
    )
    with open(profiles_path, 'r') as f:
        return yaml.safe_load(f)


@pytest.fixture
def dbt_project(get_dbt_project_path):
    dbt_project_path = (
        get_dbt_project_path
        if get_dbt_project_path.endswith('dbt_project.yml')
        else Path(get_dbt_project_path) / 'dbt_project.yml'
    )
    with open(dbt_project_path, 'r') as f:
        return yaml.safe_load(f)


@pytest.fixture
def project_profile_name(dbt_project):
    return dbt_project['profile']


@pytest.fixture
def dbt_af_graph(manifest, profiles, project_profile_name, config):
    return DbtAfGraph.from_manifest(manifest, profiles, project_profile_name, etl_service_name='', config=config)

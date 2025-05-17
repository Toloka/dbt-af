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
    parser.addoption('--models_path', action='store', default='.', help='Path to models')
    parser.addoption('--etl_name', action='store', default=None, help='List of ETLs to test')
    parser.addoption('--target', action='store', default='dev', help='Default target to use')


@pytest.fixture(scope='session')
def manifest_path(pytestconfig) -> Path:
    manifest_path = pytestconfig.getoption('manifest_path')
    return Path(manifest_path) if manifest_path.endswith('manifest.json') else Path(manifest_path) / 'manifest.json'


@pytest.fixture(scope='session')
def profiles_path(pytestconfig) -> Path:
    profiles_path = pytestconfig.getoption('profiles_path')
    return Path(profiles_path) if profiles_path.endswith('profiles.yml') else Path(profiles_path) / 'profiles.yml'


@pytest.fixture(scope='session')
def dbt_project_path(pytestconfig) -> Path:
    dbt_project_path = pytestconfig.getoption('dbt_project_path')
    return (
        Path(dbt_project_path)
        if dbt_project_path.endswith('dbt_project.yml')
        else Path(dbt_project_path) / 'dbt_project.yml'
    )


@pytest.fixture(scope='session')
def models_path(pytestconfig) -> Path:
    return Path(pytestconfig.getoption('models_path'))


@pytest.fixture(scope='session')
def etl_name(pytestconfig):
    return pytestconfig.getoption('etl_name')


@pytest.fixture(scope='session')
def target(pytestconfig):
    return pytestconfig.getoption('target')


@pytest.fixture
def config(models_path, dbt_project_path, profiles_path, target):
    return Config(
        dbt_project=DbtProjectConfig(
            dbt_project_name='dwh',
            dbt_models_path=models_path,
            dbt_project_path=dbt_project_path,
            dbt_profiles_path=profiles_path,
            dbt_target_path=dbt_project_path.parent,
            dbt_log_path=dbt_project_path.parent,
            dbt_schema='schema',
        ),
        dbt_default_targets=DbtDefaultTargetsConfig(default_target=target),
    )


@pytest.fixture
def manifest(manifest_path) -> dict:
    with open(manifest_path, 'r') as f:
        return json.load(f)


@pytest.fixture
def profiles(profiles_path) -> dict:
    with open(profiles_path, 'r') as f:
        return yaml.safe_load(f)


@pytest.fixture
def dbt_project(dbt_project_path):
    with open(dbt_project_path, 'r') as f:
        return yaml.safe_load(f)


@pytest.fixture
def project_profile_name(dbt_project):
    return dbt_project['profile']


@pytest.fixture
def dbt_af_graph(manifest, profiles, project_profile_name, config):
    return DbtAfGraph.from_manifest(manifest, profiles, project_profile_name, etl_service_name='', config=config)

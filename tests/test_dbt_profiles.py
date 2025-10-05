import sys

import pytest

from dbt_af.parser.dbt_profiles import (
    KubernetesTarget,
    KubernetesToleration,
    Profile,
    Profiles,
    Target,
    VenvTarget,
)


@pytest.fixture
def base_target_data():
    return {
        'type': 'postgres',
        'host': 'localhost',
        'port': 5432,
        'user': 'test_user',
        'password': 'test_password',
        'database': 'test_db',
    }


@pytest.fixture
def kubernetes_toleration():
    return KubernetesToleration(key='dedicated', operator='Equal')


@pytest.fixture
def kubernetes_target_data():
    return {
        'type': 'kubernetes',
        'node_pool_selector_name': 'node-pool',
        'node_pool': 'data-processing',
        'image_name': 'dbt-runner:latest',
        'pod_cpu_guarantee': '1000m',
        'pod_memory_guarantee': '2Gi',
        'tolerations': [{'key': 'dedicated', 'operator': 'Equal'}, {'key': 'spot', 'operator': 'Exists'}],
    }


@pytest.fixture
def venv_target_data():
    return {
        'type': 'venv',
        'system_site_packages': True,
        'requirements': ['dbt-core>=1.0.0', 'dbt-bigquery'],
        'python_version': '3.10',
        'pip_install_options': ['--no-cache-dir'],
        'index_urls': ['https://pypi.org/simple'],
        'inherit_env': True,
    }


@pytest.fixture
def profile_data():
    return {
        'target': 'dev',
        'outputs': {
            'dev': {
                'type': 'venv',
                'system_site_packages': False,
                'requirements': ['dbt-core'],
                'python_version': '3.9',
            },
            'prod': {
                'type': 'kubernetes',
                'node_pool_selector_name': 'production',
                'node_pool': 'prod-pool',
                'image_name': 'dbt-prod:latest',
                'pod_cpu_guarantee': '2000m',
                'pod_memory_guarantee': '4Gi',
                'tolerations': [],
            },
        },
    }


@pytest.fixture
def profiles_data():
    return {
        'config': {'send_anonymous_usage_stats': False},
        'my_project': {
            'target': 'dev',
            'outputs': {
                'dev': {
                    'type': 'postgres',
                    'host': 'localhost',
                    'port': 5432,
                    'user': 'dev_user',
                    'password': 'dev_password',
                    'database': 'dev_db',
                }
            },
        },
    }


def test_target_creation(base_target_data):
    target = Target(**base_target_data)
    assert target.target_type == 'postgres'
    assert target.host == 'localhost'
    assert target.port == 5432


def test_target_extra_fields_allowed(base_target_data):
    base_target_data['custom_field'] = 'custom_value'
    target = Target(**base_target_data)
    assert target.custom_field == 'custom_value'


def test_target_root_validator(base_target_data):
    base_target_data['extra_param'] = 'extra_value'
    target = Target(**base_target_data)
    assert hasattr(target, 'extra_param')
    assert target.extra_param == 'extra_value'


def test_kubernetes_toleration(kubernetes_toleration):
    assert kubernetes_toleration.key == 'dedicated'
    assert kubernetes_toleration.operator == 'Equal'


def test_kubernetes_target_creation(kubernetes_target_data):
    k8s_target = KubernetesTarget(**kubernetes_target_data)
    assert k8s_target.target_type == 'kubernetes'
    assert k8s_target.node_pool_selector_name == 'node-pool'
    assert k8s_target.node_pool == 'data-processing'
    assert k8s_target.image_name == 'dbt-runner:latest'
    assert k8s_target.pod_cpu_guarantee == '1000m'
    assert k8s_target.pod_memory_guarantee == '2Gi'
    assert len(k8s_target.tolerations) == 2
    assert k8s_target.tolerations[0].key == 'dedicated'


def test_venv_target_creation(venv_target_data):
    venv_target = VenvTarget(**venv_target_data)
    assert venv_target.target_type == 'venv'
    assert venv_target.system_site_packages is True
    assert len(venv_target.requirements) == 2
    assert 'dbt-core>=1.0.0' in venv_target.requirements
    assert venv_target.python_version == '3.10'
    assert venv_target.inherit_env is True


def test_venv_target_defaults():
    minimal_venv = VenvTarget(type='venv', system_site_packages=False)
    assert minimal_venv.requirements == []
    assert minimal_venv.pip_install_options == []
    assert minimal_venv.index_urls == []
    assert minimal_venv.inherit_env is False
    assert minimal_venv.python_version == '.'.join(map(str, sys.version_info[:2]))


def test_profile_creation(profile_data):
    profile = Profile(**profile_data)
    assert profile.target == 'dev'
    assert 'dev' in profile.outputs
    assert 'prod' in profile.outputs
    assert isinstance(profile.outputs['dev'], VenvTarget)
    assert isinstance(profile.outputs['prod'], KubernetesTarget)


def test_profile_with_generic_target():
    profile_data = {
        'target': 'custom',
        'outputs': {
            'custom': {'type': 'spark', 'method': 'thrift', 'host': 'spark-cluster', 'port': 10000, 'schema': 'default'}
        },
    }
    profile = Profile(**profile_data)
    assert profile.target == 'custom'
    assert isinstance(profile.outputs['custom'], Target)
    assert profile.outputs['custom'].target_type == 'spark'


def test_profiles_creation(profiles_data):
    profiles = Profiles(**profiles_data)
    assert profiles.profiles_config == {'send_anonymous_usage_stats': False}
    assert hasattr(profiles, 'my_project')
    assert isinstance(profiles.my_project, Profile)
    assert profiles.my_project.target == 'dev'


def test_profiles_getitem(profiles_data):
    profiles = Profiles(**profiles_data)
    project = profiles['my_project']
    assert isinstance(project, Profile)
    assert project.target == 'dev'


def test_profiles_multiple_projects():
    data = {
        'config': {'use_colors': True},
        'project1': {'target': 'dev', 'outputs': {'dev': {'type': 'postgres', 'host': 'localhost', 'database': 'db1'}}},
        'project2': {
            'target': 'prod',
            'outputs': {'prod': {'type': 'bigquery', 'project': 'gcp-project', 'dataset': 'analytics'}},
        },
    }
    profiles = Profiles(**data)
    assert hasattr(profiles, 'project1')
    assert hasattr(profiles, 'project2')
    assert profiles.project1.target == 'dev'
    assert profiles.project2.target == 'prod'


def test_profiles_extra_allowed():
    profiles = Profiles(
        config={}, another_project={'target': 'test', 'outputs': {'test': {'type': 'sqlite', 'path': ':memory:'}}}
    )
    assert hasattr(profiles, 'another_project')
    assert isinstance(profiles.another_project, Profile)


def test_kubernetes_target_with_extra_fields():
    data = {
        'type': 'kubernetes',
        'node_pool_selector_name': 'pool',
        'node_pool': 'default',
        'image_name': 'image:tag',
        'pod_cpu_guarantee': '500m',
        'pod_memory_guarantee': '1Gi',
        'tolerations': [],
        'extra_k8s_config': 'value',
        'namespace': 'dbt',
    }
    target = KubernetesTarget(**data)
    assert target.extra_k8s_config == 'value'
    assert target.namespace == 'dbt'


def test_venv_target_with_multiple_requirements():
    target = VenvTarget(
        type='venv',
        system_site_packages=False,
        requirements=['dbt-core==1.5.0', 'dbt-postgres>=1.5.0', 'dbt-snowflake', 'pandas>=1.0.0'],
        index_urls=['https://pypi.org/simple', 'https://custom.pypi.org/simple'],
    )
    assert len(target.requirements) == 4
    assert len(target.index_urls) == 2

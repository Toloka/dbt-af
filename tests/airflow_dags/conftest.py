import contextlib
import json
import os
import shutil
from collections import defaultdict
from pathlib import Path
from tempfile import TemporaryDirectory
from textwrap import indent

import pytest
import yaml
from click.testing import CliRunner
from dbt.cli import dbt_cli

from dbt_af.builder.dbt_af_builder import DbtAfGraph, DbtNode
from dbt_af.conf import (
    Config,
    DbtDefaultTargetsConfig,
    DbtProjectConfig,
    K8sConfig,
    MCDIntegrationConfig,
    TableauIntegrationConfig,
)

# Project specific hack to catch as many error as possible
DBT_FIXTURES_DIR = Path(__file__).parent.absolute() / 'fixtures'


@pytest.fixture
def get_config():
    def _create_dbt_af_config(
        target_path: Path,
        with_mcd: bool = False,
        with_tableau: bool = False,
        with_k8s: bool = False,
    ):
        project_path = target_path.parent

        mcd_config = None
        if with_mcd:
            mcd_config = MCDIntegrationConfig(
                callbacks_enabled=True,
                artifacts_export_enabled=True,
                success_required=True,
                metastore_name='fake_metastore_name',
            )
        tableau_config = None
        if with_tableau:
            tableau_config = TableauIntegrationConfig(
                server_address='fake_server_address',
                username='fake_username',
                password='fake_password',
                site_id='fake_site_id',
            )
        k8s_config = None
        if with_k8s:
            k8s_config = K8sConfig(
                airflow_identity_binding_selector='fake_airflow_identity_binding_selector',
            )

        return Config(
            dbt_project=DbtProjectConfig(
                dbt_project_name='dwh',
                dbt_models_path=project_path / 'etl_service' / 'dbt' / 'models',
                dbt_project_path=project_path,
                dbt_profiles_path=project_path,
                dbt_target_path=project_path,
                dbt_log_path=project_path,
                dbt_schema='.',
            ),
            dbt_default_targets=DbtDefaultTargetsConfig(
                default_target='prod',
                default_for_tests_target='prod_data_test_cluster',
                default_maintenance_target='prod_sql_cluster',
                default_backfill_target='prod_bf_cluster',
            ),
            dry_run=True,
            mcd=mcd_config,
            tableau=tableau_config,
            k8s=k8s_config,
        )

    return _create_dbt_af_config


def prepare_env_for_test():
    # fmt: off
    env_vars = {
        'DBT_CATALOG': 'dummy',
        'DBT_SCHEMA': 'dummy',
        'DBT_TOKEN': 'dummy',
        'DBT_HTTP_PATH': 'dummy',
        'DBT_HOST': 'dummy',
        'DBT_TARGET_ENV': 'dev',
        'DBT_PROFILE_NAME': 'dev',
    }
    # fmt: on
    return env_vars


def get_dbt_project_yaml_for_test(test_location):
    project = dict()

    with open(Path(test_location) / 'models.yml') as fin:
        test_model_config = yaml.safe_load(fin)

    project['name'] = 'dwh'
    project['profile'] = 'main_profile'
    project['model-paths'] = ['etl_service/dbt/models']
    project['snapshot-paths'] = ['etl_service/dbt/snapshots']
    project['analysis-paths'] = ['etl_service/dbt/analyses']
    project['test-paths'] = ['etl_service/dbt/tests']
    project['seed-paths'] = ['etl_service/dbt/seeds']
    project['macro-paths'] = ['etl_service/dbt/macros']

    project['models'] = defaultdict(dict)
    project['models'][project['name']] = test_model_config
    project['models'][project['name']]['sql_cluster'] = 'fake_sql_cluster'
    project['models'][project['name']]['daily_sql_cluster'] = 'fake_daily_sql_cluster'
    project['models'][project['name']]['py_cluster'] = 'fake_py_cluster'
    project['models'][project['name']]['bf_cluster'] = 'fake_bf_cluster'
    project['models'][project['name']]['prod_data_test_cluster'] = 'fake_prod_data_test_cluster'
    project['models'][project['name']]['k8s_cluster'] = 'fake_k8s_cluster'
    project['models'] = dict(project['models'])

    return project


def get_dbt_profiles_yaml_for_test():
    target = {
        'type': 'postgres',
        'host': 'localhost',
        'port': 5555,
        'database': 'postgres',
        'schema': 'fake_schema',
        'user': 'postgres',
        'password': 'postgres',
    }
    dbt_profiles = {
        'config': {'send_anonymous_usage_stats': False},
        'main_profile': {
            'target': 'dev',
            'outputs': {
                'dev': target,
                'fake_sql_cluster': target,
                'fake_daily_sql_cluster': target,
                'fake_py_cluster': target,
                'fake_bf_cluster': target,
                'prod_data_test_cluster': target,
                'fake_k8s_cluster': {
                    'type': 'kubernetes',
                    'schema': 'fake_schema',
                    'node_pool_selector_name': 'fake_node_pool_selector_name',
                    'node_pool': 'fake_node_pool',
                    'image_name': 'fake_image_name',
                    'pod_cpu_guarantee': '1m',
                    'pod_memory_guarantee': '1Mi',
                    'tolerations': [{'key': 'fake_key', 'operator': 'fake_operator'}],
                },
                'fake_venv_target': {
                    'type': 'venv',
                    'schema': 'fake_schema',
                    'requirements': ['pandas', 'numpy'],
                    'system_site_packages': False,
                    'python_version': '3.10',
                    'pip_install_options': ['--no-cache-dir'],
                    'index_urls': ['https://pypi.org/simple'],
                    'inherit_env': True,
                },
            },
        },
    }
    return dbt_profiles


class TestManifest:
    def __init__(self, test_name, *args, **kwargs):
        self._test_name = test_name

    def __enter__(self):
        raise NotImplementedError

    def __exit__(self, exc_type, exc_val, exc_tb):
        raise NotImplementedError


class TmpManifest(TestManifest):
    def __init__(self, test_name: str, with_dbt_run_check: bool = False):
        super().__init__(test_name)
        self._tmp_dir = TemporaryDirectory(prefix='tdp-test-')

        self.with_dbt_run_check = with_dbt_run_check

    def __enter__(self):
        tmpdir = self._tmp_dir.__enter__()
        fixture_path = DBT_FIXTURES_DIR / self._test_name
        shutil.copytree(fixture_path / 'models', Path(tmpdir) / 'etl_service' / 'dbt' / 'models')
        project_yaml = get_dbt_project_yaml_for_test(fixture_path)
        project_yaml['packages-install-path'] = (Path(tmpdir) / 'dbt_packages').absolute().as_posix()

        with open(Path(tmpdir) / 'dbt_project.yml', 'w') as fout:
            yaml.safe_dump(project_yaml, fout)

        project_location = tmpdir

        with open(Path(tmpdir) / 'profiles.yml', 'w') as fout:
            yaml.safe_dump(get_dbt_profiles_yaml_for_test(), fout)

        profile_location = tmpdir
        target_env = 'dev'

        target_dir = Path(tmpdir) / 'target'

        vars_ = {
            # 'start_dttm': env['START_DTTM'],
            # 'end_dttm': env['END_DTTM'],
            'overlap': False
        }

        dbt_command_result = CliRunner().invoke(
            dbt_cli,
            [
                '--debug',
                'parse',
                '--project-dir',
                project_location,
                '--profiles-dir',
                profile_location,
                '--target',
                target_env,
                '--target-path',
                target_dir,
                '--vars',
                json.dumps(vars_),
            ],
            env={**os.environ, **prepare_env_for_test()},
        )

        if dbt_command_result.exit_code != 0:
            exception = indent(dbt_command_result.stdout, ' ' * 4)
            raise RuntimeError('Could not compile dbt. Error:\n' + exception)

        if self.with_dbt_run_check:
            dbt_run_command_result = CliRunner().invoke(
                dbt_cli,
                [
                    '--debug',
                    'run',
                    '--project-dir',
                    project_location,
                    '--profiles-dir',
                    profile_location,
                    '--target',
                    target_env,
                    '--target-path',
                    target_dir,
                    '--vars',
                    json.dumps(vars_),
                ],
                env={**os.environ, **prepare_env_for_test()},
            )

            if dbt_run_command_result.exit_code != 0:
                exception = indent(dbt_run_command_result.stdout, ' ' * 4)
                raise RuntimeError('Could not compile dbt-run. Error:\n' + exception)

            with open(target_dir / 'run_results.json', 'r') as fin:
                run_results = json.load(fin)

                if not len(run_results['results']):
                    raise RuntimeError(f'Got empty results in run_results.json, raw data: {run_results}')

                for result in run_results['results']:
                    if result['status'] != 'success':
                        raise RuntimeError(f'Got non-successful result for node {result["unique_id"]}, data: {result}')

        return target_dir

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._tmp_dir.__exit__(exc_type, exc_val, exc_tb)


class FileManifest(TestManifest):
    """DBT manifest as a Golden file"""

    # TODO


class DockerManifest(TestManifest):
    """DBT manifest that will be created inside a docker container"""

    # TODO


@pytest.fixture
def mock_node_is_etl_service(mocker):
    mocker.patch.object(DbtNode, 'is_at_etl_service', lambda *args, **kwargs: True)


@pytest.fixture
def mock_init_airflow_environment(mocker):
    import dbt_af.operators.base as module_in_use

    mocker.patch(f'{module_in_use.__name__}.{module_in_use.init_environment.__name__}', return_value=dict())


@pytest.fixture
def mock_mcd_callbacks(mocker):
    import dbt_af.dags as module_in_use

    mocker.patch(
        f'{module_in_use.__name__}.{module_in_use.collect_af_custom_callbacks.__name__}',
        return_value=(dict(), dict()),
    )


@pytest.fixture
def dbt_manifest(mocker):
    @contextlib.contextmanager
    def _dbt_manifest(fixture_name, with_dbt_run_check: bool = False):
        with TmpManifest(fixture_name, with_dbt_run_check=with_dbt_run_check) as manifest_path:
            yield manifest_path

    return _dbt_manifest


@pytest.fixture
def dbt_profiles():
    @contextlib.contextmanager
    def _dbt_profiles():
        fake_dbt_profiles = get_dbt_profiles_yaml_for_test()
        yield fake_dbt_profiles, 'main_profile'

    return _dbt_profiles


@pytest.fixture
def compiled_main_dags(
    dbt_manifest,
    dbt_profiles,
    mock_node_is_etl_service,
    mock_init_airflow_environment,
    mock_mcd_callbacks,
    get_config,
    socket_disabled,
):
    @contextlib.contextmanager
    def _dags(
        fixture_name: str,
        with_mcd: bool = False,
        with_tableau: bool = False,
        with_k8s: bool = False,
        with_dbt_run_check: bool = False,
    ):
        with dbt_manifest(fixture_name, with_dbt_run_check=with_dbt_run_check) as manifest_path, dbt_profiles() as (
            profiles,
            profile_name,
        ):
            from dbt_af.dags import dbt_main_dags

            with open(manifest_path / 'manifest.json') as fin:
                manifest_content = json.load(fin)

            config = get_config(manifest_path, with_mcd=with_mcd, with_tableau=with_tableau, with_k8s=with_k8s)

            graph = DbtAfGraph.from_manifest(
                manifest_content, profiles, profile_name, etl_service_name='dummy', config=config
            )
            yield dbt_main_dags(graph)

    return _dags


@pytest.fixture
def dags_domain_depends_on_another_partially(compiled_main_dags):
    """
       + -> A2 -> +
    A1 +          + -> B2
       + -------> +
                  |
    B1 + -------> +

    """
    with compiled_main_dags('domain_depends_on_another_partially', with_dbt_run_check=True) as dags:
        yield dags


@pytest.fixture
def dags_domain_depends_on_two_domains(compiled_main_dags):
    """
    A1 -> +
          + -> C1
    B1 -> +

    """
    with compiled_main_dags('domain_depends_on_two_domains', with_dbt_run_check=True) as dags:
        yield dags


@pytest.fixture
def dags_hourly_task_with_tests(compiled_main_dags):
    with compiled_main_dags('hourly_task_with_tests', with_dbt_run_check=True) as dags:
        yield dags


@pytest.fixture
def dags_independent_domains(compiled_main_dags):
    """
    A1 -> A2
    B1 -> B2
    """
    with compiled_main_dags('independent_domains', with_dbt_run_check=True) as dags:
        yield dags


@pytest.fixture
def dags_sequential_domains(compiled_main_dags):
    """
    (a1 -> a2) -> (b1 -> b2) -> c1

    """
    with compiled_main_dags('sequential_domains', with_dbt_run_check=True) as dags:
        yield dags


@pytest.fixture
def dags_sequential_tasks_in_one_domain(compiled_main_dags):
    """
    A1 -> A2 -> A3
    """
    with compiled_main_dags('sequential_tasks_in_one_domain', with_dbt_run_check=True) as dags:
        yield dags


@pytest.fixture
def dags_two_domains_depend_on_another(compiled_main_dags):
    """
          + -> B1
    A1 -> +
          + -> C1

    """
    with compiled_main_dags('two_domains_depend_on_another', with_dbt_run_check=True) as dags:
        yield dags


@pytest.fixture
def dags_two_domains_depend_on_two(compiled_main_dags):
    """
    A1 -> +
          + -> C1
          + -> D1
    B1 -> +

    """
    with compiled_main_dags('two_domains_depend_on_two', with_dbt_run_check=True) as dags:
        yield dags


@pytest.fixture
def dags_task_depends_on_two_within_same_domain(compiled_main_dags):
    """
    A1 -+
        +--> A3
    A2 -+
    """
    with compiled_main_dags('task_depends_on_two_within_same_domain', with_dbt_run_check=True) as dags:
        yield dags


@pytest.fixture
def dags_two_tasks_depend_on_one(compiled_main_dags):
    """
        +--> A2
    A1 -+
        +--> A3
    """
    with compiled_main_dags('two_tasks_depend_on_one', with_dbt_run_check=True) as dags:
        yield dags


@pytest.fixture
def dags_two_tasks_depend_on_two(compiled_main_dags):
    """
    A1 -> +
          + -> A3
          + -> A4
    A2 -> +
    """
    with compiled_main_dags('two_tasks_depend_on_two', with_dbt_run_check=True) as dags:
        yield dags


@pytest.fixture
def dags_domain_with_different_schedule(compiled_main_dags):
    """
    A1@hourly -> A2@daily -> A3@hourly
    """
    with compiled_main_dags('domain_with_different_schedule', with_dbt_run_check=True) as dags:
        yield dags


@pytest.fixture
def dags_domain_depends_on_another_with_multischeduling(compiled_main_dags):
    """
    A1@hourly -> A2@daily -> B1@hourly
    """
    with compiled_main_dags('domain_depends_on_another_with_multischeduling', with_dbt_run_check=True) as dags:
        yield dags


@pytest.fixture
def dags_domain_depends_on_another_with_test(compiled_main_dags):
    """
    A1@hourly(+small test) -> B1@daily
    """
    with compiled_main_dags('domain_depends_on_another_with_test', with_dbt_run_check=True) as dags:
        yield dags


@pytest.fixture
def dags_domain_w_enable_disable_models(compiled_main_dags):
    with compiled_main_dags('domain_w_enable_disable_models', with_dbt_run_check=True) as dags:
        yield dags


@pytest.fixture
def dags_domain_w_source_freshness(compiled_main_dags):
    with compiled_main_dags('domain_w_source_freshness', with_dbt_run_check=True) as dags:
        yield dags


@pytest.fixture
def dags_domain_w_task_in_kubernetes(compiled_main_dags):
    with compiled_main_dags('domain_w_task_in_kubernetes') as dags:
        yield dags


@pytest.fixture
def dags_domain_w_task_in_venv(compiled_main_dags):
    with compiled_main_dags('domain_w_task_in_venv') as dags:
        yield dags


@pytest.fixture
def dags_domain_model_w_maintenance(compiled_main_dags):
    with compiled_main_dags('domain_model_w_maintenance') as dags:
        yield dags


@pytest.fixture
def dags_task_with_tableau_integration(compiled_main_dags):
    with compiled_main_dags('task_with_tableau_integration', with_tableau=True, with_dbt_run_check=True) as dags:
        yield dags


@pytest.fixture
def dags_domain_with_shift(compiled_main_dags):
    """
    A1@every15minutes_shift_3_minutes
    A2@hourly_shift_5_minutes
    A3@daily_shift_5_hours
    A4@weekly_shift_2_days
    A5@monthly_shift_2_days
    A6@monthly_shift_1_days_6_hours
    """
    with compiled_main_dags('domain_w_shift', with_dbt_run_check=True) as dags:
        yield dags


@pytest.fixture
def dags_two_domains_with_diff_scheduling_and_shifts(compiled_main_dags):
    """
    A1@hourly_shift_10_minutes -> B1@daily_shift_2_hours
    """
    with compiled_main_dags('two_domains_with_diff_scheduling_and_shifts', with_dbt_run_check=True) as dags:
        yield dags

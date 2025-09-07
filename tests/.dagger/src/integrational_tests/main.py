import typing as tp

import anyio
import dagger
from dagger import DefaultPath, Doc, dag, function, object_type
from packaging.version import Version

PYTHON_VERSIONS = [Version('3.10'), Version('3.11'), Version('3.12')]
AIRFLOW_2_VERSIONS = [
    Version('2.6.3'),
    Version('2.7.3'),
    Version('2.8.4'),
    Version('2.9.3'),
    Version('2.10.5'),
    Version('2.11.0'),
]
AIRFLOW_3_VERSIONS = [Version('3.0.6')]


@object_type
class IntegrationalTests:
    @function
    async def build_env(
        self,
        source: tp.Annotated[dagger.Directory, DefaultPath('/'), Doc('dbt-af source directory')],
        python_version: str,
        airflow_version: str,
    ) -> dagger.Container:
        python_cache = dag.cache_volume('python_cache')

        base: dagger.Container = (
            dag.container()
            .from_(f'apache/airflow:{airflow_version}-python{python_version}')
            .with_user('root')
            .with_exec(['apt-get', 'update', '--allow-releaseinfo-change'])
            .with_exec(['apt-get', 'install', '--no-install-recommends', '-y', 'build-essential', 'libpq-dev'])
            # .with_exec(['bash', '-lc', 'curl -LsSf https://astral.sh/uv/install.sh | sh'])
            # .with_user('airflow')
            # .with_exec(['pip', 'install', 'uv'])
        )

        airflow_home_env = await base.env_variable('AIRFLOW_HOME')
        try:
            await base.with_user('airflow').with_exec(['which', 'uv'])
            pip_cmd = ['uv', 'pip']
            is_uv_available = True
        except dagger.ExecError:
            pip_cmd = ['pip']
            is_uv_available = False

        env = (
            base.with_mounted_cache(f'{airflow_home_env}.local', python_cache)
            .with_directory(f'{airflow_home_env}/dbt_af/dbt_af', source.directory('dbt_af'), owner='airflow:0')
            .with_directory(
                f'{airflow_home_env}/dbt_af/dbt_af_functional_tests',
                source.directory('dbt_af_functional_tests'),
                owner='airflow:0',
            )
            .with_directory(f'{airflow_home_env}/dbt_af/scripts', source.directory('scripts'), owner='airflow:0')
            .with_directory(f'{airflow_home_env}/dbt_af/tests', source.directory('tests'), owner='airflow:0')
            .with_file(f'{airflow_home_env}/dbt_af/pyproject.toml', source.file('pyproject.toml'), owner='airflow:0')
            .with_file(f'{airflow_home_env}/dbt_af/poetry.lock', source.file('poetry.lock'), owner='airflow:0')
            .with_file(
                f'{airflow_home_env}/dbt_af/requirements.txt',
                source.file('requirements.txt'),
                owner='airflow:0',
            )
            .with_file(
                f'{airflow_home_env}/dbt_af/requirements-dev.txt',
                source.file('requirements-dev.txt'),
                owner='airflow:0',
            )
            .with_file(f'{airflow_home_env}/dbt_af/README.md', source.file('README.md'), owner='airflow:0')
            .with_user('root')
            .with_workdir(f'{airflow_home_env}/dbt_af')
            .with_exec([*pip_cmd, 'install', '--system', '-e', f'{airflow_home_env}/dbt_af[all]'])
            .with_exec(
                [
                    *pip_cmd,
                    'install',
                    '--system',
                    '-c',
                    f'https://raw.githubusercontent.com/apache/airflow/constraints-{airflow_version}/constraints-{python_version}.txt',
                    '-r',
                    'requirements.txt',
                ]
            )
            .with_exec([*pip_cmd, 'install', '--system', '-r', 'requirements-dev.txt'])
            .with_exec([*pip_cmd, 'install', '--system', f'apache-airflow[fab,cncf-kubernetes]=={airflow_version}'])
            .with_workdir(airflow_home_env)
        )

        return env

    @function
    async def test_one_versions_combination(
        self,
        source: tp.Annotated[dagger.Directory, DefaultPath('/'), Doc('dbt-af source directory')],
        python_version: str,
        airflow_version: str,
    ) -> str:
        env = await self.build_env(
            source=source,
            python_version=python_version,
            airflow_version=airflow_version,
        )
        airflow_home_env = await env.env_variable('AIRFLOW_HOME')
        return await (
            env.with_workdir(f'{airflow_home_env}/dbt_af')
            # .terminal()
            .with_exec(['pytest', '-qsvv', '--log-cli-level=INFO', 'tests'])
            .stdout()
        )

    @function
    async def all(
        self,
        source: tp.Annotated[dagger.Directory, DefaultPath('/'), Doc('dbt-af source directory')],
        python_versions: list[str] | None = None,
        airflow_versions: list[str] | None = None,
    ):
        python_versions = (
            PYTHON_VERSIONS
            if python_versions is None
            else [Version(python_version) for python_version in python_versions]
        )
        airflow_versions = (
            AIRFLOW_2_VERSIONS + AIRFLOW_3_VERSIONS
            if airflow_versions is None
            else [Version(airflow_version) for airflow_version in airflow_versions]
        )

        async with anyio.create_task_group() as tg:
            for python_version in python_versions:
                for airflow_version in airflow_versions:
                    if airflow_version < Version('2.9.0') and python_version > Version('3.11'):
                        continue

                    tg.start_soon(
                        self.test_one_versions_combination,
                        source,
                        python_version.base_version,
                        airflow_version.base_version,
                    )

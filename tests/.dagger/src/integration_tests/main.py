import json
import typing as tp

import anyio
import dagger
from dagger import DefaultPath, Doc, dag, function, object_type
from packaging.version import Version

PYTHON_VERSIONS = [Version('3.10'), Version('3.11'), Version('3.12')]
DBT_VERSIONS = [
    Version('1.7'),
    Version('1.8'),
    Version('1.9'),
    Version('1.10'),
]
AIRFLOW_2_VERSIONS = [
    Version('2.6.3'),
    Version('2.7.3'),
    Version('2.8.4'),
    Version('2.9.3'),
    Version('2.10.5'),
    Version('2.11.0'),
]
AIRFLOW_3_VERSIONS = []

POETRY_VERSION = '1.8.5'


@object_type
class IntegrationTests:
    @staticmethod
    async def _get_all_available_package_versions(package_name: str) -> list[Version]:
        pip_index_stdout = await (
            dag.container()
            .from_('python:3.12-slim')
            .with_exec(['pip', 'install', '--upgrade', 'pip'])
            .with_exec(['pip', 'index', 'versions', '--json', package_name])
            .stdout()
        )
        if versions := json.loads(pip_index_stdout).get('versions'):
            return [Version(v) for v in versions]

        raise Exception(
            (
                f"Couldn't find any available versions for package {package_name}; "
                f'pip index output: {pip_index_stdout}'
            )
        )

    @staticmethod
    def _add_to_env_airflow(
        env: dagger.Container,
        airflow_version: Version,
    ) -> dagger.Container:
        pendulum_version = '2' if airflow_version < Version('2.8.0') else '3'

        env = (
            env
            # change apache-airflow package version in pyproject.toml to resolve other dependencies
            .with_exec(
                [
                    'poetry',
                    'add',
                    f'apache-airflow[fab,cncf-kubernetes]@{airflow_version}',
                    f'pendulum@^{pendulum_version}',
                ]
            )
        )

        # update pluggy version
        if airflow_version < Version('2.8.0'):
            env = env.with_exec(
                [
                    'poetry',
                    'add',
                    'pluggy@>=1.3.0',
                ]
            )

        return env

    async def _add_to_env_dbt(self, env: dagger.Container, dbt_version: Version) -> dagger.Container:
        _all_dbt_postgres_versions = await self._get_all_available_package_versions('dbt-postgres')
        dbt_postgres_versions = [
            v for v in _all_dbt_postgres_versions if v.major == dbt_version.major and v.minor == dbt_version.minor
        ]
        dbt_postgres_version = max(dbt_postgres_versions) if dbt_postgres_versions else max(_all_dbt_postgres_versions)

        return env.with_exec(
            [
                'poetry',
                'add',
                f'dbt-core@~{dbt_version}',
                f'dbt-postgres@{dbt_postgres_version}',
            ]
        )

    @function
    async def build_env(
        self,
        source: tp.Annotated[dagger.Directory, DefaultPath('/'), Doc('dbt-af source directory')],
        python_version: str,
    ) -> dagger.Container:
        """
        Prepare python requirements for dbt-af with pinned airflow, python and dbt version
        """

        python_version = Version(python_version)

        base = (
            dag.container()
            .from_(f'python:{python_version}-slim')
            # install curl
            .with_exec(['apt-get', 'update'])
            .with_exec(['apt-get', 'install', '--no-install-recommends', '-y', 'curl'])
            .with_exec(['pip', 'install', '--upgrade', 'pip'])
            # install poetry
            .with_exec(
                [
                    'bash',
                    '-lc',
                    f'curl -sSL https://install.python-poetry.org | POETRY_VERSION={POETRY_VERSION} python -',
                ]
            )
            .with_env_variable('PATH', '/root/.local/bin:$PATH', expand=True)
            .with_exec(['poetry', '--version'])
            # copy source code
            .with_directory('/dbt_af/dbt_af', source.directory('dbt_af'))
            .with_directory('/dbt_af/dbt_af_functional_tests', source.directory('dbt_af_functional_tests'))
            .with_directory('/dbt_af/scripts', source.directory('scripts'))
            .with_directory('/dbt_af/tests', source.directory('tests'))
            .with_file('/dbt_af/pyproject.toml', source.file('pyproject.toml'))
            .with_file('/dbt_af/README.md', source.file('README.md'))
            .with_workdir('/dbt_af')
            # pin python version
            .with_exec(
                [
                    'sed',
                    '-i',
                    f's/^python = "[^"]*"/python = "~{python_version.base_version}"/',
                    'pyproject.toml',
                ]
            )
            # resolve current dependencies
            .with_exec(['poetry', 'lock', '--no-update'])
            .with_exec(['poetry', 'install', '--with', 'dev', '--all-extras'])
        )

        if python_version < Version('3.12'):
            base = base.with_exec(
                [
                    'sed',
                    '-i',
                    r'/^setuptools = {.*python *= *">=3\.12".*}/d',
                    'pyproject.toml',
                ]
            )

        return base

    @function
    async def test_one_versions_combination(
        self,
        source: tp.Annotated[dagger.Directory, DefaultPath('/'), Doc('dbt-af source directory')],
        python_version: str,
        airflow_version: str,
        dbt_version: str,
        prebuild_env: dagger.Container | None = None,
        with_running_airflow_tasks: bool = False,
    ) -> str:
        python_version = Version(python_version)
        airflow_version = Version(airflow_version)
        dbt_version = Version(dbt_version)

        if prebuild_env:
            env = prebuild_env
        else:
            env = await self._add_to_env_dbt(
                self._add_to_env_airflow(
                    await self.build_env(
                        source,
                        python_version.base_version,
                    ),
                    airflow_version,
                ),
                dbt_version,
            )

        # postgres database from dbt tests
        postgres = (
            dag.container()
            .from_('postgres:16')
            .with_env_variable('POSTGRES_USER', 'postgres')
            .with_env_variable('POSTGRES_PASSWORD', 'postgres')
            .with_env_variable('POSTGRES_DB', 'postgres')
            .with_file(
                '/docker-entrypoint-initdb.d/init.sql',
                source.file('./tests/.dagger/src/integration_tests/init_test_db.sql'),
            )
            .with_exposed_port(5432)
            .as_service(use_entrypoint=True)
        )

        return await (
            env.with_workdir('/dbt_af')
            .with_service_binding('db', postgres)
            # download apache-airflow official constraints
            .with_exec(
                [
                    'curl',
                    '-s',
                    '-o',
                    'airflow-constraints.txt',
                    f'https://raw.githubusercontent.com/apache/airflow/constraints-{airflow_version}/constraints-{python_version}.txt',
                ],
            )
            # remove croniter from airflow constraints
            .with_exec(
                [
                    'sed',
                    '-i',
                    r'/^croniter==*/d',
                    'airflow-constraints.txt',
                ]
            )
            # install apache-airflow with provided constraints
            .with_exec(
                [
                    'poetry',
                    # 'add',
                    'run',
                    'pip',
                    'install',
                    f'apache-airflow[fab,cncf-kubernetes]=={airflow_version}',
                    '-c',
                    'airflow-constraints.txt',
                ]
            )
            .with_exec(
                [
                    'poetry',
                    'run',
                    'airflow',
                    'db',
                    'migrate' if airflow_version >= Version('2.7.0') else 'init',
                ]
            )
            .with_exec(
                [
                    'poetry',
                    'run',
                    'pytest',
                    '-qsxvv',
                    '--log-cli-level=INFO',
                ]
                + (
                    [
                        '--run-airflow-tasks',
                    ]
                    if with_running_airflow_tasks
                    else []
                )
                + [
                    'tests',
                ]
            )
            .stdout()
        )

    @function
    async def all(
        self,
        source: tp.Annotated[dagger.Directory, DefaultPath('/'), Doc('dbt-af source directory')],
        python_versions: list[str] | None = None,
        airflow_versions: list[str] | None = None,
        dbt_versions: list[str] | None = None,
        with_running_airflow_tasks: bool = False,
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
        dbt_versions = DBT_VERSIONS if dbt_versions is None else [Version(dbt_version) for dbt_version in dbt_versions]

        for python_version in python_versions:
            python_env = await self.build_env(source, python_version.base_version)
            for airflow_version in airflow_versions:
                if airflow_version < Version('2.9.0') and python_version > Version('3.11'):
                    continue
                airflow_env = self._add_to_env_airflow(python_env, airflow_version)
                async with anyio.create_task_group() as tg:
                    for dbt_version in dbt_versions:
                        dbt_env = await self._add_to_env_dbt(airflow_env, dbt_version)

                        tg.start_soon(
                            self.test_one_versions_combination,
                            source,
                            python_version.base_version,
                            airflow_version.base_version,
                            dbt_version.base_version,
                            dbt_env,
                            with_running_airflow_tasks,
                        )

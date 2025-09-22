import json
import typing as tp
import uuid

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

UV_VERSION = '0.8.19'


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
            f"Couldn't find any available versions for package {package_name}; pip index output: {pip_index_stdout}"
        )

    async def _add_to_env_dbt(self, env: dagger.Container, dbt_version: Version) -> dagger.Container:
        _all_dbt_core_versions = await self._get_all_available_package_versions('dbt-core')
        _all_dbt_postgres_versions = await self._get_all_available_package_versions('dbt-postgres')

        dbt_core_versions = [
            v for v in _all_dbt_core_versions if v.major == dbt_version.major and v.minor == dbt_version.minor
        ]
        dbt_core_version = max(dbt_core_versions) if dbt_core_versions else max(_all_dbt_core_versions)
        dbt_postgres_versions = [
            v for v in _all_dbt_postgres_versions if v.major == dbt_version.major and v.minor == dbt_version.minor
        ]
        dbt_postgres_version = max(dbt_postgres_versions) if dbt_postgres_versions else max(_all_dbt_postgres_versions)

        return env.with_exec(
            [
                'uv',
                'add',
                f'dbt-core=={dbt_core_version}',
            ]
        ).with_exec(
            [
                'uv',
                'add',
                '--group=dev',
                f'dbt-postgres=={dbt_postgres_version}',
            ]
        )

    @staticmethod
    def _install_all_requirements(
        env: dagger.Container,
        python_version: Version,
        airflow_version: Version,
    ) -> dagger.Container:
        pendulum_version = 2 if airflow_version < Version('2.8.0') else 3

        return (
            env.with_workdir('/dbt_af')
            # remove airflow from main project dependencies to install it from constraints
            .with_exec(['uv', 'remove', 'apache-airflow'])
            # add pendulum and pluggy to main project dependencies
            .with_exec(
                [
                    'uv',
                    'add',
                    # pendulum3 has breaking changes for airflow<2.8.0
                    f'pendulum>={pendulum_version},<{pendulum_version + 1}',
                    # old pluggy version is incompatible with tests
                    'pluggy>=1.3.0',
                ]
            )
            .with_exec(['uv', 'lock'])
            .with_exec(
                [
                    'uv',
                    'export',
                    '--all-extras',
                    '--dev',
                    '--no-editable',
                    '--output-file=requirements.txt',
                ]
            )
            # install airflow from official constraints
            .with_exec(
                [
                    'uv',
                    'pip',
                    'install',
                    '--system',
                    '-c',
                    f'https://raw.githubusercontent.com/apache/airflow/constraints-{airflow_version}/constraints-{python_version}.txt',
                    f'apache-airflow[fab,cncf-kubernetes]=={airflow_version}',
                ]
            )
            .with_exec(
                [
                    'uv',
                    'pip',
                    'install',
                    '--system',
                    '-r',
                    'requirements.txt',
                ]
            )
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
            .with_exec(['apt-get', 'install', '--no-install-recommends', '-y', 'curl', 'vim'])
            .with_exec(['pip', 'install', '--upgrade', 'pip'])
            # install uv
            .with_exec(
                [
                    'curl',
                    '-LsSf',
                    '-o',
                    '/install_uv.sh',
                    f'https://astral.sh/uv/{UV_VERSION}/install.sh',
                ],
            )
            .with_exec(['sh', '/install_uv.sh'])
            .with_env_variable('PATH', '$PATH:/root/.local/bin', expand=True)
            .with_exec(['uv', '--version'])
            # copy source code
            .with_directory('/dbt_af/dbt_af', source.directory('dbt_af'))
            .with_directory('/dbt_af/dbt_af_functional_tests', source.directory('dbt_af_functional_tests'))
            .with_directory('/dbt_af/scripts', source.directory('scripts'))
            .with_directory('/dbt_af/tests', source.directory('tests'))
            .with_file('/dbt_af/pyproject.toml', source.file('pyproject.toml'))
            .with_file('/dbt_af/README.md', source.file('README.md'))
            # pin python version
            .with_new_file('/dbt_af/.python-version', contents=python_version.base_version)
            .with_workdir('/dbt_af')
        )

        return base

    async def _test_one_versions_combination_impl(
        self,
        source: tp.Annotated[dagger.Directory, DefaultPath('/'), Doc('dbt-af source directory')],
        python_version: str,
        airflow_version: str,
        dbt_version: str,
        prebuild_env: dagger.Container | None = None,
        with_running_airflow_tasks: bool = False,
        limiter: anyio.CapacityLimiter | None = None,
    ) -> str:
        limiter = limiter or anyio.CapacityLimiter(1)

        async with limiter:
            python_version = Version(python_version)
            airflow_version = Version(airflow_version)
            dbt_version = Version(dbt_version)

            if prebuild_env:
                env = prebuild_env
            else:
                env = self._install_all_requirements(
                    await self._add_to_env_dbt(
                        await self.build_env(
                            source,
                            python_version.base_version,
                        ),
                        dbt_version,
                    ),
                    python_version,
                    airflow_version,
                )

            # postgres database from dbt tests
            postgres = (
                dag.container()
                .from_('postgres:16')
                .with_env_variable('POSTGRES_USER', 'postgres')
                .with_env_variable('POSTGRES_PASSWORD', 'postgres')
                .with_env_variable('POSTGRES_DB', 'postgres')
                # unique instance id for each integration test
                .with_env_variable('INSTANCE_ID', uuid.uuid4().hex)
                .with_file(
                    '/docker-entrypoint-initdb.d/init.sql',
                    source.file('./tests/.dagger/src/integration_tests/init_test_db.sql'),
                )
                .with_exposed_port(5432)
                .as_service(args=['postgres', '-c', 'log_statement=all'], use_entrypoint=True)
            )

            return await (
                env.with_workdir('/dbt_af')
                .with_service_binding('db', postgres)
                # init airflow database
                .with_exec(
                    [
                        'airflow',
                        'db',
                        'migrate' if airflow_version >= Version('2.7.0') else 'init',
                    ]
                )
                .with_exec(
                    [
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
    async def test_one_versions_combination(
        self,
        source: tp.Annotated[dagger.Directory, DefaultPath('/'), Doc('dbt-af source directory')],
        python_version: str,
        airflow_version: str,
        dbt_version: str,
        prebuild_env: dagger.Container | None = None,
        with_running_airflow_tasks: bool = False,
    ) -> str:
        return await self._test_one_versions_combination_impl(
            source,
            python_version,
            airflow_version,
            dbt_version,
            prebuild_env,
            with_running_airflow_tasks,
        )

    @function
    async def all(
        self,
        source: tp.Annotated[dagger.Directory, DefaultPath('/'), Doc('dbt-af source directory')],
        python_versions: list[str] | None = None,
        airflow_versions: list[str] | None = None,
        dbt_versions: list[str] | None = None,
        with_running_airflow_tasks: bool = False,
        number_of_concurrent_tests: int = 2,
    ):
        limiter = anyio.CapacityLimiter(number_of_concurrent_tests)

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
            for airflow_version in airflow_versions:
                if airflow_version < Version('2.9.0') and python_version > Version('3.11'):
                    continue
                async with anyio.create_task_group() as tg:
                    for dbt_version in dbt_versions:
                        tg.start_soon(
                            self._test_one_versions_combination_impl,
                            source,
                            python_version.base_version,
                            airflow_version.base_version,
                            dbt_version.base_version,
                            None,
                            with_running_airflow_tasks,
                            limiter,
                        )

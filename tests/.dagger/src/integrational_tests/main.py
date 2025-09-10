import typing as tp

import anyio
import dagger
from dagger import DefaultPath, Doc, dag, function, object_type
from packaging.version import Version

PYTHON_VERSIONS = [Version('3.10'), Version('3.11'), Version('3.12')]
DBT_VERSIONS = [
    Version('1.7.19'),
    Version('1.8.9'),
    Version('1.9.10'),
    Version('1.10.11'),
]
AIRFLOW_2_VERSIONS = [
    Version('2.6.3'),
    Version('2.7.3'),
    Version('2.8.4'),
    Version('2.9.3'),
    Version('2.10.5'),
    Version('2.11.0'),
]
AIRFLOW_3_VERSIONS = [
    # Version('3.0.6'),
]

POETRY_VERSION = '1.8.5'


@object_type
class IntegrationalTests:
    @function
    async def prepare_python_requirements(
        self,
        source: tp.Annotated[dagger.Directory, DefaultPath('/'), Doc('dbt-af source directory')],
        python_version: str,
        airflow_version: str,
        dbt_version: str,
    ) -> dagger.Container:
        """
        Prepare python requirements for dbt-af with pinned airflow, python and dbt version
        """
        base = (
            dag.container()
            .from_(f'python:{python_version}-slim')
            # install curl
            .with_exec(['apt-get', 'update'])
            .with_exec(['apt-get', 'install', '--no-install-recommends', '-y', 'curl'])
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
        )

        python_version = Version((await base.with_exec(['python', '--version']).stdout()).split()[1])

        dbt_af_container = (
            base
            # copy source code
            .with_directory('/dbt_af/dbt_af', source.directory('dbt_af'))
            .with_directory('/dbt_af/dbt_af_functional_tests', source.directory('dbt_af_functional_tests'))
            .with_directory('/dbt_af/scripts', source.directory('scripts'))
            .with_directory('/dbt_af/tests', source.directory('tests'))
            .with_file('/dbt_af/pyproject.toml', source.file('pyproject.toml'))
            .with_file('/dbt_af/README.md', source.file('README.md'))
            .with_workdir('/dbt_af')
        )
        if python_version < Version('3.12'):
            dbt_af_container = dbt_af_container.with_exec(
                [
                    'sed',
                    '-i',
                    r'/^setuptools = {.*python *= *">=3\.12".*}/d',
                    'pyproject.toml',
                ]
            )

        try:
            await (
                dag.container()
                .from_(f'python:{python_version}-slim')
                .with_exec(['pip', 'install', f'dbt-postgres=={dbt_version}'])
                .stdout()
            )
            dbt_core_version, dbt_postgres_version = dbt_version, dbt_version
        except dagger.ExecError:
            dbt_core_version = dbt_version
            dbt_postgres_version = await (
                dag.container()
                .from_(f'python:{python_version}-slim')
                .with_exec(
                    [
                        'bash',
                        '-lc',
                        "pip index versions dbt-postgres 2>/dev/null | head -n 1 | awk '{print $2}' | tr -d '()'",
                    ]
                )
                .stdout()
            )

        return (
            dbt_af_container.with_workdir('/dbt_af')
            # pin python version
            .with_exec(
                [
                    'sed',
                    '-i',
                    f's/^python = "[^"]*"/python = "{python_version.base_version}"/',
                    'pyproject.toml',
                ]
            )
            .with_exec(['poetry', 'lock', '--no-update'])
            # install dependencies
            .with_exec(
                [
                    'poetry',
                    'add',
                    f'apache-airflow[fab,cncf-kubernetes]=={airflow_version}',
                    f'dbt-core@{dbt_core_version}',
                    f'dbt-postgres@{dbt_postgres_version}',
                ]
            )
            # export dependencies
            .with_exec(
                [
                    'poetry',
                    'export',
                    '--format',
                    'requirements.txt',
                    '--without-hashes',
                    '--with',
                    'dev',
                    '--all-extras',
                ],
                redirect_stdout='./requirements.compiled.txt',
            )
            .with_exec(['bash', '-lc', 'curl -LsSf https://astral.sh/uv/install.sh | sh'])
        )

    @function
    async def build_env(
        self,
        source: tp.Annotated[dagger.Directory, DefaultPath('/'), Doc('dbt-af source directory')],
        python_version: str,
        airflow_version: str,
        dbt_version: str,
    ) -> dagger.Container:
        python_cache = dag.cache_volume(f'python_cache-{python_version}-{airflow_version}-{dbt_version}')

        base: dagger.Container = (
            dag.container()
            .from_(f'python:{python_version}-slim')
            .with_user('root')
            .with_exec(['apt-get', 'update', '--allow-releaseinfo-change'])
            .with_exec(['apt-get', 'install', '--no-install-recommends', '-y', 'build-essential', 'libpq-dev', 'curl'])
            .with_exec(['pip', 'install', 'uv'])
        )

        requirements_env = await self.prepare_python_requirements(source, python_version, airflow_version, dbt_version)
        requirements = await requirements_env.file('requirements.compiled.txt')

        env = (
            base.with_mounted_cache('$HOME/.local', python_cache, expand=True)
            .with_directory('/dbt_af/dbt_af', source.directory('dbt_af'))
            .with_directory('/dbt_af/dbt_af_functional_tests', source.directory('dbt_af_functional_tests'))
            .with_directory('/dbt_af/scripts', source.directory('scripts'))
            .with_directory('/dbt_af/tests', source.directory('tests'))
            .with_file('/dbt_af/pyproject.toml', source.file('pyproject.toml'))
            .with_file('/dbt_af/poetry.lock', source.file('poetry.lock'))
            .with_file('/dbt_af/README.md', source.file('README.md'))
            .with_file('/dbt_af/requirements.txt', requirements)
            .with_workdir('/dbt_af')
            # First install the package without extras to avoid conflicts
            # Install constrained dependencies for the specific Airflow version
            .with_exec(['uv', 'pip', 'install', '--system', '-r', 'requirements.txt'])
            .with_env_variable('PYTHONPATH', '/dbt_af:$PYTHONPATH', expand=True)
        )

        return env

    @function
    async def test_one_versions_combination(
        self,
        source: tp.Annotated[dagger.Directory, DefaultPath('/'), Doc('dbt-af source directory')],
        python_version: str,
        airflow_version: str,
        dbt_version: str,
        with_running_airflow_tasks: bool = False,
    ) -> str:
        env = await self.build_env(
            source=source,
            python_version=python_version,
            airflow_version=airflow_version,
            dbt_version=dbt_version,
        )
        return await (
            env.with_workdir('/dbt_af')
            .terminal()
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

        async with anyio.create_task_group() as tg:
            for python_version in python_versions:
                for airflow_version in airflow_versions:
                    for dbt_version in dbt_versions:
                        if airflow_version < Version('2.9.0') and python_version > Version('3.11'):
                            continue

                        tg.start_soon(
                            self.test_one_versions_combination,
                            source,
                            python_version.base_version,
                            airflow_version.base_version,
                            dbt_version.base_version,
                            with_running_airflow_tasks,
                        )

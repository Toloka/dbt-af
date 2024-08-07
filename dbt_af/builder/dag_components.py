from collections import defaultdict
from typing import Generator, Optional

from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from dbt_af.builder.domain_dag import DomainDag
from dbt_af.builder.task_dependencies import DagDelayedDependencyRegistry
from dbt_af.common.scheduling import ScheduleTag
from dbt_af.operators.branch import DbtBranchOperator, create_decision_path_function
from dbt_af.operators.kubernetes_pod import DbtKubernetesPodOperator
from dbt_af.operators.run import DbtRun, DbtSeed, DbtSnapshot, DbtTest
from dbt_af.operators.sensors import AfExecutionDateFn, DbtExternalSensor, DbtSourceFreshnessSensor
from dbt_af.operators.supplemental import TableauExtractsRefreshOperator
from dbt_af.parser.dbt_node_model import DbtNode, DbtNodeConfig
from dbt_af.parser.dbt_profiles import KubernetesTarget
from dbt_af.parser.dbt_source_model import DbtSource


class DagComponent:
    add_external_dependencies = True

    def __init__(self, name: str, domain_dag: DomainDag, node_config: DbtNodeConfig):
        self.name = name
        self.domain_dag = domain_dag
        self.node_config = node_config

        # dbt-af components
        self._depends_on: set[DagComponent] = set()
        self._depends_on_sources: set[DbtSource] = set()
        self._domains_dependencies: dict[DomainDag, set[DagComponent]] = defaultdict(set)
        self.delayed_deps_registry = DagDelayedDependencyRegistry()
        self._small_tests: set[str] = set()

        # airflow components
        self.af_component: Optional[DbtRun | DbtKubernetesPodOperator | TaskGroup] = None
        self.model_task: Optional[DbtRun | DbtKubernetesPodOperator] = None
        self.task_group: Optional[TaskGroup] = None
        self.af_sensor_endpoint: Optional[EmptyOperator | DbtRun | DbtKubernetesPodOperator] = None
        self._af_callbacks: dict[str, list[Optional[callable]]] = {}

    @property
    def depends_on(self) -> list['DagComponent']:
        return list(self._depends_on)

    @property
    def safe_name(self) -> str:
        return self.name.replace('.', '__')

    def add_af_callbacks(self, callbacks: dict[str, list[Optional[callable]]]):
        self._af_callbacks.update(callbacks)

    def add_dependency(self, dep: 'DagComponent'):
        if self.node_config.dependencies[dep.name].skip:
            return

        self._depends_on.add(dep)
        if self.domain_dag.config.model_dependencies.wait_policy.per_domain:
            self._domains_dependencies[dep.domain_dag].add(dep)

    def add_source_dependency(self, dep: DbtSource):
        self._depends_on_sources.add(dep)

    def add_small_test(self, resource_name: str):
        self._small_tests.add(resource_name)

    def _create_opt_brancher(self, delayed_deps: DagDelayedDependencyRegistry) -> Optional[DbtBranchOperator]:
        """
        Create a brancher task to decide if the model should be run or not based on the enable_from_dttm and
        disable_from_dttm parameters
        """
        if not self.node_config.enable_from_dttm and not self.node_config.disable_from_dttm:
            return None

        brancher = DbtBranchOperator(
            task_id=self.safe_name,
            task_group=self.task_group,
            python_callable=create_decision_path_function(self.node_config, self.safe_name),
            dag=self.domain_dag.af_dag,
        )
        delayed_deps(brancher) >> delayed_deps(self.model_task)

        return brancher

    def _ext_dep_waits_generator(
        self,
        dep: 'DagComponent',
        task_group: TaskGroup,
    ) -> Generator[DbtExternalSensor, None, None]:
        execution_date_fns = AfExecutionDateFn(
            upstream_schedule_tag=dep.domain_dag.schedule,
            downstream_schedule_tag=self.domain_dag.schedule,
            wait_policy=self.node_config.dependencies[dep.name].wait_policy,
        ).get_execution_dates()

        for i, execution_date_fn in enumerate(execution_date_fns):
            # airflow task id must be less than 250 chars. It's not necessary to have a long name for the only
            # one external dependency wait
            _suffix = f'__{i}' if len(execution_date_fns) > 1 else ''
            wait = DbtExternalSensor(
                dbt_af_config=self.domain_dag.config,
                task_id=f'wait__{dep.safe_name}{_suffix}',
                task_group=task_group,
                external_dag_id=dep.domain_dag.af_dag.dag_id,
                external_task_id=dep.af_sensor_endpoint.task_id,
                execution_date_fn=execution_date_fn,
                dep_schedule=dep.domain_dag.schedule,
                dag=self.domain_dag.af_dag,
            )
            yield wait

    def _is_external_dep_valid(self, dep: 'DagComponent') -> bool:
        return (
            dep.domain_dag != self.domain_dag
            and self.add_external_dependencies
            and dep.domain_dag.schedule != ScheduleTag.manual()
            and self.domain_dag.schedule != ScheduleTag.manual()
        )

    def _init_dependencies_per_domain_af(self, delayed_deps: DagDelayedDependencyRegistry):
        for dep_domain_dag, deps in self._domains_dependencies.items():
            for dep in deps:
                if not self._is_external_dep_valid(dep):
                    continue

                deps_registry = self.domain_dag.registered_domains_dependencies[dep_domain_dag]
                if not deps_registry.is_registered(dep):
                    if not deps_registry.task_group:
                        deps_registry.task_group = TaskGroup(
                            group_id=f'{dep_domain_dag.dag_name}__dependencies__group',
                            dag=self.domain_dag.af_dag,
                        )
                    for wait in self._ext_dep_waits_generator(dep, deps_registry.task_group):
                        deps_registry.add_dependency(dep, wait)

                for wait_task in deps_registry.get_dependency_wait_task(dep):
                    delayed_deps(wait_task) >> delayed_deps(self.model_task)

    def _init_dependencies_per_task_af(
        self,
        delayed_deps: DagDelayedDependencyRegistry,
        brancher: Optional[DbtBranchOperator],
    ):
        for dep in self._depends_on:
            if not self._is_external_dep_valid(dep):
                continue

            for wait in self._ext_dep_waits_generator(dep, self.af_component):
                delayed_deps(brancher) >> delayed_deps(wait)
                delayed_deps(wait) >> delayed_deps(self.model_task)

    def _init_dependencies_af(self, delayed_deps: DagDelayedDependencyRegistry):
        """
        Create dependencies between the model and its upstreams. If the model has external dependencies, create
        external sensor tasks to wait for the upstreams to finish
        """
        brancher = self._create_opt_brancher(delayed_deps=delayed_deps)

        for dep in self._depends_on:
            if dep.af_component is None:
                dep.init_af()

            if dep.domain_dag == self.domain_dag:
                delayed_deps(dep.model_task) >> delayed_deps(self.model_task)
                delayed_deps(dep.af_component) >> delayed_deps(self.af_component)
                delayed_deps(dep.model_task) >> delayed_deps(brancher)

        if self.domain_dag.config.model_dependencies.wait_policy.per_domain:
            self._init_dependencies_per_domain_af(delayed_deps)
        elif self.domain_dag.config.model_dependencies.wait_policy.per_task:
            self._init_dependencies_per_task_af(delayed_deps, brancher)
        else:
            raise ValueError(
                f'Unknown wait policy (or all policies are turned off): '
                f'{self.domain_dag.config.model_dependencies.wait_policy}'
            )

    def _get_ext_deps(self) -> list['DagComponent']:
        return [dep for dep in self._depends_on if self._is_external_dep_valid(dep)]

    def _get_source_deps_with_freshness_check(self) -> list[DbtSource]:
        return [dep for dep in self._depends_on_sources if dep.need_to_check_freshness()]

    def _create_task_group(self) -> Optional[TaskGroup]:
        """
        Create a task group for the model if it has external dependencies or small tests. If waits for all external
        dependencies are built per domain, a task group is not needed
        """
        if (
            not self._small_tests
            and (not self._get_ext_deps() or self.domain_dag.config.model_dependencies.wait_policy.per_domain)
            and not self._get_source_deps_with_freshness_check()
            and not self.node_config.enable_from_dttm
            and not self.node_config.disable_from_dttm
            and not self.node_config.tableau_refresh_tasks
        ):
            return None

        return TaskGroup(f'{self.safe_name}__group', dag=self.domain_dag.af_dag)

    def init_af(self):
        raise NotImplementedError

    def __hash__(self) -> int:
        return hash(f'{self.name}@{self.domain_dag.dag_name}')

    def __eq__(self, other) -> bool:
        if isinstance(other, DagComponent):
            return self.name == other.name and self.domain_dag == other.domain_dag
        raise TypeError(f'Cannot compare {self} with {other}')

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.name}, {self.domain_dag})'


class DagModel(DagComponent):
    runner_class = DbtRun
    add_external_dependencies = True
    overlap = True

    def __init__(self, dbt_node: DbtNode, domain_dag: DomainDag):
        super().__init__(dbt_node.resource_name, domain_dag, node_config=dbt_node.config)

        self.dbt_node = dbt_node
        self.target_environment = self.dbt_node.target_environment(domain_dag.config.dbt_default_targets)
        self.max_active_tis_per_dag = self.dbt_node.get_airflow_parallelism()

    def _create_dbt_runner_task(self) -> DbtRun:
        return self.runner_class(
            task_id=self.safe_name,
            model_name=self.name,
            dag=self.domain_dag.af_dag,
            task_group=self.task_group,
            schedule_tag=self.domain_dag.schedule,
            overlap=self.overlap,
            max_active_tis_per_dag=self.max_active_tis_per_dag,
            model_type=self.dbt_node.model_type,
            target_environment=self.target_environment,
            dbt_af_config=self.domain_dag.config,
            **self._af_callbacks,
        )

    def _create_k8s_runner_task(self) -> DbtKubernetesPodOperator:
        """
        Create a k8s operator to run the dbt model not in Databricks but in a k8s pod
        It's used only in rare cases when model requires a lot of resources and/or data processing on the same node
        """
        return DbtKubernetesPodOperator(
            task_id=self.safe_name,
            dbt_model_name=self.name,
            dbt_model_path=self.dbt_node.path,
            task_group=self.task_group,
            dag=self.domain_dag.af_dag,
            target_details=self.dbt_node.target_details,
            dbt_af_config=self.domain_dag.config,
        )

    def _create_runner_task(self) -> DbtRun | DbtKubernetesPodOperator:
        if isinstance(self.dbt_node.target_details, KubernetesTarget):
            return self._create_k8s_runner_task()

        return self._create_dbt_runner_task()

    def _init_small_tests_af(self, delayed_deps: DagDelayedDependencyRegistry) -> Optional[EmptyOperator]:
        """
        Create small tests for the model if it has any. If there are any tests, they will be run after the model and
        after all tests are finished, the empty endpoint task will be created
        """
        if not self._small_tests:
            return None

        endpoint_task = EmptyOperator(
            task_id=f'{self.safe_name}__end',
            task_group=self.task_group,
            dag=self.domain_dag.af_dag,
        )
        for test in self._small_tests:
            test_task = DbtTest(
                task_id=test.replace('.', '__'),
                model_name=test,
                dag=self.domain_dag.af_dag,
                task_group=self.task_group,
                schedule_tag=self.domain_dag.schedule,
                dbt_af_config=self.domain_dag.config,
            )
            delayed_deps(self.model_task) >> delayed_deps(test_task)
            delayed_deps(test_task) >> delayed_deps(endpoint_task)

        return endpoint_task

    def _init_source_dependencies_af(self, delayed_deps: DagDelayedDependencyRegistry):
        for source_dep in self._depends_on_sources:
            if source_dep.need_to_check_freshness():
                source_wait = DbtSourceFreshnessSensor(
                    task_id=f'wait_freshness__{source_dep.name}__for__{self.safe_name}',
                    task_group=self.af_component,
                    dag=self.domain_dag.af_dag,
                    env=self.model_task.env,
                    source_name=source_dep.source_name,
                    source_identifier=source_dep.identifier,
                    dbt_af_config=self.domain_dag.config,
                )

                delayed_deps(source_wait) >> delayed_deps(self.model_task)

    def _init_supplemental_dependencies_af(self, delayed_deps: DagDelayedDependencyRegistry):
        if self.dbt_node.config.tableau_refresh_tasks:
            tableau_refresh_task = TableauExtractsRefreshOperator(
                task_id=f'tableau_refresh__{self.safe_name}',
                task_group=self.task_group,
                dag=self.domain_dag.af_dag,
                tableau_refresh_tasks=self.dbt_node.config.tableau_refresh_tasks,
                dbt_af_config=self.domain_dag.config,
            )
            delayed_deps(self.model_task) >> delayed_deps(tableau_refresh_task)

    def init_af(self):
        """
        Initialize all Airflow components for the dbt-model and it's dependencies
        """
        if self.domain_dag.af_dag is None:
            raise ValueError(f'{self!r}: dag not set')

        with self.delayed_deps_registry as delayed_deps:
            self.task_group = self._create_task_group()
            self.model_task = self._create_runner_task()
            endpoint_task = self._init_small_tests_af(delayed_deps)

            self.af_component = self.task_group or self.model_task
            self.af_sensor_endpoint = endpoint_task or self.model_task

            self._init_dependencies_af(delayed_deps)
            self._init_source_dependencies_af(delayed_deps)
            self._init_supplemental_dependencies_af(delayed_deps)


class DagSnapshot(DagModel):
    runner_class = DbtSnapshot


class DagSeed(DagModel):
    runner_class = DbtSeed


class MediumTests(DagComponent):
    def __init__(self, domain_dag: DomainDag, node_config: DbtNodeConfig):
        name = f'medium_tests__{domain_dag.dag_name}'
        super().__init__(name, domain_dag, node_config=node_config)
        self._tests: set[str] = set()

    @staticmethod
    def get_medium_test_name(node: DbtNode, parent_model: DagModel) -> str:
        return f'{parent_model.safe_name}__{node.resource_name}'

    def add_test(self, node_id: str):
        self._tests.add(node_id)

    def init_af(self):
        with self.delayed_deps_registry as delayed_deps:
            self.af_component = TaskGroup(self.safe_name, dag=self.domain_dag.af_dag)

            for test in self._tests:
                DbtTest(
                    task_id=test.replace('.', '__'),
                    model_name=test,
                    task_group=self.af_component,
                    dag=self.domain_dag.af_dag,
                    schedule_tag=self.domain_dag.schedule,
                    dbt_af_config=self.domain_dag.config,
                )

            for dep in self.depends_on:
                if dep.af_component is None:
                    dep.init_af()

                delayed_deps(dep.af_component) >> delayed_deps(self.af_component)


class LargeTest(DagComponent):
    add_external_dependencies = True

    def __init__(self, name: str, domain_dag: DomainDag, node_config: DbtNodeConfig):
        super().__init__(name, domain_dag, node_config=node_config)

    def init_af(self):
        with self.delayed_deps_registry as delayed_deps:
            self.task_group = self._create_task_group()

            self.model_task = DbtTest(
                task_id=self.safe_name,
                model_name=self.name,
                dag=self.domain_dag.af_dag,
                task_group=self.task_group,
                schedule_tag=self.domain_dag.schedule,
                dbt_af_config=self.domain_dag.config,
            )
            self.af_component = self.task_group or self.model_task

            self._init_dependencies_af(delayed_deps)

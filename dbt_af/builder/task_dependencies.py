import typing as tp
from collections import defaultdict

from airflow.models.baseoperator import BaseOperator
from airflow.utils.task_group import TaskGroup

if tp.TYPE_CHECKING:
    from dbt_af.builder.dag_components import DagComponent


class DagDelayedDependency:
    """
    Delayed fixed pair of two tasks in Airflow DAG that have dependency between.
    It works because in Airflow it's possible to set dependency between tasks at any time
    after tasks and Dag are created.
    """

    def __init__(self, upstream: BaseOperator, downstream: BaseOperator):
        self.upstream = upstream
        self.downstream = downstream

    def resolve(self):
        self.upstream.set_downstream(self.downstream)


class DagDelayedDependencyStream:
    """
    Helper class to create delayed dependency between two tasks in Airflow DAG.
    It represents a dependency between two tasks in Airflow DAG using >> and << operators.
    """

    def __init__(self, stream: BaseOperator, registry):
        self.stream = stream
        self.registry = registry

    def __rshift__(self, other: 'DagDelayedDependencyStream'):
        if not isinstance(other, DagDelayedDependencyStream):
            raise TypeError(f'Cannot create dependency between {self} and {other}')
        if self.stream is None or other.stream is None:
            return self

        self.registry.register(self.stream, other.stream)
        return self

    def __lshift__(self, other: 'DagDelayedDependencyStream'):
        if not isinstance(other, DagDelayedDependencyStream):
            raise TypeError(f'Cannot create dependency between {self} and {other}')
        if self.stream is None or other.stream is None:
            return self

        self.registry.register(other.stream, self.stream)
        return self


class DagDelayedDependencyRegistry:
    """
    Registry of delayed dependencies between tasks in Airflow DAG. It's used to create delayed dependencies and
    resolve them at the end of the DagComponent creation.
    Supports context manager interface to resolve dependencies at the exit of the context.
    Correctly resolves dependencies between TaskGroup >> TaskGroup and TaskGroup >> Task

    Example:
        with DagDelayedDependencyRegistry() as delayed_deps:
            delayed_deps(group1) >> delayed_deps(task2)
            delayed_deps(task1) >> delayed_deps(task2)
            delayed_deps(group1) >> delayed_deps(group2)

    It's the same as (at the end of the function):
        group1 >> group2  # this dependency will be resolved first
        group1 >> task2
        task1 >> task2
    """

    def __init__(self):
        self._registry: tp.List[DagDelayedDependency] = []

    def __call__(self, task: tp.Union[BaseOperator, TaskGroup]) -> DagDelayedDependencyStream:
        return DagDelayedDependencyStream(task, registry=self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.resolve_dependencies()

    def __iter__(self):
        return iter(self._registry)

    def register(self, upstream: BaseOperator, downstream: BaseOperator):
        self._registry.append(DagDelayedDependency(upstream, downstream))

    def _sort_dependencies(self):
        # HACK: https://github.com/apache/airflow/issues/16764#issuecomment-1015058864
        # we need to resolve TaskGroup >> TaskGroup dependencies first and then all other dependencies
        # it's necessary to avoid bug with inconsistent state of TaskGroup and dependencies between them.
        self._registry.sort(
            key=lambda d: (isinstance(d.upstream, TaskGroup)) + (isinstance(d.downstream, TaskGroup)),
            reverse=True,
        )

    def resolve_dependencies(self):
        self._sort_dependencies()
        for dep in self._registry:
            dep.resolve()


class RegistryDomainDependencies:
    def __init__(self):
        self._registry: tp.Dict['DagComponent', tp.List[BaseOperator]] = defaultdict(list)
        self.task_group: tp.Optional[TaskGroup] = None

    def __repr__(self):
        return f'{self.__class__.__name__}({self._registry})'

    def is_registered(self, component: 'DagComponent') -> bool:
        return component in self._registry

    def add_dependency(self, component: 'DagComponent', wait_task: BaseOperator) -> None:
        self._registry[component].append(wait_task)

    def get_dependency_wait_task(self, component: 'DagComponent') -> tp.List[BaseOperator]:
        return self._registry[component]

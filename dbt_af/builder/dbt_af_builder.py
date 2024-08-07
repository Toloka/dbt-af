from collections import defaultdict
from typing import Optional

import pendulum

from dbt_af.builder.backfill_dag_components import BackfillDagModel, BackfillDagSnapshot
from dbt_af.builder.dag_components import DagComponent, DagModel, DagSeed, DagSnapshot, LargeTest, MediumTests
from dbt_af.builder.domain_dag import BackfillDomainDag, DomainDag, DomainDagFactory, DomainDagType
from dbt_af.builder.maintenance_dag_components import MaintenanceDagComponent
from dbt_af.common.constants import DOMAIN_DAG_START_DATE_FMT
from dbt_af.conf import Config
from dbt_af.parser.dbt_node_model import DbtModelMaintenanceType, DbtNode
from dbt_af.parser.dbt_profiles import Profiles
from dbt_af.parser.dbt_source_model import DbtSource


class DagComponentFactory:
    @staticmethod
    def create(dbt_node: DbtNode, domain_dag: DomainDag, backfill: bool = False) -> DagComponent:
        if dbt_node.is_model():
            return DagModel(dbt_node, domain_dag) if not backfill else BackfillDagModel(dbt_node, domain_dag)
        if dbt_node.is_snapshot():
            return DagSnapshot(dbt_node, domain_dag) if not backfill else BackfillDagSnapshot(dbt_node, domain_dag)
        if dbt_node.is_large_test() and not backfill:
            return LargeTest(dbt_node.resource_name, domain_dag, dbt_node.config)
        if dbt_node.is_seed():
            return DagSeed(dbt_node, domain_dag)

        raise ValueError(f'Unknown node type: {dbt_node} for {"backfill" if backfill else ""} {domain_dag}')


class DomainDagsRegistry:
    def __init__(self, config: Config, dags_type: DomainDagType = DomainDagType.SCHEDULED):
        self.config = config
        self.dags_type = dags_type

        self._domain_dags: dict[str, DomainDag] = {}

    def clear(self):
        self._domain_dags = {}

    def _get_domain_dag_hash(self, dbt_node: DbtNode) -> str:
        if dbt_node.is_large_test():
            return f'{dbt_node.domain}__large_tests'
        if self.dags_type == DomainDagType.BACKFILL:
            return f'{dbt_node.domain}__bf'
        if self.dags_type == DomainDagType.SCHEDULED:
            return f'{dbt_node.domain}_{dbt_node.config.schedule.safe_name}'
        if self.dags_type == DomainDagType.MAINTENANCE:
            return f'{dbt_node.domain}__maintenance'

        raise TypeError(f'Unknown dag type: {self.dags_type}')

    def get(self, dbt_node: DbtNode) -> DomainDag:
        domain_dag_name = self._get_domain_dag_hash(dbt_node)

        if domain_dag_name not in self._domain_dags:
            self._domain_dags[domain_dag_name] = DomainDagFactory.create(
                dag_type=self.dags_type if not dbt_node.is_large_test() else DomainDagType.LARGE_TESTS,
                domain_name=dbt_node.domain,
                schedule=dbt_node.config.schedule,
                config=self.config,
            )

        return self._domain_dags[domain_dag_name]


class DbtAfGraph:
    def __init__(self, nodes: list[DbtNode], sources: list[DbtSource], config: Config):
        self.config = config

        self.dbt_nodes: list[DbtNode] = nodes
        self.dbt_sources: list[DbtSource] = sources

        # dbt-af components
        self._domain_dags_registry = DomainDagsRegistry(config=self.config)
        self._domain_bf_dags_registry = DomainDagsRegistry(config=self.config, dags_type=DomainDagType.BACKFILL)
        self._domain_maintenance_dags_registry = DomainDagsRegistry(
            config=self.config,
            dags_type=DomainDagType.MAINTENANCE,
        )
        self._models: dict[str, DagModel] = {}
        self._large_tests: dict[str, LargeTest] = {}
        self._dag_components_registry: dict[str, DagComponent] = {}
        self._medium_tests: dict[DomainDag, MediumTests] = {}
        self._maintenance_components: dict[DomainDag, dict[DbtModelMaintenanceType, MaintenanceDagComponent]] = (
            defaultdict(dict)
        )

        self.nodes: list[DagComponent] = []

    @classmethod
    def from_manifest(
        cls,
        manifest: dict,
        profiles: dict,
        project_profile_name: str,
        config: Config,
        etl_service_name: Optional[str] = None,
    ) -> 'DbtAfGraph':
        project_profile = Profiles(**profiles)[project_profile_name]

        nodes = []
        for node_info in manifest['nodes'].values():
            node = DbtNode(**node_info)
            node.set_target_details(project_profile, config.dbt_default_targets)
            if node.resource_type in ('test', 'model', 'snapshot', 'seed'):
                # TODO: add sensors for models in different etl services
                if etl_service_name and not node.is_at_etl_service(etl_service_name):
                    continue
                nodes.append(node)

        sources = [DbtSource(**source_info) for source_info in manifest['sources'].values()]

        graph = cls(nodes, sources, config)
        graph._build_dags()
        return graph

    def clear_registries(self):
        self._domain_dags_registry.clear()
        self._domain_bf_dags_registry.clear()
        self._models = {}
        self._large_tests = {}
        self._dag_components_registry = {}
        self._medium_tests = {}

    def _build_dags(self):
        dag_components = self._build_dag_components(self.dbt_nodes)
        self.clear_registries()
        backfill_dag_components = self._build_backfill_dag_components(self.dbt_nodes)

        self.nodes = dag_components + backfill_dag_components

    def _find_parent_node_for_test(self, node: DbtNode) -> DagModel | BackfillDomainDag:
        original_file_path = node.original_file_path_without_extension
        for upstream in node.depends_on:
            if self._models[upstream].dbt_node.original_file_path_without_extension == original_file_path:
                return self._models[upstream]

        raise ValueError(f'Could not find parent node for medium test {node.unique_id}')

    def _collect_all_models(self, nodes, backfill: bool = False) -> None:
        domain_dags_registry = self._domain_bf_dags_registry if backfill else self._domain_dags_registry
        for node in nodes:
            domain_dag = domain_dags_registry.get(node)
            try:
                self._dag_components_registry[node.unique_id] = DagComponentFactory.create(
                    dbt_node=node,
                    domain_dag=domain_dag,
                    backfill=backfill,
                )
            except ValueError:
                # node is not an independent dag component or should not be in dag
                continue

        self._models = {
            node.unique_id: self._dag_components_registry[node.unique_id]
            for node in nodes
            if node.is_model() or node.is_seed()
        }
        if not backfill:
            self._large_tests = {
                node.unique_id: self._dag_components_registry[node.unique_id] for node in nodes if node.is_large_test()
            }

    def _collect_maintenance_components(self) -> None:
        for model in self._models.values():
            if maintenance_types := model.dbt_node.get_required_maintenance_types():
                domain_dag = self._domain_maintenance_dags_registry.get(model.dbt_node)
                for maintenance_type in maintenance_types:
                    if maintenance_type not in self._maintenance_components[domain_dag]:
                        self._maintenance_components[domain_dag][maintenance_type] = MaintenanceDagComponent(
                            domain_dag=domain_dag,
                            maintenance_type=maintenance_type,
                            node_config=model.dbt_node.config,
                        )
                    self._maintenance_components[domain_dag][maintenance_type].add_model(model.dbt_node)

    def _resolve_dependencies(self, nodes: list[DbtNode], backfill: bool = False) -> None:
        sources: dict[str, DbtSource] = {source.unique_id: source for source in self.dbt_sources}

        # set dependencies for models and snapshots
        for node in nodes:
            if node.is_model() or node.is_snapshot():
                for upstream in node.depends_on:
                    self._models[node.unique_id].add_dependency(self._models[upstream])
                for upstream in node.depends_on_sources:
                    self._models[node.unique_id].add_source_dependency(sources[upstream])

            elif node.is_small_test():
                for upstream in node.depends_on:
                    self._models[upstream].add_small_test(node.resource_name)

            elif node.is_medium_test():
                parent_node = self._find_parent_node_for_test(node)
                parent_domain_dag = parent_node.domain_dag
                if parent_domain_dag not in self._medium_tests:
                    self._medium_tests[parent_domain_dag] = MediumTests(parent_domain_dag, parent_node.dbt_node.config)
                self._medium_tests[parent_domain_dag].add_test(MediumTests.get_medium_test_name(node, parent_node))

            elif node.is_large_test() and not backfill:
                # set dependencies for large tests only for regular scheduled dags
                for upstream in node.depends_on:
                    self._large_tests[node.unique_id].add_dependency(self._models[upstream])

    def _bind_medium_tests(self) -> None:
        """
        Bind medium tests to all model nodes in domain so medium tests will be executed after all models
        """
        for model in self._models.values():
            if model.domain_dag in self._medium_tests:
                self._medium_tests[model.domain_dag].add_dependency(model)

    def _build_dag_components(self, nodes: list[DbtNode]) -> list[DagComponent]:
        self._collect_all_models(nodes)
        self._collect_maintenance_components()

        self._resolve_dependencies(nodes)

        self._bind_medium_tests()

        return (
            list(self._models.values())
            + list(self._medium_tests.values())
            + list(self._large_tests.values())
            + list(*[maintenance.values() for maintenance in self._maintenance_components.values()])
        )

    def _build_backfill_dag_components(self, nodes: list[DbtNode]) -> list[DagComponent]:
        self._collect_all_models(nodes, backfill=True)

        self._resolve_dependencies(nodes, backfill=True)

        self._bind_medium_tests()

        return list(self._models.values()) + list(self._medium_tests.values())


def get_domain_dag_start_date(graph: DbtAfGraph, domain_dag: DomainDag) -> pendulum.datetime:
    min_date_from_nodes = min(
        node.node_config.domain_start_date for node in graph.nodes if node.domain_dag == domain_dag
    )
    if min_date_from_nodes:
        return pendulum.from_format(min_date_from_nodes, DOMAIN_DAG_START_DATE_FMT)
    return graph.config.dag_start_date

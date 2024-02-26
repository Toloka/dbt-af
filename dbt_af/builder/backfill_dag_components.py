from typing import TYPE_CHECKING

from dbt_af.builder.dag_components import DagModel
from dbt_af.operators.run import DbtRun, DbtSnapshot

if TYPE_CHECKING:
    from dbt_af.builder.dbt_af_builder import DomainDag
    from dbt_af.parser.dbt_node_model import DbtNode


class BackfillDagModel(DagModel):
    runner_class = DbtRun
    max_active_tis_per_dag = 1
    add_external_dependencies = False
    overlap = False

    def __init__(self, dbt_node: 'DbtNode', domain_dag: 'DomainDag'):
        super().__init__(dbt_node, domain_dag)
        self.target_environment = (
            self.dbt_node.config.bf_cluster or domain_dag.config.dbt_default_targets.default_backfill_target
        )

    @property
    def safe_name(self) -> str:
        return f'{super().safe_name}__bf'

    def __str__(self) -> str:
        return f'{self.name}__bf'

    def __hash__(self) -> int:
        return hash(f'{self.name}@bf@{self.domain_dag.dag_name}')

    def _get_ext_deps(self) -> list:
        return []


class BackfillDagSnapshot(BackfillDagModel):
    runner_class = DbtSnapshot

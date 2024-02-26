from airflow.utils.task_group import TaskGroup

from dbt_af.builder.dag_components import DagComponent
from dbt_af.builder.dbt_af_builder import DomainDag
from dbt_af.operators.macros import DbtMaintenanceOperatorFactory
from dbt_af.parser.dbt_node_model import DbtModelMaintenanceType, DbtNode, DbtNodeConfig


class MaintenanceDagComponent(DagComponent):
    def __init__(
        self,
        domain_dag: DomainDag,
        maintenance_type: DbtModelMaintenanceType,
        node_config: 'DbtNodeConfig',
    ):
        name = f'{maintenance_type.value}__{domain_dag.dag_name}'
        super().__init__(name=name, domain_dag=domain_dag, node_config=node_config)
        self.maintenance_type = maintenance_type

        self._model_names: set[DbtNode] = set()

    def __hash__(self):
        return hash(self.safe_name)

    def add_model(self, model_name: DbtNode):
        self._model_names.add(model_name)

    def init_af(self):
        self.af_component = TaskGroup(self.safe_name, dag=self.domain_dag.af_dag)

        for model_name in self._model_names:
            DbtMaintenanceOperatorFactory.create(
                model_name=model_name.resource_name,
                schedule_tag=self.domain_dag.schedule,
                maintenance_type=self.maintenance_type,
                task_group=self.af_component,
                af_dag=self.domain_dag.af_dag,
                maintenance_config=model_name.config.maintenance,
                dbt_af_config=self.domain_dag.config,
            )

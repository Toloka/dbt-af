from typing import TYPE_CHECKING, Any, Callable, List, Optional

from airflow.operators.python import BranchPythonOperator
from airflow.utils.context import Context

from dbt_af.parser.dbt_node_model import DbtNodeConfig

if TYPE_CHECKING:
    from airflow.models import DAG
    from airflow.utils.task_group import TaskGroup

DOWNSTREAM_TASK_IDS_KWARG = 'downstream_task_ids'


class DbtBranchOperator(BranchPythonOperator):
    def __init__(
        self, task_id: str, task_group: 'Optional[TaskGroup]', python_callable: Callable, dag: 'DAG', **kwargs
    ):
        task_id = f'{task_id}_branch' if not task_id.endswith('_branch') else task_id
        super().__init__(task_id=task_id, task_group=task_group, python_callable=python_callable, dag=dag, **kwargs)

    def execute(self, context: Context) -> Any:
        downstream_task_ids = [t.task_id for t in self.downstream_list]
        self.op_kwargs[DOWNSTREAM_TASK_IDS_KWARG] = downstream_task_ids
        return super().execute(context)


def create_decision_path_function(node_config: DbtNodeConfig, node_name: str) -> Callable:
    def decide_which_path(**kwargs) -> List[str]:
        is_enable = True
        if node_config.enable_from_dttm:
            if str(kwargs['data_interval_end']) < node_config.enable_from_dttm:
                is_enable = False
        is_disable = False
        if node_config.disable_from_dttm:
            if str(kwargs['data_interval_start']) > node_config.disable_from_dttm:
                is_disable = True
        if is_enable and not is_disable:
            downstream = [f'{node_name}__group.{node_name}']
            if DOWNSTREAM_TASK_IDS_KWARG in kwargs:
                downstream += kwargs[DOWNSTREAM_TASK_IDS_KWARG]
            return downstream

        return []

    return decide_which_path

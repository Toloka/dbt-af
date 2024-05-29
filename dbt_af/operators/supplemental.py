from typing import TYPE_CHECKING, Sequence

from airflow.operators.python import PythonOperator

from dbt_af.conf import Config
from dbt_af.integrations.tableau import is_tableau_installed, tableau_extracts_refresh

if TYPE_CHECKING:
    from dbt_af.parser.dbt_node_model import TableauRefreshTaskConfig


class TableauExtractsRefreshOperator(PythonOperator):
    template_fields: Sequence[str] = ('tableau_refresh_tasks',)
    template_fields_renderers = {'template_fields_renderers': 'py'}

    def __init__(self, tableau_refresh_tasks: 'list[TableauRefreshTaskConfig]', dbt_af_config: Config, **kwargs):
        if not is_tableau_installed():
            raise ImportError('tableauserverclient is not installed. Please install it to use this operator.')

        super().__init__(
            python_callable=tableau_extracts_refresh,
            op_kwargs={
                'tableau_refresh_tasks': tableau_refresh_tasks,
                'dbt_af_config': dbt_af_config,
            },
            **kwargs,
        )

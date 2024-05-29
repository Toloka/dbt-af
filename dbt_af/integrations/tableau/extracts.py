from functools import partial
from typing import TYPE_CHECKING

from cachetools import TTLCache, cachedmethod
from cachetools.keys import hashkey

from dbt_af.integrations.tableau.auth import get_tableau_auth_object
from dbt_af.integrations.tableau.exceptions import (
    FailedTableauRefreshTasksException,
    UnknownTableauResourceTypeException,
)
from dbt_af.parser.dbt_node_model import TableauRefreshResourceType, TableauRefreshTaskConfig

if TYPE_CHECKING:
    import tableauserverclient

    from dbt_af.conf import Config


class _TableauExtractsRegistry:
    def __init__(self, server: 'tableauserverclient.Server'):
        self.server = server

        self._cache: TTLCache = TTLCache(maxsize=1024, ttl=60 * 15)

    @cachedmethod(lambda self: self._cache, key=partial(hashkey, '_get_workbooks_id_mapping'))
    def _get_workbooks_id_mapping(self) -> dict[tuple[str, str], str]:
        import tableauserverclient as tsc

        workbooks_id_mapping: dict[tuple[str, str], str] = {}
        for wb in tsc.Pager(self.server.workbooks):
            workbooks_id_mapping[(wb.project_name, wb.name)] = wb.id

        return workbooks_id_mapping

    @cachedmethod(lambda self: self._cache, key=partial(hashkey, '_get_datasources_id_mapping'))
    def _get_datasources_id_mapping(self) -> dict[tuple[str, str], str]:
        import tableauserverclient as tsc

        datasources_id_mapping: dict[tuple[str, str], str] = {}
        for ds in tsc.Pager(self.server.datasources):
            datasources_id_mapping[(ds.project_name, ds.name)] = ds.id

        return datasources_id_mapping

    def get_resource_id(self, resource_type: TableauRefreshResourceType, project_name: str, resource_name: str) -> str:
        match resource_type:
            case TableauRefreshResourceType.workbook:
                return self._get_workbooks_id_mapping()[(project_name, resource_name)]
            case TableauRefreshResourceType.datasource:
                return self._get_datasources_id_mapping()[(project_name, resource_name)]
            case _:
                raise UnknownTableauResourceTypeException(f'Unknown resource type: {resource_type}')


def tableau_extracts_refresh(tableau_refresh_tasks: 'list[TableauRefreshTaskConfig]', dbt_af_config: 'Config') -> None:
    import tableauserverclient as tsc

    tableau_auth = get_tableau_auth_object(dbt_af_config)
    tableau_server = tsc.Server(server_address=dbt_af_config.tableau.server_address, use_server_version=True)
    tableau_server.auth.sign_in(tableau_auth)

    extracts_registry = _TableauExtractsRegistry(tableau_server)

    failed_tasks: list[TableauRefreshTaskConfig] = []
    for refresh_task in tableau_refresh_tasks:
        try:
            resource_id = extracts_registry.get_resource_id(
                refresh_task.resource_type,
                refresh_task.project_name,
                refresh_task.resource_name,
            )
            match refresh_task.resource_type:
                case TableauRefreshResourceType.workbook:
                    tableau_server.workbooks.refresh(resource_id)
                case TableauRefreshResourceType.datasource:
                    tableau_server.datasources.refresh(resource_id)
                case _:
                    raise UnknownTableauResourceTypeException(f'Unknown resource type: {refresh_task.resource_type}')
        except UnknownTableauResourceTypeException:
            failed_tasks.append(refresh_task)

    if not failed_tasks:
        return

    raise FailedTableauRefreshTasksException(f'Failed to refresh the following Tableau resources: {failed_tasks}')

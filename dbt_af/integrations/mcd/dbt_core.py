import logging
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from pycarlo.core import Client


def _get_resource_id(client: 'Client', metastore_name: str) -> str:
    """
    Get the resource ID by the metastore name

    copy function from https://github.com/mvfolino68/pycarlo-examples/blob/main/dbt/dbt.py#L16
    """

    from pycarlo.core import Query

    query = Query()
    query.get_user().account.warehouses.__fields__('name', 'connection_type', 'uuid')
    warehouses = client(query).get_user.account.warehouses
    for wh in warehouses:
        if wh.name == metastore_name:
            return wh.uuid
    raise ValueError(f'Warehouse {metastore_name} not found. Found: {warehouses}')


def send_dbt_artefacts_to_montecarlo(
    target_path: str,
    log_path: Optional[str],
    model_name: str,
    metastore_name: str,
    project_name: str,
):
    from airflow_mcd.hooks import SessionHook
    from pycarlo.core import Client
    from pycarlo.features.dbt import DbtImporter

    logging.info('Sending dbt artefacts to MonteCarlo')
    mc_client = Client(session=SessionHook(mcd_session_conn_id=SessionHook.default_conn_name).get_conn())
    resource_id = _get_resource_id(mc_client, metastore_name)

    DbtImporter(mc_client=mc_client).import_run(
        manifest_path=f'{target_path}/manifest.json',
        run_results_path=f'{target_path}/run_results.json',
        logs_path=log_path,
        project_name=project_name,
        job_name=f'dbt_run_{model_name}',
        resource_id=resource_id,
    )
    logging.info('Successfully sent dbt artefacts to MonteCarlo')

import json
from typing import Optional

import yaml
from airflow.models.dag import DAG
from airflow.models.param import Param

from dbt_af.builder.dbt_af_builder import BackfillDomainDag, DbtAfGraph, get_domain_dag_start_date
from dbt_af.common.af_callbacks import collect_af_custom_callbacks
from dbt_af.common.constants import DBT_MODEL_DAG_PARAM, DEFAULT_DAG_ARGS
from dbt_af.conf import Config
from dbt_af.operators.run import DbtRun


def dbt_main_dags(graph: DbtAfGraph) -> dict[str, DAG]:
    af_dags = {}

    dag_callbacks, task_callbacks = collect_af_custom_callbacks(graph.config)
    domains = {node.domain_dag for node in graph.nodes}

    for domain_dag in domains:
        dag = DAG(
            domain_dag.dag_name,
            start_date=get_domain_dag_start_date(graph, domain_dag),
            description=graph.config.af_dag_description,
            schedule=domain_dag.schedule.af_repr(),
            catchup=domain_dag.catchup if not graph.config.is_dev else False,
            default_args=DEFAULT_DAG_ARGS,
            max_active_runs=graph.config.max_active_dag_runs,
            render_template_as_native_obj=False,
            tags=['dbt'] + domain_dag.tags,
            **dag_callbacks,
        )
        domain_dag.af_dag = dag
        af_dags[domain_dag.dag_name] = dag

        if isinstance(domain_dag, BackfillDomainDag):
            domain_dag.wrap_dag_with_endpoints()

    for node in graph.nodes:
        node.domain_dag.af_dag = af_dags[node.domain_dag.dag_name]

    for node in graph.nodes:
        node.add_af_callbacks(task_callbacks)
        if node.af_component is None:
            node.init_af()

    for node in graph.nodes:
        if isinstance(node.domain_dag, BackfillDomainDag):
            start_task = node.domain_dag.start_endpoint
            if len(node.af_component.upstream_task_ids) == 0:
                start_task >> node.af_component

    return af_dags


def dbt_run_model_dag(config: Config) -> dict[str, DAG]:
    dag_name = 'dbt_run_model'

    dag_callbacks, task_callbacks = collect_af_custom_callbacks(config)
    dag = DAG(
        dag_name,
        start_date=config.dag_start_date,
        description=config.af_dag_description,
        schedule_interval=None,
        catchup=False,
        default_args=DEFAULT_DAG_ARGS,
        max_active_runs=config.max_active_dag_runs,
        tags=['dbt', 'system'],
        params={
            DBT_MODEL_DAG_PARAM: Param('', type='string'),
            'start_dttm': Param('2000-01-01T00:00:00', type='string'),
            'end_dttm': Param('2000-01-01T00:00:00', type='string'),
        },
        **dag_callbacks,
    )

    target_environment = config.dbt_default_targets.default_target
    DbtRun(
        task_id='dbt_model',
        model_name=None,
        dag=dag,
        target_environment=target_environment,
        dbt_af_config=config,
        **task_callbacks,
    )

    return {dag_name: dag}


def _compile_dbt_dags(
    manifest_content: dict,
    profiles: dict,
    project_profile_name: str,
    config: Config,
    etl_service_name: Optional[str] = None,
) -> dict[str, DAG]:
    dags = {}

    graph = DbtAfGraph.from_manifest(
        manifest_content, profiles, project_profile_name, etl_service_name=etl_service_name, config=config
    )

    dags.update(dbt_main_dags(graph))
    if config.include_single_model_manual_dag:
        dags.update(dbt_run_model_dag(config=config))

    return dags


def compile_dbt_af_dags(manifest_path: str, config: Config, etl_service_name: Optional[str] = None) -> dict[str, DAG]:
    """
    Compiles airflow DAGs from manifest according to provided dbt-af config.
    It's possible to use different etl service name for different models groups in one dbt project.
    """

    with open(manifest_path) as fin:
        manifest = json.load(fin)

    with open(config.dbt_project.dbt_project_path / 'profiles.yml') as fin:
        profiles = yaml.safe_load(fin)

    with open(config.dbt_project.dbt_project_path / 'dbt_project.yml') as fin:
        dbt_project_profile_name = yaml.safe_load(fin)['profile']

    return _compile_dbt_dags(
        manifest, profiles, dbt_project_profile_name, etl_service_name=etl_service_name, config=config
    )

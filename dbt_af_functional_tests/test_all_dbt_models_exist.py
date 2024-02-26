import unittest
from collections import defaultdict

from dbt_af.builder.dbt_af_builder import DbtAfGraph
from dbt_af.dags import dbt_main_dags
from dbt_af.parser.dbt_node_model import DbtNode


def _model_safe_name(node: DbtNode) -> str:
    return node.fqn[-1].replace('.', '__')


def _model_schedule_safe_name(schedule_name: str) -> str:
    return schedule_name.replace('@', '')


def check_if_all_dbt_models_are_in_dbt_af_graph(manifest, profiles, project_profile_name, config, etl=None):
    dbt_graph = DbtAfGraph.from_manifest(
        manifest=manifest,
        profiles=profiles,
        project_profile_name=project_profile_name,
        etl_service_name=etl,
        config=config,
    )
    dags = dbt_main_dags(dbt_graph)

    raw_dbt_nodes_by_domain_schedule = defaultdict(lambda: defaultdict(set))
    for raw_node in manifest['nodes'].values():
        node = DbtNode(**raw_node)
        if not node.is_at_etl_service(etl) or not node.is_model():
            continue

        raw_dbt_nodes_by_domain_schedule[node.domain][_model_schedule_safe_name(node.config.schedule.name)].add(
            _model_safe_name(node)
        )

    for dag_name, dag in dags.items():
        if (
            'dbt' not in dag.tags
            or 'backfill' in dag.tags
            or 'frontier' not in dag.tags
            or 'dbt_large_tests' in dag.tags
        ):
            continue

        domain_name, schedule, *_ = dag_name.split('__')
        task_ids = [
            task.task_id.split('.')[-1]
            for task in dag.tasks
            if task.operator_name not in ('DbtTest', 'EmptyOperator', 'DbtBranchOperator')
        ]
        task_ids_by_domain = [
            task_id for task_id in task_ids if task_id.startswith(domain_name) and not task_id.endswith('_increment')
        ]
        unittest.TestCase().assertSetEqual(
            raw_dbt_nodes_by_domain_schedule[domain_name][schedule],
            set(task_ids_by_domain),
            msg=f'{dag.dag_id}, {domain_name}',
        )


def test_check_if_all_dbt_models_are_in_dbt_af_graph(manifest, profiles, project_profile_name, config, get_etl_name):
    check_if_all_dbt_models_are_in_dbt_af_graph(manifest, profiles, project_profile_name, config, get_etl_name)

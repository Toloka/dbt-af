from dbt_af.parser.dbt_node_model import DbtNode


def test_dbt_node_model_from_manifest_does_not_crash(manifest):
    for node in manifest['nodes'].values():
        DbtNode(**node)

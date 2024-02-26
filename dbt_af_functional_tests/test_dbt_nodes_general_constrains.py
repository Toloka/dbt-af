from dbt_af.parser.dbt_node_model import DbtNode


def test_all_dbt_tests_have_unique_name(manifest):
    unique_tests_names = set()
    for raw_node in manifest['nodes'].values():
        node = DbtNode(**raw_node)
        if node.is_test():
            test_name = f'{node.name}'

            assert test_name not in unique_tests_names
            unique_tests_names.add(test_name)


def test_dbt_node_name_is_same_as_path(manifest):
    for raw_node in manifest['nodes'].values():
        node = DbtNode(**raw_node)
        if node.is_model():
            real_node_name = node.fqn[-1]
            expected_node_name = f'{node.fqn[1]}.{node.fqn[2]}.{node.name.split(".")[-1]}'
            assert real_node_name == expected_node_name


def test_all_medium_tests_have_parents(dbt_af_graph):
    for node in dbt_af_graph.dbt_nodes:
        if node.is_medium_test():
            dbt_af_graph._find_parent_node_for_test(node)

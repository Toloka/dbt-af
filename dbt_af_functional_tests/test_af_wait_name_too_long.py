def test_af_wait_name_is_less_250_chars(manifest):
    """
    Check that wait name is less than 250 chars. 5 chars are reserved for suffix with pattern __{dep_number}
    where 0 <= dep_number <= 999
    """

    for node in manifest['nodes'].values():
        if node['resource_type'] == 'model':
            for dep in node['depends_on']['nodes']:
                if dep.startswith('model'):
                    dep_name = manifest['nodes'][dep]['name']
                    dep_safe_name = dep_name.replace('.', '__')
                    dep_domain_name = dep_name.split('.')[0]
                    wait_name = f'{dep_domain_name}__scheduletag__dependencies__group.wait__{dep_safe_name}'
                    assert len(wait_name) <= 245
        elif node['resource_type'] == 'test':
            for dep in node['depends_on']['nodes']:
                if dep.startswith('model'):
                    model_name_safe_name = '.'.join(dep.split('.')[2:]).replace('.', '__')
                    wait_name = f"{model_name_safe_name}__group.{node['name']}"
                    assert len(wait_name) <= 250

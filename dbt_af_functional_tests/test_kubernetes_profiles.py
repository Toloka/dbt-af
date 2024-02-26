from dbt_af.parser.dbt_profiles import Profile


def test_kubernetes_profiles_have_correct_structure(profiles):
    for profile_name, profile in profiles.items():
        if profile_name == 'config':
            continue
        profile = Profile(**profile)
        for target_name, target in profile.outputs.items():
            if target.target_type == 'kubernetes':
                assert target.node_pool_selector_name is not None
                assert target.node_pool is not None
                assert target.image_name is not None
                assert target.pod_cpu_guarantee is not None
                assert target.pod_memory_guarantee is not None
                assert target.tolerations is not None

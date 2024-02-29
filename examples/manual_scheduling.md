# Manual DAGs
By default, `dbt-af` will apply to all models `@daily` schedule as it's the most common case. 
But in some cases, you may want to have a _manual_ scheduling for your ad-hoc domains or if you want to have external trigger for DAG.

To enable manual scheduling, you need to use `@manual` scheduling in your dbt models.

All models with `@manual` scheduling will appear in the `manual` DAG in Airflow with suffix `__manual`.

## List of Examples
1. [Basic Project](basic_project.md): a single domain, small tests, and a single target.
2. [Advanced Project](advanced_project.md): several domains, medium and large tests, and different targets.
3. [Dependencies management](dependencies_management.md): how to manage dependencies between models in different domains.
5. [Maintenance and source freshness](maintenance_and_source_freshness.md): how to manage maintenance tasks and source freshness.
6. [Kubernetes tasks](kubernetes_tasks.md): how to run dbt models in Kubernetes.
7. [Integration with other tools](integration_with_other_tools.md): how to integrate dbt-af with other tools.
8. [\[Preview\] Extras and scripts](extras_and_scripts.md): available extras and scripts.
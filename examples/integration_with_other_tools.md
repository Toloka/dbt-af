# Integrations with other tools
## Installation
Install `dbt-af` with extra `mcd`. Run `pip install dbt-af[mcd]`.

## Monte carlo Data

Follow the instructions to set up connections ([MCD docs](https://docs.getmontecarlo.com/docs/airflow)).

To set up integration of dbt runs in Airflow with your Monte Carlo Data account, you need to specify the following config:

```python
from dbt_af.conf import Config, MCDIntegrationConfig

config = Config(
    # ...
    mcd=MCDIntegrationConfig(
        # whether to enable provided callbacks for airflow DAGs and tasks by airflow_mcd package
        callbacks_enabled=True,
        # whether to enable dbt artifacts export to Monte Carlo Data
        artifacts_export_enabled=True,
        # whether to require dbt artifacts export to MCD to be successful
        success_required=True,
        # name of metastore in MCD
        metastore_name='name',
    )
    # ...
)
```

## List of Examples
1. [Basic Project](basic_project.md): a single domain, small tests, and a single target.
2. [Advanced Project](advanced_project.md): several domains, medium and large tests, and different targets.
3. [Dependencies management](dependencies_management.md): how to manage dependencies between models in different domains.
4. [Manual scheduling](manual_scheduling.md): domains with manual scheduling.
5. [Maintenance and source freshness](maintenance_and_source_freshness.md): how to manage maintenance tasks and source freshness.
6. [Kubernetes tasks](kubernetes_tasks.md): how to run dbt models in Kubernetes.
8. [\[Preview\] Extras and scripts](extras_and_scripts.md): available extras and scripts.
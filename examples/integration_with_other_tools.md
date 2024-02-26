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

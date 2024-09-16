# Integrations with other tools

## Table of Contents

- [Monte carlo Data](#monte-carlo-data)
    - [Installation](#installation)
    - [Configuration](#configuration)
- [Tableau](#tableau)
    - [Installation](#installation-1)
    - [Configuration](#configuration-1)

## Monte carlo Data

### Installation

Install `dbt-af` with extra `mcd`. Run `pip install dbt-af[mcd]`.

### Configuration

Follow the instructions to set up connections ([MCD docs](https://docs.getmontecarlo.com/docs/airflow)).

> [!NOTE]
> To send dbt artifacts to Monte Carlo Data, you need to configure a default connection in Airflow
> (not Gateway Connection).
>
> So, if you want to use Gateway Connection for airflow callbacks and dbt artifacts export to MCD, you need to
> specify two connections in Airflow.

To set up integration of dbt runs in Airflow with your Monte Carlo Data account, you need to specify the following
config:

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

## Tableau

_dbt-af_ provides a way to trigger Tableau extracts refresh after dbt runs.

### Installation

First you need to install extra dependencies running command: `pip install dbt-af[tableau]`.

### Configuration

In your _dbt-af_ config fill in the following section:

```python
from dbt_af.conf import Config, TableauIntegrationConfig

config = Config(
    # ...
    tableau=TableauIntegrationConfig(
        server_address='https://tableau.server.com',
        username='admin',
        password='admin',
        site_id='my_site',
    ),
)
```

Tableau client supports only basic authentication with username and password or personal access token.
To use personal access token, you need to specify `token_name` and `pat` fields in the config instead of `username`
and `password`.
See [PAT docs](https://help.tableau.com/current/server/en-us/security_personal_access_tokens.htm) for more information.

### Binding Tableau extracts with dbt models

To link your Tableau extracts with dbt models, you need to specify the following section in dbt-model's config:

```yaml
tableau_refresh_tasks:
  - resource_name: embedded_data_source
    project_name: project_name
    resource_type: workbook

  - resource_name: published_data_source_with_extract
    project_name: project_name
    resource_type: datasource
```

`tabelau_refresh_tasks` is a list of dictionaries, where each dictionary represents a Tableau resource to refresh.
Each task should have the following fields:

- `resource_name` - name of the resource in Tableau (all resources within one project should have unique names
- `project_name` - name of the project in Tableau (to avoid conflicts with resources with the same name in different
  projects)
- `resource_type` - type of the resource in Tableau (`workbook` or `datasource`)

## List of Examples

1. [Basic Project](basic_project.md): a single domain, small tests, and a single target.
2. [Advanced Project](advanced_project.md): several domains, medium and large tests, and different targets.
3. [Dependencies management](dependencies_management.md): how to manage dependencies between models in different
   domains.
4. [Manual scheduling](manual_scheduling.md): domains with manual scheduling.
5. [Maintenance and source freshness](maintenance_and_source_freshness.md): how to manage maintenance tasks and source
   freshness.
6. [Kubernetes tasks](kubernetes_tasks.md): how to run dbt models in Kubernetes.
8. [\[Preview\] Extras and scripts](extras_and_scripts.md): available extras and scripts.
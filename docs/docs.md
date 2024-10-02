# `dbt-af` Configuration

## Table of Contents

1. [Model Config](#dbt-model-config-options)
    1. [DependencyConfig](#dependencyconfig)
    2. [DbtAFMaintenanceConfig](#dbtafmaintenanceconfig)
    3. [TableauRefreshTaskConfig](#tableaurefreshtaskconfig)

## dbt model config options

Model's config is an entrypoint to finetune the behavior of the model. It can be used to define the following options:

- `schedule` (_str_): tag to define the schedule of the model. Supported tags are:
    1. **@monthly** model will be run once a month on the first day of the month at midnight
       (equals to `0 0 1 * *` cron schedule)
    2. **@weekly** model will be run once a week on Sunday at midnight (equals to `0 0 * * 0` cron schedule)
    3. **@daily** model will be run once a day at midnight (equals to `0 0 * * *` cron schedule)
    4. **@hourly** model will be run once an hour at the beginning of the hour (equals to `0 * * * *` cron schedule)
    5. **@every15minutes** model will be run every 15 minutes (equals to `*/15 * * * *` cron schedule)
    6. **@manual** - special tag to mark the model has no schedule. Read more about it
       in [tutorial](../examples/manual_scheduling.md)
- `dependencies` (_dict[str, DependencyConfig]_): config to define how the model depends on other models. Read more
  about [DependencyConfig](#dependencyconfig)
- `enable_from_dttm` (_str_): date and time when the model should be enabled. The model will be skipped until this
  date. The format is `YYYY-MM-DDTHH:MM:SS`. Can be used in combination with `disable_from_dttm`
- `disable_from_dttm` (_str_): date and time when the model should be disabled. The model will be skipped after this
  date. The format is `YYYY-MM-DDTHH:MM:SS`. Can be used in combination with `enable_from_dttm`
- `domain_start_date` (_str_): date when the domain of the model starts. Option is used to reduce number of catchup
  DagRuns in Airflow. The format is `YYYY-MM-DDTHH:MM:SS`. Each domain selects minimal `domain_start_date` from all its
  models. Best place to set this option is `dbt_project.yml` file:

**_dbt_project.yml_**

```yaml
models:
  project_name:
    domain_name:
      domain_start_date: "2024-01-01T00:00:00"
```

- `dbt_target` (_str_): name of the dbt target to use for this exact model. If not set, the default target will be used.
  You can find more info in [tutorial](../examples/advanced_project.md#explicit-dbt-target)
- `py_cluster`, `sql_cluster`, `daily_sql_cluster`, `bf_cluster` (_str_):
  [resolving](../examples/advanced_project.md#how-is-the-target-determined) dbt target is based on these targets
  if explicitly not set. Usually, these parameters are set in the `dbt_project.yml` file for different domains:

**_dbt_project.yml_**

```yaml
models:
  project_name:
    domain_name:
      py_cluster: "py_cluster"
      sql_cluster: "sql_cluster"
      daily_sql_cluster: "daily_sql_cluster"
      bf_cluster: "bf_cluster"
```  

- `maintenance` (_DbtAFMaintenanceConfig_): config to define maintenance tasks for the model. Read more about
  [DbtAFMaintenanceConfig](#dbtafmaintenanceconfig)
- `tableau_refresh_tasks` (_list[TableauRefreshTaskConfig]_): list of Tableau refresh tasks. Read more about
  [TableauRefreshTaskConfig](#tableaurefreshtaskconfig)

### DependencyConfig

You can find the tutorial [here](../examples/dependencies_management.md)

For each dependency, you can specify the following options:

1. `skip` (_bool_): if set to `True`, the upstream model will be skipped.
2. `wait_policy` (_WaitPolicy_): policy how to build dependency. There are two options:
    1. `last` (default) – wait only for the last run of the upstream model taking into account the execution date of the
       DagRun
    2. `all` – wait for all runs of the upstream model between the execution interval. This is useful if upstream
       model's schedule is more frequent.

Example:

**_dmn_jaffle_analytics.ods.orders.yml_**

```yaml
models:
  - name: dmn_jaffle_analytics.ods.orders
    config:
      dependencies:
        dmn_jaffle_shop.stg.orders:
          skip: True
        dmn_jaffle_shop.stg.payments:
          wait_policy: last
```

### DbtAFMaintenanceConfig

You can find the tutorial [here](../examples/maintenance_and_source_freshness.md#maintenance-tasks).

Example of configuration:

**_dmn_jaffle_analytics.ods.orders.yml_**

```yaml
models:
  - name: dmn_jaffle_analytics.ods.orders
    config:
      ttl:
        key: etl_updated_dttm
        expiration_timeout: 10
```

### TableauRefreshTaskConfig

Configuration to define Tableau refresh tasks. This will trigger Tableau refresh tasks after the model is successfully
run.

> [!NOTE] 
> To use this feature, you need to install `dbt-af` with `tableau` extra.
> ```bash
> pip install dbt-af[tableau]
> ```

Example of configuration:

**_dmn_jaffle_analytics.ods.orders.yml_**

```yaml
models:
  - name: dmn_jaffle_analytics.ods.orders
    config:
      tableau_refresh_tasks:
        - resource_name: "extract_name"
          project_name: "project_name"
          resource_type: workbook  # or datasource
```


# `dbt-af` Configuration

## Table of Contents

1. [Model Config](#dbt-model-config-options)
    1. [schedule](#schedule)
    2. [dependencies](#dependencies-_dictstr-dependencyconfig_)
    3. [enable_from_dttm](#enable_from_dttm-_str_)
    4. [disable_from_dttm](#disable_from_dttm-_str_)
    5. [domain_start_date](#domain_start_date-_str_)
    6. [dbt_target](#dbt_target-_str_)
    7. [env](#env-dictstr-str)
    8. [py_cluster, sql_cluster, daily_sql_cluster, bf_cluster](#py_cluster-sql_cluster-daily_sql_cluster-bf_cluster-_str_)
    9. [maintenance](#maintenance-_dbtafmaintenanceconfig_)
    10. [tableau_refresh_tasks](#tableau_refresh_tasks-_listtableaurefreshtaskconfig_)

## dbt model config options

Model's config is an entrypoint to finetune the behavior of the model. It can be used to define the following options:

###### schedule

Tag to define the schedule of the model. Supported tags are:

- **@monthly** model will be run once a month on the first day of the month at midnight
  (equals to `0 0 1 * *` cron schedule)
- **@weekly** model will be run once a week on Sunday at midnight (equals to `0 0 * * 0` cron schedule)
- **@daily** model will be run once a day at midnight (equals to `0 0 * * *` cron schedule)
- **@hourly** model will be run once an hour at the beginning of the hour (equals to `0 * * * *` cron schedule)
- **@every15minutes** model will be run every 15 minutes (equals to `*/15 * * * *` cron schedule)
- **@manual** - special tag to mark the model has no schedule. Read more about it
  in [tutorial](../examples/manual_scheduling.md)

###### dependencies (_dict[str, DependencyConfig]_)

Сonfig to define how the model depends on other models.
You can find the tutorial [here](../examples/dependencies_management.md)

For each dependency, you can specify the following options:

1. `skip` (_bool_): if set to `True`, the upstream model will be skipped.
2. `wait_policy` (_WaitPolicy_): policy how to build dependency. There are two options:
    1. `last` (default) – wait only for the last run of the upstream model taking into account the execution date of the
       DagRun
    2. `all` – wait for all runs of the upstream model between the execution interval. This is useful if upstream
       model's schedule is more frequent.

Example:

```yaml
# dmn_jaffle_analytics.ods.orders.yml

models:
  - name: dmn_jaffle_analytics.ods.orders
    config:
      dependencies:
        dmn_jaffle_shop.stg.orders:
          skip: True
        dmn_jaffle_shop.stg.payments:
          wait_policy: last
```

###### enable_from_dttm (_str_)

Date and time when the model should be enabled. The model will be skipped until this
date. The format is `YYYY-MM-DDTHH:MM:SS`. Can be used in combination with `disable_from_dttm`

###### disable_from_dttm (_str_)

Date and time when the model should be disabled. The model will be skipped after this
date. The format is `YYYY-MM-DDTHH:MM:SS`. Can be used in combination with `enable_from_dttm`

###### domain_start_date (_str_)

Date when the domain of the model starts. Option is used to reduce number of catchup
DagRuns in Airflow. The format is `YYYY-MM-DDTHH:MM:SS`. Each domain selects minimal `domain_start_date` from all its
models. Best place to set this option is `dbt_project.yml` file:

```yaml
# dbt_project.yml

models:
  project_name:
    domain_name:
      domain_start_date: "2024-01-01T00:00:00"
```

###### dbt_target (_str_)

Name of the dbt target to use for this exact model. If not set, the default target will be used.
You can find more info in [tutorial](../examples/advanced_project.md#explicit-dbt-target)

###### env (dict[str, str])

Additional environment variables to pass to the runtime.
All variable values are passed first to dbt jinja rendering and then to the airflow rendering.

_Example:_

```yaml
# dmn_jaffle_analytics.ods.orders.yml

models:
  - name: dmn_jaffle_analytics.ods.orders
    config:
      env:
        MY_ENV_VAR: "my_value"
        MY_ENV_WITH_AF_RENDERING: "{{'{{ var.value.get(\'my.var\', \'fallback\') }}'}}"
```

> [!NOTE]
> To use [airflow templates](https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html#) in the `env`
> values, you need to use double curly braces `{{ '{{' }}` and `{{ '}}' }}` to escape them.
>
> Pattern: `{{<airflow_template>}}` --> `{{ '{{<airflow_template>' }}`

###### py_cluster, sql_cluster, daily_sql_cluster, bf_cluster (_str_)

[resolving](../examples/advanced_project.md#how-is-the-target-determined) dbt target is based on these targets
if explicitly not set. Usually, these parameters are set in the `dbt_project.yml` file for different domains:

```yaml
# dbt_project.yml

models:
  project_name:
    domain_name:
      py_cluster: "py_cluster"
      sql_cluster: "sql_cluster"
      daily_sql_cluster: "daily_sql_cluster"
      bf_cluster: "bf_cluster"
```  

###### maintenance (_DbtAFMaintenanceConfig_)

Config to define maintenance tasks for the model.

You can find the tutorial [here](../examples/maintenance_and_source_freshness.md#maintenance-tasks).

Example of configuration:

```yaml
# dmn_jaffle_analytics.ods.orders.yml

models:
  - name: dmn_jaffle_analytics.ods.orders
    config:
      ttl:
        key: etl_updated_dttm
        expiration_timeout: 10
```

###### tableau_refresh_tasks (_list[TableauRefreshTaskConfig]_)

List of Tableau refresh tasks.

Configuration to define Tableau refresh tasks. This will trigger Tableau refresh tasks after the model is successfully
run.

> [!NOTE]
> To use this feature, you need to install `dbt-af` with `tableau` extra.
> ```bash
> pip install dbt-af[tableau]
> ```

Example of configuration:

```yaml
# dmn_jaffle_analytics.ods.orders.yml

models:
  - name: dmn_jaffle_analytics.ods.orders
    config:
      tableau_refresh_tasks:
        - resource_name: "extract_name"
          project_name: "project_name"
          resource_type: workbook  # or datasource
```


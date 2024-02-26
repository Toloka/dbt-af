# Manual DAGs
By default, `dbt-af` will apply to all models `@daily` schedule as it's the most common case. 
But in some cases, you may want to have a _manual_ scheduling for your ad-hoc domains or if you want to have external trigger for DAG.

To enable manual scheduling, you need to use `@manual` scheduling in your dbt models.

All models with `@manual` scheduling will appear in the `manual` DAG in Airflow with suffix `__manual`.

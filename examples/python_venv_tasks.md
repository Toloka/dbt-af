# Python Virtual Environment Tasks in dbt pipelines

Since _dbt-af v0.12.0_, you can create and incorporate arbitrary python tasks into your dbt pipelines.
It has the same idea as [kubernetes tasks](./kubernetes_tasks.md): write some custom code for ingestion, machine
learning, or any other task, and run in the same dbt project if it was allowed by dbt model.

## How to create a python venv task

You can create `.py` file in your domain with any code you want to run. For example, you can
create `dmn_jaffle_analytics.ods.ingest_venv.py` file with the following content:

```python
# import the necessary libraries what are not available on airflow workers
import pandas as pd
import numpy as np
from sqlalchemy import create_engine


# HACK: we need to define this function so that dbt can parse the model correctly
# here we need to define all refs that are used in the model
def model(dbt, session):
    """
    Here you can reference any dbt model or source to build the dependency graph (it's not required).
    Although dependencies can be specified, your code must perform actual reading of data.
    """
    # dbt.ref('some_model')
    # dbt.source('some_source', 'some_table')

    return pd.DataFrame()  # it's required to return DataFrame in dbt python models


def ingest_venv():
    # do some work here
    print("Ingesting data")
    df = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list('ABCD'))
    print(df.head())
    # save the data to the postgres
    engine = create_engine('postgresql://airflow:airflow@localhost:5432/airflow')
    df.to_sql('random_data', con=engine, if_exists='replace', index=False)


# run the function
ingest_venv()

```

> [!NOTE]
> It's important to keep your model's entrypoint to be available just by
> running `python ./dmn_jaffle_analytics.ods.ingest_venv.py` in the terminal.

> [!WARNING]
> The function `model` is required to be defined in the file to make dbt able to parse the model correctly.

### Model's config

In the model's config you have to specify exact dbt target, that is defined in `profiles.yml` file, using `dbt_target`
key.

```yaml
version: 2

models:
  - name: dmn_jaffle_analytics.ods.ingest_venv
    config:
      dbt_target: ingest
      env: # this option is allowed only for airflow versions 2.10.0 and higher
        SOME_FLAG: "some_value"
```

### profiles.yml

To allow dbt-af to run models in venv, you need to create a new target in your `profiles.yml` file.

```yaml
default:
  target: dev
  outputs:
    ingest:
      type: venv # custom type for python venv tasks
      schema: "{{ env_var('POSTGRES_SCHEMA') }}"
      requirements:
        - dbxio==0.5.2
        - pandas
        - numpy
      system_site_packages: true
      python_version: "3.10"
      pip_install_options:
        - "--no-cache-dir"
      index_urls:
        - "https://pypi.org/simple"
      inherit_env: true # this option is allowed only for airflow versions 2.10.0 and higher
```

If everything is set up correctly, you can view the model in the dbt graph and run it will be run as a usual DAG
component.

![](../docs/static/py_venv_task.png)

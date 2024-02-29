# How to run tasks in Kubernetes

If you come to this point, you deserve a little hack to run tasks in Kubernetes without using actual dbt :smile:. 

The idea is to use the same `dbt` models interface, 
but instead of running them in your warehouse, you will run them in a Kubernetes cluster as usual python payload.

When you can use this approach:
- You need to run some heavy tasks that are not suitable for your warehouse (i.e., ML models)
- You want to export data somewhere else from your warehouse
- ...

> **Note**: Airflow instance must be running in the Kubernetes cluster. Created pods will be allocated in the same cluster.

### Step 1: Create docker image
Create docker image with all the necessary dependencies to run your code. Here is possible `Dockerfile` example:

```Dockerfile
FROM python:3.10-slim

COPY requirements.txt /requirements.txt

RUN pip install -r /requirements.txt
```

> **Note**: Do not use CMD or ENTRYPOINT in your Dockerfile.

### Step 2: Prepare kubernetes infrastructure
If you want to schedule pods on different nodes with specific tolerations, 
you have to prepare it before running the task.

### Step 3: Create new `profile` in dbt
`dbt-af` adds a new profile type that can be used to run tasks in Kubernetes.

Add this to your `profiles.yml`:

```yaml
k8s_target:
  schema: "{{ env_var('DBT_SCHEMA') }}"
  node_pool_selector_name: "selector_name"
  node_pool: "node_pool_name"
  image_name: "image_name"
  pod_cpu_guarantee: "500m"  # limit in k8s notation
  pod_memory_guarantee: "2Gi"  # limit in k8s notation
  type: kubernetes
  tolerations:  # optional (see Step 2)
    - key: "key"
      operator: "Exists"
```

### Step 4: Write "dbt" model
The structure of the model is practically the same as python models in dbt. 
The only difference is that you have to define your own entrypoint in a script.

The possible template of the script could be like this:

```python
import os
import typer
import pandas as pd
from typing import Optional


# HACK: we need to define this function so that dbt can parse the model correctly
# here we need to define all refs that are used in the model
def model(dbt, session):
    dbt.ref('your.model.name')
    
    return pd.DataFrame()  # it's required to return DataFrame in dbt python models

def main(
    model_config_b64: Optional[str] = os.getenv('MODEL_CONFIG_B64'),
    dag_run_conf: Optional[str] = os.getenv('DAG_RUN_CONF'),
    start_dttm: Optional[str] = os.getenv('START_DTTM'),
    end_dttm: Optional[str] = os.getenv('END_DTTM'),
):
    # your code here
    ...
    
if __name__ == '__main__':
    typer.run(main)
```

This snippet shows minimum requirements for the script to work.
1. It's required to define `model(dbt, session)` function for dbt to parse the model correctly. There you can define all refs that are used in the model and `dbt-af` will generate these dependencies in the DAG.
2. Make the script runnable from the command line with pure `python` command.
3. Four environment variables are passed to the script:
    - `MODEL_CONFIG_B64` - base64 encoded model configuration
    - `DAG_RUN_CONF` - DAG run configuration with parameters
    - `START_DTTM` - start date of the DAG run
    - `END_DTTM` - end date of the DAG run

### Step 5: Write yml file
Create a new yml file with the common structure for the dbt model. 
The only difference is that you have to set up `dbt_target` parameter in config 
(see [explicit dbt target](advanced_project.md#explicit-dbt-target)) with the new created profile.


## Limitations
1. The pod will be created in the same k8s cluster.
2. Only resource limits for pods are supported.
3. For now, only azure authentication is supported. If you want to pass `aadpodidbinding` to the pod, use `K8sConfig` in the main `dbt-af` config (see [K8sConfig](../dbt_af/conf/config.py)).


## List of Examples
1. [Basic Project](basic_project.md): a single domain, small tests, and a single target.
2. [Advanced Project](advanced_project.md): several domains, medium and large tests, and different targets.
3. [Dependencies management](dependencies_management.md): how to manage dependencies between models in different domains.
4. [Manual scheduling](manual_scheduling.md): domains with manual scheduling.
5. [Maintenance and source freshness](maintenance_and_source_freshness.md): how to manage maintenance tasks and source freshness.
7. [Integration with other tools](integration_with_other_tools.md): how to integrate dbt-af with other tools.
8. [\[Preview\] Extras and scripts](extras_and_scripts.md): available extras and scripts.
import base64
import datetime
import json
import logging
import os
import shlex
from typing import TYPE_CHECKING

import kubernetes.client.models as k8s
from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

from dbt_af.common.constants import AZ_MI_BINDING_LABEL_NAME
from dbt_af.conf import Config
from dbt_af.parser.dbt_profiles import KubernetesTarget

if TYPE_CHECKING:
    from airflow.utils.context import Context

PYTHON_SCRIPT_ENV = '__PYTHON_SCRIPT'
PYTHON_SCRIPT_FILE = 'script.py'


class DbtKubernetesPodOperator(KubernetesPodOperator):
    retries = 2

    def __init__(
        self,
        dbt_model_name: str,
        dbt_model_path: str,
        target_details: KubernetesTarget,
        dbt_af_config: Config,
        *args,
        **kwargs,
    ):
        self.dbt_model_name = dbt_model_name
        self.dbt_model_path = dbt_model_path
        self.target_details = target_details
        self.dbt_af_config = dbt_af_config

        super().__init__(
            *args,
            retries=self.retries,
            retry_delay=datetime.timedelta(minutes=5),
            max_active_tis_per_dag=1,
            cmds=['bash', '-c', ' && '.join(self._prepare_docker_cmds())],
            in_cluster=True,
            get_logs=True,
            log_events_on_failure=True,
            do_xcom_push=True,
            startup_timeout_seconds=1200,
            namespace=conf.get('kubernetes_executor', 'NAMESPACE'),
            **kwargs,
        )

    def _find_model_config_by_name(self):
        with open(self.dbt_af_config.dbt_project.dbt_project_path / 'target/manifest.json', 'r') as fn:
            manifest = json.load(fn)

        model_config = {}
        for node_info in manifest['nodes'].values():
            if node_info['name'] == self.dbt_model_name:
                model_config = node_info['config']
                break
        logging.info(f'Fetched model config for {self.dbt_model_name}: {model_config}')

        return model_config

    @staticmethod
    def _prepare_docker_cmds():
        _file_name = shlex.quote(PYTHON_SCRIPT_FILE)
        make_xcom_dir_cmd = 'mkdir -p /airflow/xcom'
        write_source_cmd = (
            f'python -c "import os;'
            rf'x = os.environ[\"{PYTHON_SCRIPT_ENV}\"];'
            rf'f = open(\"{_file_name}\", \"w\"); f.write(x); f.close()"'
        )
        exec_script_cmd = f'python {_file_name}'
        return [
            make_xcom_dir_cmd,
            write_source_cmd,
            exec_script_cmd,
        ]

    def execute(self, context: 'Context'):
        model_config = self._find_model_config_by_name()
        model_config_b64 = base64.b64encode(json.dumps(model_config).encode()).decode()

        self.env_vars.append(k8s.V1EnvVar(name='MODEL_CONFIG_B64', value=model_config_b64))
        self.env_vars.append(k8s.V1EnvVar(name='START_DTTM', value=context['data_interval_start'].isoformat()))
        self.env_vars.append(k8s.V1EnvVar(name='END_DTTM', value=context['data_interval_end'].isoformat()))
        self.env_vars.append(k8s.V1EnvVar(name='AIRFLOW_UNIQUE_NAME', value=os.getenv('AIRFLOW_UNIQUE_NAME')))
        self.env_vars.append(k8s.V1EnvVar(name='DAG_RUN_CONF', value=json.dumps(context['dag_run'].conf or {})))

        with open(self.dbt_af_config.dbt_project.dbt_models_path / self.dbt_model_path, 'r') as f:
            raw_node_source = f.read()
        self.env_vars.append(k8s.V1EnvVar(name=PYTHON_SCRIPT_ENV, value=raw_node_source))

        self.env_vars.extend(
            [
                k8s.V1EnvVar(
                    name='AIRFLOW__SECRETS__BACKEND_KWARGS', value=os.getenv('AIRFLOW__SECRETS__BACKEND_KWARGS', '{}')
                ),
                k8s.V1EnvVar(name='AIRFLOW__SECRETS__BACKEND', value=os.getenv('AIRFLOW__SECRETS__BACKEND', '')),
            ]
        )

        if self.dbt_af_config.k8s.airflow_identity_binding_selector:
            # TODO: migrate to workload identity (https://learn.microsoft.com/en-us/azure/aks/workload-identity-migrate-from-pod-identity#deploy-the-workload-with-migration-sidecar)
            self.labels = {AZ_MI_BINDING_LABEL_NAME: self.dbt_af_config.k8s.airflow_identity_binding_selector}

        self.image = self.target_details.image_name
        self.tolerations = [
            k8s.V1Toleration(key=toleration.key, operator=toleration.operator)
            for toleration in self.target_details.tolerations
        ]
        self.container_resources = k8s.V1ResourceRequirements(
            limits={
                'cpu': self.target_details.pod_cpu_guarantee,
                'memory': self.target_details.pod_memory_guarantee,
            },
        )
        self.node_selector = {self.target_details.node_pool_selector_name: self.target_details.node_pool}

        super().execute(context)

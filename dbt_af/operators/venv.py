import logging
from typing import Any, Sequence

from airflow import __version__ as airflow_version
from airflow.operators.python import PythonVirtualenvOperator
from airflow.utils.context import Context
from packaging.version import Version

from dbt_af.conf import Config
from dbt_af.parser.dbt_profiles import VenvTarget


class DbtPythonVenvOperator(PythonVirtualenvOperator):
    template_fields: Sequence[str] = tuple(
        {'dbt_model_path', 'target_details'}.union(PythonVirtualenvOperator.template_fields)
    )
    template_fields_renderers = {'target_details': 'python'}

    def __init__(
        self,
        dbt_model_path: str,
        target_details: VenvTarget,
        dbt_af_config: Config,
        env: dict[str, str] | None = None,
        **kwargs,
    ):
        self.target_details = target_details
        self.env_vars = env
        self.dbt_model_path = dbt_model_path
        self.dbt_af_config = dbt_af_config

        def _python_callable(source_code: str) -> None:
            exec(source_code)

        if Version(airflow_version) >= Version('2.10.0'):
            kwargs |= {'inherit_env': target_details.inherit_env, 'env_vars': env}
        if Version(airflow_version) >= Version('2.8.0'):
            kwargs |= {'index_urls': target_details.index_urls}

        super().__init__(
            python_callable=_python_callable,
            requirements=target_details.requirements,
            system_site_packages=target_details.system_site_packages,
            python_version=target_details.python_version,
            pip_install_options=target_details.pip_install_options,
            **dbt_af_config.retries_config.k8s_task_retry_policy.as_dict(),
            **kwargs,
        )

    def execute(self, context: Context) -> Any:
        with open(self.dbt_af_config.dbt_project.dbt_models_path / self.dbt_model_path, 'r') as f:
            model_code = f.read()
        self.op_args = (model_code,)

        if Version(airflow_version) < Version('2.10.0') and self.env_vars:
            logging.error('Environment variables are not supported in Airflow versions below 2.10.0')

        return super().execute(context)

from typing import TYPE_CHECKING, Optional

from airflow import Dataset

from dbt_af.common.constants import DBT_MODEL_DAG_PARAM
from dbt_af.conf import Config
from dbt_af.operators.base import DbtBaseActionOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DbtBaseDatasetOperator(DbtBaseActionOperator):
    def __init__(self, model_name: Optional[str], is_dataset_enable=False, model_type: str = 'sql', **kwargs) -> None:
        if model_name:
            # exactly one model
            super().__init__(
                model_name=model_name,
                model_type=model_type,
                outlets=[Dataset(model_name)] if is_dataset_enable else [],
                **kwargs,
            )
        else:
            super().__init__(model_name=DBT_MODEL_DAG_PARAM, **kwargs)

    def execute(self, context: 'Context'):
        if 'params' in context:
            if DBT_MODEL_DAG_PARAM in context['params'] and self.model_name == DBT_MODEL_DAG_PARAM:
                self.bash_command = self.bash_command.replace(
                    DBT_MODEL_DAG_PARAM, context['params'][DBT_MODEL_DAG_PARAM]
                )

        super().execute(context)

    def _patch_path_to_dbt_bash(self, **kwargs):
        if self.model_name_wo_type == DBT_MODEL_DAG_PARAM:
            return 'PATH_TO_DBT=$DBT_PROJECT_DIR && '
        return super()._patch_path_to_dbt_bash(**kwargs)


class DbtRun(DbtBaseDatasetOperator):
    @property
    def cli_command(self) -> str:
        return 'run'

    def __init__(self, dbt_af_config: 'Config', **kwargs) -> None:
        super().__init__(
            dbt_af_config=dbt_af_config,
            retry_policy=dbt_af_config.retries_config.dbt_run_retry_policy,
            **kwargs,
        )


class DbtSeed(DbtBaseDatasetOperator):
    @property
    def cli_command(self) -> str:
        return 'seed'

    def __init__(self, dbt_af_config: 'Config', **kwargs) -> None:
        super().__init__(
            dbt_af_config=dbt_af_config,
            retry_policy=dbt_af_config.retries_config.dbt_seed_retry_policy,
            **kwargs,
        )


class DbtSnapshot(DbtBaseDatasetOperator):
    @property
    def cli_command(self) -> str:
        return 'snapshot'

    def __init__(self, dbt_af_config: 'Config', **kwargs) -> None:
        super().__init__(
            dbt_af_config=dbt_af_config,
            retry_policy=dbt_af_config.retries_config.dbt_snapshot_retry_policy,
            **kwargs,
        )


class DbtTest(DbtBaseActionOperator):
    @property
    def cli_command(self) -> str:
        return 'test'

    def __init__(self, dbt_af_config: 'Config', **kwargs) -> None:
        super().__init__(
            dbt_af_config=dbt_af_config,
            max_active_tis_per_dag=None,
            target_environment=dbt_af_config.dbt_default_targets.default_for_tests_target,
            retry_policy=dbt_af_config.retries_config.dbt_test_retry_policy,
            overlap=True,
            **kwargs,
        )

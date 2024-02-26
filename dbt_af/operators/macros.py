import json
from typing import Optional

from airflow.models import DAG
from airflow.utils.task_group import TaskGroup

from dbt_af.conf import Config
from dbt_af.operators.base import DbtIntervalActionOperator
from dbt_af.parser.dbt_node_model import DbtAFMaintenanceConfig, DbtModelMaintenanceType


class DbtRunMacroOperation(DbtIntervalActionOperator):
    def __init__(self, **kwargs):
        self.macro = self.macro_name

        super().__init__(task_id=self.af_task_name, **kwargs)

    @property
    def af_task_name(self) -> str:
        safe_macro_name = f'dbt_{self.macro_name}'
        if hasattr(self, 'model_name'):
            return f'{safe_macro_name}__{self.model_name.replace(".", "_")}'
        return safe_macro_name

    @property
    def cli_command(self) -> str:
        return 'run-operation {macro}'

    @property
    def macro_name(self) -> str:
        raise NotImplementedError()

    @property
    def macro_args(self) -> Optional[dict]:
        return None

    def generate_bash(self, **kwargs):
        base_bash = super().generate_bash(**kwargs)
        operation_args = f" --args '{json.dumps(self.macro_args)}'" if self.macro_args else ''

        return base_bash.format(**kwargs) + operation_args


class DbtStageExternalSources(DbtRunMacroOperation):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def macro_name(self) -> str:
        return 'stage_external_sources'


class DbtPersistFullDocumentation(DbtRunMacroOperation):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def macro_name(self) -> str:
        return 'persist_docs_full'


class DbtPersistTableDocumentation(DbtRunMacroOperation):
    def __init__(self, model_name, maintenance_config: DbtAFMaintenanceConfig, **kwargs) -> None:
        self.model_name = model_name
        self.maintenance_config = maintenance_config

        super().__init__(**kwargs)

    @property
    def macro_name(self) -> str:
        return 'persist_docs'

    @property
    def macro_args(self) -> dict:
        return {'model_name': self.model_name}


class DbtOptimizeTable(DbtRunMacroOperation):
    def __init__(
        self,
        model_name: str,
        maintenance_config: DbtAFMaintenanceConfig,
        target_environment: str,
        max_active_tis_per_dag: int | None = 1,
        **kwargs,
    ) -> None:
        self.model_name = model_name
        self.maintenance_config = maintenance_config

        super().__init__(target_environment=target_environment, max_active_tis_per_dag=max_active_tis_per_dag, **kwargs)

    @property
    def macro_name(self) -> str:
        return 'optimize_table'

    @property
    def macro_args(self) -> dict:
        return {'model_name': self.model_name}


class DbtVacuumTable(DbtRunMacroOperation):
    def __init__(
        self,
        model_name: str,
        maintenance_config: DbtAFMaintenanceConfig,
        target_environment: str,
        max_active_tis_per_dag: int | None = 1,
        **kwargs,
    ) -> None:
        self.model_name = model_name
        self.maintenance_config = maintenance_config

        super().__init__(target_environment=target_environment, max_active_tis_per_dag=max_active_tis_per_dag, **kwargs)

    @property
    def macro_name(self) -> str:
        return 'vacuum_table'

    @property
    def macro_args(self) -> dict:
        return {'model_name': self.model_name}


class DbtDeduplicateTable(DbtRunMacroOperation):
    def __init__(
        self, model_name: str, maintenance_config: DbtAFMaintenanceConfig, target_environment: str, **kwargs
    ) -> None:
        self.retries = 0
        self.model_name = model_name
        self.maintenance_config = maintenance_config

        super().__init__(target_environment=target_environment, **kwargs)

    @property
    def macro_name(self) -> str:
        return 'deduplicate_table'

    @property
    def macro_args(self) -> dict:
        return {'model_name': self.model_name}


class DbtSetTTLOnTable(DbtRunMacroOperation):
    def __init__(
        self,
        model_name: str,
        maintenance_config: DbtAFMaintenanceConfig,
        target_environment: str,
        max_active_tis_per_dag: int = 1,
        **kwargs,
    ) -> None:
        self.model_name = model_name
        self.maintenance_config = maintenance_config

        super().__init__(target_environment=target_environment, max_active_tis_per_dag=max_active_tis_per_dag, **kwargs)

    @property
    def macro_name(self) -> str:
        return 'set_ttl_on_table'

    @property
    def macro_args(self) -> dict:
        return {
            'model_name': self.model_name,
            'key': self.maintenance_config.ttl.key,
            'expiration_timeout': self.maintenance_config.ttl.expiration_timeout,
            'additional_predicate': self.maintenance_config.ttl.additional_predicate,
            'force_predicate': self.maintenance_config.ttl.force_predicate,
        }


_MAPPING_MAINTENANCE_TYPE_TO_OPERATOR = {
    DbtModelMaintenanceType.PERSIST_DOCS: DbtPersistTableDocumentation,
    DbtModelMaintenanceType.OPTIMIZE_TABLES: DbtOptimizeTable,
    DbtModelMaintenanceType.VACUUM_TABLE: DbtVacuumTable,
    DbtModelMaintenanceType.DEDUPLICATE_TABLE: DbtDeduplicateTable,
    DbtModelMaintenanceType.SET_TTL_ON_TABLE: DbtSetTTLOnTable,
}


class DbtMaintenanceOperatorFactory:
    @staticmethod
    def create(
        model_name: str,
        maintenance_type: 'DbtModelMaintenanceType',
        task_group: 'TaskGroup',
        af_dag: 'DAG',
        maintenance_config: DbtAFMaintenanceConfig,
        dbt_af_config: 'Config',
        **kwargs,
    ) -> DbtRunMacroOperation:
        target_env = (
            dbt_af_config.dbt_default_targets.default_maintenance_target
            or dbt_af_config.dbt_default_targets.default_target
        )

        return _MAPPING_MAINTENANCE_TYPE_TO_OPERATOR[maintenance_type](
            model_name=model_name,
            maintenance_config=maintenance_config,
            task_group=task_group,
            dag=af_dag,
            max_active_tis_per_dag=1,
            target_environment=target_env,
            dbt_af_config=dbt_af_config,
            **kwargs,
        )

import json
import logging
import shutil
from datetime import timedelta
from tempfile import TemporaryDirectory
from typing import Dict, Optional

try:
    import pydantic.v1 as pydantic
except ModuleNotFoundError:
    import pydantic

from airflow.operators.bash import BashOperator
from airflow.utils.context import Context

from dbt_af.common.constants import DBT_COMPILE_POOL
from dbt_af.common.scheduling import BaseScheduleTag, ScheduleTag
from dbt_af.common.utils import find_latest_log_file, init_environment
from dbt_af.conf import Config


def get_delay_by_schedule(schedule_tag):
    if schedule_tag is not None and schedule_tag == ScheduleTag.hourly():
        return {'execution_timeout': timedelta(minutes=60)}
    return {'execution_timeout': timedelta(hours=6)}


class DbtBaseOperator(BashOperator):
    retries: int = 1

    @property
    def cli_command(self) -> str:
        raise NotImplementedError()

    def _patch_path_to_dbt_bash(self, **kwargs) -> str:
        return 'PATH_TO_DBT=$DBT_PROJECT_DIR && '

    def generate_bash(self, **kwargs) -> str:
        return (
            self._patch_path_to_dbt_bash(**kwargs)
            + ' cd $PATH_TO_DBT  && dbt {debug} {cli} '
            '--profiles-dir $DBT_PROFILES_DIR '
            '--project-dir $PATH_TO_DBT '
            '--target {target_environment}'.format(**kwargs)
        )

    def __init__(
        self,
        dbt_af_config: Config,
        schedule_tag: BaseScheduleTag,
        target_environment: str,
        max_active_tis_per_dag: int = 1,  # only one parallel tasks in the universe
        debug_flg: bool = True,
        pool: Optional[str] = None,
        **kwargs,
    ) -> None:
        self.debug = '--debug' if debug_flg else ''
        self.cli = self.cli_command

        self.target_environment = target_environment or dbt_af_config.dbt_default_targets.default_target
        assert self.target_environment, 'Target environment must be specified'

        self.dbt_af_config = dbt_af_config

        kwargs.update(get_delay_by_schedule(schedule_tag))
        af_pool = pool or f'dbt_{self.target_environment}' if dbt_af_config.use_dbt_target_specific_pools else None

        super().__init__(
            max_active_tis_per_dag=max_active_tis_per_dag,
            retries=self.retries,
            env=init_environment(self.dbt_af_config),
            pool=af_pool,
            append_env=True,
            bash_command=self.generate_bash(**self.__dict__),
            **kwargs,
        )

    def execute(self, context: Context):
        with TemporaryDirectory(dir=self.dbt_af_config.dbt_project.dbt_target_path) as tmp_target_path:
            # copy manifest.json to tmp target path to isolate it
            shutil.copy(
                self.dbt_af_config.dbt_project.dbt_project_path / 'target/manifest.json',
                f'{tmp_target_path}/manifest.json',
            )

            self.bash_command += f' --target-path {tmp_target_path}'
            if self.dbt_af_config.is_dev:
                # there is no dry-run mode in dbt, so we use -h flag just for empty dbt run
                self.bash_command += ' -h'

            super().execute(context)

            if self.dbt_af_config.mcd and self.dbt_af_config.mcd.artifacts_export_enabled:
                from dbt_af.integrations.mcd import send_dbt_artefacts_to_montecarlo

                latest_log_file = find_latest_log_file(context, self.dbt_af_config.dbt_project.dbt_log_path)
                try:
                    send_dbt_artefacts_to_montecarlo(
                        target_path=tmp_target_path,
                        log_path=latest_log_file,
                        model_name=context['task'].task_id,
                        metastore_name=self.dbt_af_config.mcd.metastore_name,
                        project_name=self.dbt_af_config.dbt_project.dbt_project_name,
                    )
                except Exception as e:
                    logging.warning(f'Failed to send dbt artefacts to MonteCarlo: {e}')
                    if self.dbt_af_config.mcd.success_required:
                        raise e


class DbtConstOperator(DbtBaseOperator):
    def __init__(self, pool: str = DBT_COMPILE_POOL, **kwargs) -> None:
        task_id = f'dbt_{self.cli_command.replace(" ", "_")}'
        super().__init__(pool=pool, task_id=task_id, **kwargs)


class DbtCompile(DbtConstOperator):
    @property
    def cli_command(self) -> str:
        return 'compile'


class DbtParse(DbtConstOperator):
    @property
    def cli_command(self) -> str:
        return 'parse'


class DbtDeps(DbtConstOperator):
    @property
    def cli_command(self) -> str:
        return 'deps'


class DbtClean(DbtConstOperator):
    @property
    def cli_command(self) -> str:
        return 'clean'


class DbtDocsGenerate(DbtConstOperator):
    @property
    def cli_command(self) -> str:
        return 'docs generate'


class DbtModelVars(pydantic.BaseModel):
    start_dttm: str
    end_dttm: str
    overlap: bool = False
    extra: Dict = pydantic.Field(exclude=True)

    _raw_keys_to_drop = {'start_dttm', 'end_dttm'}
    _dbt_keys_to_patch = {'dbt_start_dttm', 'dbt_end_dttm'}

    @pydantic.root_validator(pre=True)
    def build_extra(cls, values: Dict):
        required_fields = [field.alias for field in cls.__fields__.values() if field.alias != 'extra']

        extra = {}
        for field_name in list(values):
            if field_name in cls._raw_keys_to_drop:
                continue
            if field_name in cls._dbt_keys_to_patch:
                values[field_name.strip('dbt_')] = values[field_name]
            if field_name not in required_fields:
                extra[field_name] = values.pop(field_name)
                if isinstance(extra[field_name], (list, tuple)):
                    # brackets [] are not supported for array values in Databricks
                    # HACK: if it's empty collection, we pass empty string. It will work ONLY with string fields, but
                    # it's impossible to fetch correct type of the field from Databricks, and it's impossible to
                    # pass just empty brackets
                    extra[field_name] = (
                        f"({','.join(map(json.dumps, extra[field_name]))})" if len(extra[field_name]) > 0 else '("")'
                    )

        values['extra'] = extra
        return values

    def dict(self, **kwargs):
        """
        Updates the dict representation of the model with the extra fields.
        """
        return dict(**super().dict(**kwargs), **self.extra)


class DbtIntervalActionOperator(DbtBaseOperator):
    overlap = False

    def execute(self, context):
        updated_context = self._time_delta_logic(context)

        self.log.debug(
            'Context params:\n%s',
            '\n'.join(f'{k}={v}' for k, v in updated_context['params'].items()),
        )

        self.bash_command += (
            f" --vars '{json.dumps(DbtModelVars(**updated_context['params'], overlap=self.overlap).dict())}'"
        )

        super().execute(updated_context)

    @staticmethod
    def _time_delta_logic(context):
        """
        Here we parse the time interval and set the dbt_start_dttm and dbt_end_dttm variables
        If we get start_dttm and end_dttm in the context, we use them (in this case we get them from user-defined
        parameters)
        """
        # user defined parameters
        if 'start_dttm' in context['params'] and 'end_dttm' in context['params']:
            if context['params']['start_dttm'] != context['params']['end_dttm']:
                context['params']['dbt_start_dttm'] = context['params']['start_dttm']
                context['params']['dbt_end_dttm'] = context['params']['end_dttm']
                return context

        # if data_interval_start is equal to data_interval_end, we run dbt for the whole day starting
        # from data_interval_start.date()
        if context['data_interval_start'] == context['data_interval_end']:
            ds = context['data_interval_start'].replace(hour=0, minute=0, second=0, microsecond=0)
            de = ds + timedelta(days=1)
            context['params']['dbt_start_dttm'] = ds.isoformat()
            context['params']['dbt_end_dttm'] = de.isoformat()
            return context

        # take start_dttm == data_interval_start and end_dttm == data_interval_end (most common use case)
        context['params']['dbt_start_dttm'] = context['data_interval_start'].isoformat()
        context['params']['dbt_end_dttm'] = context['data_interval_end'].isoformat()

        return context


class DbtBaseActionOperator(DbtIntervalActionOperator):
    def generate_bash(self, **kwargs):
        base_bash = super().generate_bash(**kwargs)
        return base_bash + ' --select {model_name}'.format(**kwargs)

    def __init__(
        self,
        model_name: str,
        model_type: str = '',
        schedule_tag: Optional[BaseScheduleTag] = None,
        overlap: bool = False,
        **kwargs,
    ) -> None:
        self.model_name = f'{model_name}.{model_type}' if model_type else model_name
        self.model_name_wo_type = model_name
        self.overlap = overlap

        super().__init__(schedule_tag=schedule_tag, **kwargs)

    def _patch_path_to_dbt_bash(self, **kwargs) -> str:
        return (
            'if [[ "$DBT_ENABLE_MINI_DBT" = "true" ]]; '
            'then PATH_TO_DBT="$DBT_MINI_DBT_DIR/{model_name_wo_type}"; '
            'else PATH_TO_DBT="$DBT_PROJECT_DIR"; fi && '
        ).format(**kwargs)

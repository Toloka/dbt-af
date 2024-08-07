from pathlib import Path
from typing import Optional

import attrs
import pendulum


@attrs.define(frozen=True)
class CustomAfCallbacksConfig:
    """
    Config to define callbacks functions for airflow DAGs and tasks

    :param task_on_success_callback: success callback function for task (invoked when the task succeeds)
    :param task_on_failure_callback: failure callback function for task (invoked when the airflow task fails)
    :param task_on_retry_callback: retry callback function for task (invoked when the task is up for retry)
    :param task_on_execute_callback: execute callback function for task (invoked right before task begins executing)
    :param dag_on_failure_callback: failure callback function for DAG (invoked when the airflow task fails)
    :param dag_on_success_callback: success callback function for DAG (invoked when the task succeeds)
    :param dag_sla_miss_callback: sla miss callback function for DAG (invoked when a task misses its defined SLA)
    """

    task_on_success_callback: tuple[callable] = attrs.field(factory=tuple)
    task_on_failure_callback: tuple[callable] = attrs.field(factory=tuple)
    task_on_retry_callback: tuple[callable] = attrs.field(factory=tuple)
    task_on_execute_callback: tuple[callable] = attrs.field(factory=tuple)

    dag_on_failure_callback: tuple[callable] = attrs.field(factory=tuple)
    dag_on_success_callback: tuple[callable] = attrs.field(factory=tuple)
    dag_sla_miss_callback: tuple[callable] = attrs.field(factory=tuple)


@attrs.define(frozen=True)
class DependencyWaitPolicy:
    """
    Policy to build waits for models' dependencies.
    Default policy is to build waits per domain because it produces fewer tasks in airflow dag. It's possible to
    choose both policies at the same time, but only per domain policy will be used.

    :param per_domain: whether to build waits for models' dependencies per domain; if it's set then all waits will be
        collected in one task group per upstream domain
    :param per_task: whether to build waits for models' dependencies per task; if it's set then all waits will be
        put in the same task group with downstream model
    """

    per_domain: bool = attrs.field(default=True)
    per_task: bool = attrs.field(default=False)

    def __attrs_post_init__(self):
        if not self.per_domain and not self.per_task:
            raise ValueError('At least one of policy must be set')


@attrs.define(frozen=True)
class ModelDependenciesSection:
    """
    Section to parametrize how model dependencies are handled.

    :param wait_policy: policy to build waits for models' dependencies
    """

    wait_policy: DependencyWaitPolicy = attrs.field(factory=DependencyWaitPolicy)


@attrs.define(frozen=True)
class MCDIntegrationConfig:
    """
    Config for Monte Carlo Data integration.

    :param callbacks_enabled: whether to enable provided callbacks for airflow DAGs and tasks by airflow_mcd package
    :param artifacts_export_enabled: whether to enable dbt artifacts export to Monte Carlo Data
    :param success_required: whether to require dbt artifacts export to MCD to be successful
    :param metastore_name: name of metastore in MCD
    """

    callbacks_enabled: bool = attrs.field(default=False)
    artifacts_export_enabled: bool = attrs.field(default=False)
    success_required: bool = attrs.field(default=False)
    metastore_name: str = attrs.field(default='')


@attrs.define(frozen=True)
class TableauIntegrationConfig:
    """
    Config for Tableau integration.
    There are two ways to authenticate:
        - with username and password
        - with token name and personal access token (PAT)
    You can use only one of them. If both are specified, then username and password will be used.

    :param server_address: The address of the Tableau Server or Tableau Online instance.

    :param username: The name of the user whose credentials will be used to sign in
    :param password: The password of the user.
    :param user_id_to_impersonate: Specifies the id (not the name) of the user to sign in as.

    :param token_name: The personal access token name.
    :param pat: The personal access token.

    :param site_id: This corresponds to the contentUrl attribute in the Tableau REST API.
        The site_id is the portion of the URL that follows the /site/ in the URL. For example,
        “MarketingTeam” is the site_id in the following URL MyServer/#/site/MarketingTeam/projects.
        To specify the default site on Tableau Server, you can use an empty string '' (single quotes, no space).
        For Tableau Cloud, you must provide a value for the site_id.
    """

    server_address: str = attrs.field(validator=attrs.validators.instance_of(str))

    username: Optional[str] = attrs.field(
        default=None,
        validator=attrs.validators.optional(attrs.validators.instance_of(str)),
    )
    password: Optional[str] = attrs.field(
        default=None,
        validator=attrs.validators.optional(attrs.validators.instance_of(str)),
    )
    user_id_to_impersonate: Optional[str] = attrs.field(
        default=None,
        validator=attrs.validators.optional(attrs.validators.instance_of(str)),
    )

    token_name: Optional[str] = attrs.field(
        default=None,
        validator=attrs.validators.optional(attrs.validators.instance_of(str)),
    )
    pat: Optional[str] = attrs.field(
        default=None,
        validator=attrs.validators.optional(attrs.validators.instance_of(str)),
    )

    site_id: Optional[str] = attrs.field(
        default=None,
        validator=attrs.validators.optional(attrs.validators.instance_of(str)),
    )


@attrs.define(frozen=True)
class DbtProjectConfig:
    """
    Config for dbt project.

    :param dbt_project_name: name of dbt project; refer to field `name` in dbt_project.yml
    # TODO: read from dbt_project.yml
    :param dbt_models_path: path to dbt models; it's used in k8s operators to find raw models code
    :param dbt_project_path: path to directory with dbt_project.yml
    :param dbt_profiles_path: path to directory with profiles.yml
    :param dbt_target_path: path to directory where compiled files (e.g. compiled models and tests) will be written
    :param dbt_log_path: path to directory where dbt logs will be written
    :param dbt_schema: dbt custom schema for models
    :additional_dbt_env: additional dbt env vars that will be passed to all dbt commands; they could be accessed in
        dbt project with `env_var` function; all keys and values must be strings

    """

    dbt_project_name: str = attrs.field(validator=attrs.validators.instance_of(str))
    dbt_models_path: str | Path = attrs.field(validator=attrs.validators.instance_of((str, Path)), converter=Path)
    dbt_project_path: str | Path = attrs.field(validator=attrs.validators.instance_of((str, Path)), converter=Path)
    dbt_profiles_path: str | Path = attrs.field(validator=attrs.validators.instance_of((str, Path)), converter=Path)
    dbt_target_path: str | Path = attrs.field(validator=attrs.validators.instance_of((str, Path)), converter=Path)
    dbt_log_path: str | Path = attrs.field(validator=attrs.validators.instance_of((str, Path)), converter=Path)
    dbt_schema: str = attrs.field(validator=attrs.validators.instance_of(str))
    additional_dbt_env: dict[str, str] = attrs.field(factory=dict, hash=True, eq=str)

    @additional_dbt_env.validator
    def validate_additional_dbt_env_type(self, attribute, value):
        for k, v in value.items():
            if not isinstance(k, str) or not isinstance(v, str):
                raise ValueError(f'additional_dbt_env must be a dict[str, str], got key={repr(k)}, value={repr(v)}')


@attrs.define(frozen=True)
class DbtDefaultTargetsConfig:
    """
    Config for default dbt targets for different operators.
    Target can be specified with:
        - explicit `dbt_af.dbt_target` field in model's config
        - fields `py_cluster`, `sql_cluster`, `daily_sql_cluster`, `bf_cluster` in model's config
        - default targets specified in this config

    :param default_target: default dbt target for all operators if other not specified explicitly; to set specific
        target for model please use `dbt_af.dbt_target` field in model's config
    :param default_for_tests_target: default dbt target for all dbt tests
    :param default_maintenance_target: default dbt target for all dbt maintenance tasks
    :param default_backfill_target: default dbt target for all dbt backfill tasks
    """

    default_target: str = attrs.field()
    default_for_tests_target: str = attrs.field(default=None)
    default_maintenance_target: str = attrs.field(default=None)
    default_backfill_target: str = attrs.field(default=None)

    def __attrs_post_init__(self):
        object.__setattr__(self, 'default_for_tests_target', self.default_for_tests_target or self.default_target)
        object.__setattr__(self, 'default_maintenance_target', self.default_maintenance_target or self.default_target)
        object.__setattr__(self, 'default_backfill_target', self.default_backfill_target or self.default_target)


@attrs.define(frozen=True)
class K8sConfig:
    """
    Config for k8s operators.

    :param airflow_identity_binding_selector: selector for airflow identity binding; if not specified, then
        no label aadpodidbinding will be used; it's azure specific label, please refer to docs for more details
        (https://learn.microsoft.com/en-us/azure/aks/use-azure-ad-pod-identity)
    """

    airflow_identity_binding_selector: str = attrs.field(default=None)


@attrs.define(frozen=True)
class Config:
    """
    Main config for dbt-af.

    :param dbt_project: all dbt related params
    :param dbt_default_targets: default dbt targets for different operators
    :param model_dependencies: section to parametrize how model dependencies are handled
    :param include_single_model_manual_dag: whether to include single model manual dag; it will create airflow dag
        without schedule, only with manual trigger and preset trigger form, where model name and date interval can be
        specified
    :param max_active_dag_runs: max active dag runs for each airflow dag
    :param af_dag_description: description for airflow dags
    :param dag_start_date: default dag start date
    :param is_dev: whether it is dev environment; it's useful for local development, when you want to run dbt-af and
        turn off actual dbt runs and integrations with some external systems
    :param use_dbt_target_specific_pools: whether to use dbt target specific pools; if True, then airflow pools will be
        created for each dbt target with pattern `dbt_{target_name}`; if False, then only the default pool will be used
    :param af_callbacks: config with callback functions for airflow DAGs and tasks
    :param mcd: config for mcd integration; must be installed as extra dependency
    :params tableau: config for Tableau integration
    :param k8s: settings for k8s operators
    """

    # dbt specific params
    dbt_project: DbtProjectConfig = attrs.field()

    # dbt-af specific params
    dbt_default_targets: DbtDefaultTargetsConfig = attrs.field()
    model_dependencies: ModelDependenciesSection = attrs.field(factory=ModelDependenciesSection)
    include_single_model_manual_dag: bool = attrs.field(default=True)

    # airflow-specific params
    max_active_dag_runs: int = attrs.field(default=50)
    af_dag_description: str = attrs.field(default='https://www.buymeacoffee.com/tolokadataplatform')
    dag_start_date: pendulum.datetime = attrs.field(default=pendulum.datetime(2023, 10, 1, 0, 0, 0, tz='UTC'))
    is_dev: bool = attrs.field(default=False)
    use_dbt_target_specific_pools: bool = attrs.field(default=True)

    # airflow callbacks config
    af_callbacks: Optional[CustomAfCallbacksConfig] = attrs.field(default=None)

    # Monte Carlo Data integration
    mcd: Optional[MCDIntegrationConfig] = attrs.field(default=None)

    # Tableau integration
    tableau: Optional[TableauIntegrationConfig] = attrs.field(default=None)

    # k8s
    k8s: K8sConfig = attrs.field(factory=K8sConfig)

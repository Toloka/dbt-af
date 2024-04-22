from typing import Any, Literal

try:
    from pydantic.v1 import BaseModel, Field, root_validator
except ModuleNotFoundError:
    from pydantic import BaseModel, Field, root_validator

KUBERNETES_TARGET_TYPE = 'kubernetes'


class Target(BaseModel):
    target_type: Literal['postgres', 'snowflake', 'bigquery', 'redshift', 'databricks'] = Field(alias='type')
    target_schema: str = Field(alias='schema')
    threads: int = Field(default=1)

    # here could be any database-specific connection details

    @root_validator
    def validate_target(cls, values: dict[str, Any]) -> dict[str, Any]:
        for key, value in values.items():
            if key in cls.__fields__:
                continue
            values[key] = value
        return values

    class Config:
        extra = 'allow'


class KubernetesToleration(BaseModel):
    key: str
    operator: str


class KubernetesTarget(Target):
    target_type: Literal['kubernetes'] = Field(alias='type')
    node_pool_selector_name: str
    node_pool: str
    image_name: str
    pod_cpu_guarantee: str
    pod_memory_guarantee: str
    tolerations: list[KubernetesToleration]


class Profile(BaseModel):
    target: str
    outputs: dict[str, Target | KubernetesTarget]


class Profiles(BaseModel):
    profiles_config: dict[str, Any] = Field(alias='config', default_factory=dict)

    @root_validator
    def validate_profiles(cls, values: dict[str, Any]) -> dict[str, Any]:
        for key, value in values.items():
            if key in cls.__fields__:
                continue
            values[key] = Profile.parse_obj(value)
        return values

    class Config:
        extra = 'allow'

    def __getitem__(self, item):
        return getattr(self, item)

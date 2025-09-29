import sys
from typing import Any, Literal

try:
    from pydantic.v1 import BaseModel, Field, root_validator
except ModuleNotFoundError:
    from pydantic import BaseModel, Field, root_validator


class Target(BaseModel):
    target_type: str = Field(alias='type')

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


class VenvTarget(Target):
    target_type: Literal['venv'] = Field(alias='type')
    system_site_packages: bool
    requirements: list[str] = Field(default_factory=list)
    python_version: str = Field(default_factory=lambda: '.'.join(map(str, sys.version_info[:2])))
    pip_install_options: list[str] = Field(default_factory=list)
    index_urls: list[str] = Field(default_factory=list)
    inherit_env: bool = False


class Profile(BaseModel):
    target: str
    outputs: dict[str, VenvTarget | KubernetesTarget | Target]


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

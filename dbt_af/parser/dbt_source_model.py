import os
from typing import Any, Dict, List, Optional

try:
    import pydantic.v1 as pydantic
except ModuleNotFoundError:
    import pydantic


class _FreshnessAfterModel(pydantic.BaseModel):
    count: Optional[int]
    period: Optional[str]

    def empty(self) -> bool:
        return self.count is None and self.period is None


class DbtSourceFreshness(pydantic.BaseModel):
    warn_after: _FreshnessAfterModel
    error_after: _FreshnessAfterModel
    filter: Optional[str] = pydantic.Field(default=None)


class DbtSourceConfig(pydantic.BaseModel):
    enabled: bool


class DbtSource(pydantic.BaseModel):
    database: str
    node_schema: Optional[str] = pydantic.Field(..., alias='schema')
    name: str
    resource_type: str
    package_name: str
    path: str
    original_file_path: str
    unique_id: str
    fqn: List[str]
    source_name: str
    source_description: str
    loader: str
    identifier: str
    quoting: Dict[str, Any]
    loaded_at_field: str = pydantic.Field(default=None)
    freshness: DbtSourceFreshness
    external: Optional[Dict[str, Any]] = pydantic.Field(default=None)
    description: str
    columns: Dict[str, Any]
    meta: Dict[str, Any]
    source_meta: Dict[str, Any]
    tags: List[str]
    config: DbtSourceConfig
    patch_path: Optional[str] = pydantic.Field(default=None)
    unrendered_config: Dict[str, Any]
    relation_name: str
    created_at: float

    def __hash__(self) -> int:
        return hash(self.unique_id)

    def is_at_etl_service(self, etl_service_name) -> bool:
        return etl_service_name in self.original_file_path

    @property
    def original_file_path_dirname(self):
        return os.path.dirname(self.original_file_path)

    @property
    def domain(self) -> str:
        return self.fqn[1]

    def need_to_check_freshness(self) -> bool:
        if not self.config.enabled:
            return False
        if self.freshness.warn_after.empty() and self.freshness.error_after.empty():
            return False

        return True

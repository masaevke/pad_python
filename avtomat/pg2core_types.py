"""Типы конфигурации и метаданных для подготовки PostgreSQL-источников."""

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any


JsonObject = dict[str, Any]
RawConfig = dict[str, Any]


@dataclass
class TableOverride:
    primary_key: list[str] | None = None
    delete_key_paths: dict[str, str] = field(default_factory=dict)
    delete_entity_path: str = "[0]"
    snapshot_topic: str | None = None
    core_expressions: dict[str, str] = field(default_factory=dict)


@dataclass
class Config:
    generate_flows: bool
    apply_ddl_to_adb: bool
    create_flows_via_api: bool
    output_dir: str
    pg_host: str
    pg_port: int
    pg_database: str
    pg_schema_name: str
    pg_tables_to_convert: list[str]
    pg_user_env: str
    pg_password_env: str
    pg_sslmode: str
    metadata_file: str | None
    source_id: str
    dataflow_workspace_id: str
    dataflow_topic_prefix: str | None
    dataflow_flow_prefix: str
    dataflow_snapshot_ttl_days: int
    dataflow_oplog_topic: str
    dataflow_pg_port: int
    dataflow_pg_credential: str
    dataflow_adb_credential: str
    dataflow_stream_credential: str
    dataflow_target_database: str | None
    dataflow_endpoint: str
    dataflow_flow_type_id: str
    dataflow_owner_id: str
    dataflow_custom_team_id: str | None
    dataflow_yt_task_link: str
    dataflow_api_url: str
    dataflow_identity_url: str
    dataflow_client_id_env: str
    dataflow_identity_secret_env: str
    dataflow_token_env: str
    dataflow_request_timeout: int
    stream_state_path: str
    adb_host: str
    adb_port: int
    adb_database: str
    adb_user_env: str
    adb_password_env: str
    adb_prompt_password: bool
    adb_sslmode: str
    adb_raw_schema: str
    adb_prep_schema: str
    adb_core_schema: str
    partition_start: str | None
    partition_months: int
    dbt_project_name: str
    model_prefix: str
    model_directory: str
    core_alias_suffix: str
    table_overrides: dict[str, TableOverride]


@dataclass
class ColumnMetadata:
    name: str
    data_type: str
    description: str = ""


@dataclass
class TableMetadata:
    columns: list[ColumnMetadata] = field(default_factory=list)
    primary_key: list[str] = field(default_factory=list)


Metadata = dict[str, TableMetadata]


@dataclass
class TablePlan:
    name: str
    columns: list[ColumnMetadata]
    keys: list[str]
    override: TableOverride
    model: str
    prep_model: str
    view_model: str
    alias: str
    view_alias: str
    raw_schema: str


def config_from_dict(values: RawConfig) -> Config:
    """Преобразовать JSON-конфигурацию, сохранив исходный словарь."""
    try:
        prepared = dict(values)
        if "table_overrides" in prepared:
            overrides: dict[str, TableOverride] = {}
            for name, override in prepared["table_overrides"].items():
                if "primary_key" in override and override["primary_key"] is None:
                    raise ValueError(name + ": нужен primary_key (непустой список без повторов)")
                overrides[name] = TableOverride(**override)
            prepared["table_overrides"] = overrides
        return Config(**prepared)
    except (TypeError, AttributeError) as exc:
        raise ValueError("Некорректная структура конфигурации: " + str(exc)) from exc


def metadata_from_dict(values: dict[str, Any], tables: Iterable[str] | None = None) -> Metadata:
    """Преобразовать JSON-метаданные в таблицы и колонки."""
    try:
        selected = values if tables is None else {name: values[name] for name in tables if name in values}
        return {
            name: TableMetadata(
                columns=[
                    ColumnMetadata(
                        name=column["name"],
                        data_type=column["data_type"],
                        description=column.get("description", ""),
                    )
                    for column in table.get("columns", [])
                ],
                primary_key=table.get("primary_key", []),
            )
            for name, table in selected.items()
        }
    except (TypeError, AttributeError) as exc:
        raise ValueError("Некорректная структура метаданных таблиц: " + str(exc)) from exc

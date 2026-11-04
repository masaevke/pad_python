"""Проверки типов данных, имён и метаданных мигрируемых таблиц."""

import pytest

from pg2core_generator import (
    MetadataValidationError,
    PostgresToCoreConverter,
    identifier,
    safe_type,
)
from pg2core_types import (
    Config,
    Metadata,
    TableOverride,
    config_from_dict,
    metadata_from_dict,
)


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        (" INT8 ", "bigint"),
        ("varchar(200)", "character varying(200)"),
        ("numeric(18, 2)", "numeric(18,2)"),
        ("timestamptz", "timestamp with time zone"),
    ],
)
def test_safe_type_normalizes_postgres_types(source: str, expected: str) -> None:
    assert safe_type(source) == expected


@pytest.mark.parametrize("source", ["money", "integer; drop table orders"])
def test_safe_type_rejects_unsupported_types(source: str) -> None:
    with pytest.raises(ValueError, match="поддерживаемого"):
        safe_type(source)


@pytest.mark.parametrize("name", ["Orders", "public.orders", "a" * 64, None])
def test_identifier_rejects_invalid_names(name: object) -> None:
    with pytest.raises(ValueError, match="lower_snake_case"):
        identifier(name)


def test_missing_table_raises_custom_exception(
    demo_config: Config, demo_metadata: Metadata
) -> None:
    del demo_metadata["orders"]

    with pytest.raises(MetadataValidationError, match="orders"):
        PostgresToCoreConverter(demo_config).plan(demo_metadata)


@pytest.mark.parametrize("keys", [[], ["missing"], ["order_id", "order_id"]])
def test_table_metadata_rejects_invalid_primary_key(
    demo_config: Config, demo_metadata: Metadata, keys: list[str]
) -> None:
    demo_metadata["orders"].primary_key = keys

    with pytest.raises(ValueError, match="primary_key|ключ отсутствует"):
        PostgresToCoreConverter(demo_config).plan(demo_metadata)


def test_table_metadata_rejects_duplicate_columns(
    demo_config: Config, demo_metadata: Metadata
) -> None:
    demo_metadata["orders"].columns.append(demo_metadata["orders"].columns[0])

    with pytest.raises(ValueError, match="неверные колонки"):
        PostgresToCoreConverter(demo_config).plan(demo_metadata)


def test_config_rejects_duplicate_tables(demo_config: Config) -> None:
    demo_config.pg_tables_to_convert.append("orders")

    with pytest.raises(ValueError, match="без повторов"):
        PostgresToCoreConverter(demo_config).validate()


def test_config_rejects_same_raw_and_core_schema(demo_config: Config) -> None:
    demo_config.adb_core_schema = demo_config.adb_raw_schema

    with pytest.raises(ValueError, match="разные схемы"):
        PostgresToCoreConverter(demo_config).validate()


def test_override_rejects_unknown_column(
    demo_config: Config, demo_metadata: Metadata
) -> None:
    demo_config.table_overrides["orders"] = TableOverride(
        core_expressions={"missing": "1"}
    )

    with pytest.raises(ValueError, match="неизвестные поля"):
        PostgresToCoreConverter(demo_config).plan(demo_metadata)


def test_config_parser_rejects_explicit_null_primary_key() -> None:
    with pytest.raises(ValueError, match="orders: нужен primary_key"):
        config_from_dict({"table_overrides": {"orders": {"primary_key": None}}})


def test_metadata_parser_ignores_unselected_tables() -> None:
    metadata = metadata_from_dict(
        {
            "orders": {
                "columns": [{"name": "id", "data_type": "bigint"}],
                "primary_key": ["id"],
            },
            "unused": None,
        },
        ["orders", "missing"],
    )

    assert list(metadata) == ["orders"]
    assert metadata["orders"].primary_key == ["id"]

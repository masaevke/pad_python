"""Проверки согласованности локального плана миграции и сохранённых файлов."""

import json
from pathlib import Path

import pytest
import yaml

from pg2core_generator import PostgresToCoreConverter
from pg2core_types import Config, JsonObject, Metadata, TableOverride


def test_plan_saves_raw_ddl_and_three_model_layers(
    demo_config: Config, demo_metadata: Metadata, tmp_path: Path,
) -> None:
    converter = PostgresToCoreConverter(demo_config).plan(demo_metadata)
    converter.save_plan(tmp_path)

    model_files = list((tmp_path / "project" / "models").rglob("*.sql"))
    assert len(model_files) == 9
    assert {path.stem for path in model_files} == {
        model_name
        for table in converter.tables
        for model_name in (table.model, table.prep_model, table.view_model)
    }
    order_items = next(table for table in converter.tables if table.name == "order_items")
    raw_sql = (tmp_path / "raw.sql").read_text(encoding="utf-8")
    assert 'create table if not exists "raw_sales"."order_items"' in raw_sql
    assert '"order_id" bigint not null' in raw_sql
    assert '"line_number" integer not null' in raw_sql
    assert 'distributed by ("order_id", "line_number")' in raw_sql
    assert '"_entity" jsonb not null' in raw_sql
    assert '"unit_price"' not in raw_sql
    assert "end ('2027-02-01 00:00:00+00'::timestamptz) exclusive" in raw_sql

    base = tmp_path / "project" / "models" / demo_config.model_directory
    core_sql = (base / demo_config.adb_core_schema / f"{order_items.model}.sql").read_text(encoding="utf-8")
    view_sql = (base / demo_config.adb_core_schema / f"{order_items.view_model}.sql").read_text(encoding="utf-8")
    assert "(_entity ->> 'unit_price')::numeric(18,2)" in core_sql
    assert f"ref('{order_items.prep_model}')" in core_sql
    assert f"ref('{order_items.model}')" in view_sql
    assert "where not _is_deleted" in view_sql


def test_model_properties_keep_composite_keys_and_layer_contracts(
    demo_config: Config, demo_metadata: Metadata, tmp_path: Path,
) -> None:
    converter = PostgresToCoreConverter(demo_config).plan(demo_metadata)
    converter.save_plan(tmp_path)
    table = next(table for table in converter.tables if table.name == "order_items")
    base = tmp_path / "project" / "models" / demo_config.model_directory

    core_document: JsonObject = yaml.safe_load(
        (base / demo_config.adb_core_schema / "properties" / f"{table.model}.yml").read_text(encoding="utf-8")
    )
    core: JsonObject = core_document["models"][0]
    assert core["config"]["unique_key"] == ["order_id", "line_number"]
    assert core["config"]["contract"] == {"enforced": True}
    assert core["config"]["incremental_strategy"] == "delete+insert"
    assert core["data_tests"] == [{
        "dbt_utils.unique_combination_of_columns": {
            "arguments": {"combination_of_columns": ["order_id", "line_number"]},
        },
    }]
    key_columns = {column["name"]: column for column in core["columns"] if column["name"] in table.keys}
    assert all(column["data_tests"] == ["not_null"] for column in key_columns.values())

    prep_document: JsonObject = yaml.safe_load(
        (base / demo_config.adb_prep_schema / "properties" / f"{table.prep_model}.yml").read_text(encoding="utf-8")
    )
    prep: JsonObject = prep_document["models"][0]
    assert prep["config"]["unique_key"] == table.keys
    test_names = {name for test in prep["data_tests"] for name in test}
    assert {"snapshot_missing_keys", "snapshot_orphan_keys"} <= test_names

    view_document: JsonObject = yaml.safe_load(
        (base / demo_config.adb_core_schema / "properties" / f"{table.view_model}.yml").read_text(encoding="utf-8")
    )
    view: JsonObject = view_document["models"][0]
    assert view["config"]["materialized"] == "safe_view"
    assert "unique_key" not in view["config"]
    assert "contract" not in view["config"]


def test_metadata_artifact_contains_normalized_types_without_changing_input(
    demo_config: Config, demo_metadata: Metadata, tmp_path: Path,
) -> None:
    converter = PostgresToCoreConverter(demo_config).plan(demo_metadata)
    converter.save_plan(tmp_path)
    saved: JsonObject = json.loads((tmp_path / "metadata.json").read_text(encoding="utf-8"))

    customer_types = {column["name"]: column["data_type"] for column in saved["customers"]["columns"]}
    assert customer_types["customer_name"] == "character varying(200)"
    assert customer_types["registered_at"] == "timestamp with time zone"
    assert saved["order_items"]["primary_key"] == ["order_id", "line_number"]
    assert saved["customers"]["columns"][1]["description"] == "Имя покупателя"
    assert demo_metadata["customers"].columns[1].data_type == "varchar(200)"
    assert not (tmp_path / "flows").exists()


def test_saving_the_same_plan_again_preserves_artifacts(
    demo_config: Config, demo_metadata: Metadata, tmp_path: Path,
) -> None:
    converter = PostgresToCoreConverter(demo_config).plan(demo_metadata)
    converter.save_plan(tmp_path)
    original = {path.relative_to(tmp_path): path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()}

    converter.save_plan(tmp_path)

    assert {path.relative_to(tmp_path): path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()} == original


def test_conflicting_plan_does_not_overwrite_existing_artifacts(
    demo_config: Config, demo_metadata: Metadata, tmp_path: Path,
) -> None:
    PostgresToCoreConverter(demo_config).plan(demo_metadata).save_plan(tmp_path)
    original = {path.relative_to(tmp_path): path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()}
    demo_metadata["customers"].columns[1].description = "Новое описание покупателя"
    changed = PostgresToCoreConverter(demo_config).plan(demo_metadata)

    with pytest.raises(ValueError, match="Другой план уже существует"):
        changed.save_plan(tmp_path)

    assert {path.relative_to(tmp_path): path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()} == original


def test_flow_artifact_maps_both_keys_and_snapshot_oplog_sources(
    demo_config: Config, demo_metadata: Metadata, tmp_path: Path,
) -> None:
    demo_config.generate_flows = True
    demo_config.pg_tables_to_convert = ["order_items"]
    demo_config.pg_host = "postgres.example.invalid"
    demo_config.adb_host = "adb.example.invalid"
    demo_config.dataflow_workspace_id = "demo"
    demo_config.dataflow_flow_prefix = "sales"
    demo_config.dataflow_oplog_topic = "demo/sales_oplog"
    demo_config.dataflow_pg_credential = "demo_pg"
    demo_config.dataflow_adb_credential = "demo_adb"
    demo_config.dataflow_stream_credential = "demo_stream"
    demo_config.dataflow_owner_id = "00000000-0000-4000-8000-000000000001"
    demo_config.table_overrides["order_items"] = TableOverride(
        snapshot_topic="demo/order_lines_snapshot",
        delete_key_paths={"order_id": "[0].orderId", "line_number": "[0].lineNo"},
    )
    converter = PostgresToCoreConverter(demo_config).plan(demo_metadata)
    converter.save_plan(tmp_path)
    flow: JsonObject = json.loads(
        (tmp_path / "flows" / "sales_order_items_to_adb.json").read_text(encoding="utf-8")
    )

    assert flow == converter.flows[0]
    content: JsonObject = flow["content"]
    assert content["SnapshotBufferConfig"]["Topic"] == "demo/order_lines_snapshot"
    assert content["OplogBufferConfig"]["Topic"] == "demo/sales_oplog"
    assert content["TargetInfo"]["PgTargetConfiguration"]["ServerProperties"]["TableName"] == "raw_sales.order_items"
    operators: list[JsonObject] = content["MappingInfo"]["Operators"]
    key_extraction = next(
        extraction
        for operator in operators if operator["Type"] == "Extract"
        for extraction in operator["ExtractionConfigs"] if extraction["FieldName"] == "KeyFields"
    )
    assert key_extraction["ValueExtractionConfigs"][:2] == [
        {"Path": "[0].orderId", "TargetName": "key_deleted_0"},
        {"Path": "[0].lineNo", "TargetName": "key_deleted_1"},
    ]
    mappings = {
        mapping["TargetName"]: mapping
        for operator in operators if operator["Type"] == "Mapping"
        for mapping in operator["MappingConfig"]
    }
    for index, key in enumerate(("order_id", "line_number")):
        # У ключа есть отдельный Coalesce перед финальным отображением полей.
        coalesced = next(
            mapping
            for operator in operators if operator["Type"] == "Mapping"
            for mapping in operator["MappingConfig"]
            if mapping["TargetName"] == key and mapping.get("Converter") == "Coalesce"
        )
        assert coalesced["FieldSourceNames"] == [f"key_deleted_{index}", f"key_raw_{index}"]
    assert mappings["_event_source"]["MapValues"] == {
        "demo/order_lines_snapshot": 0, "demo/sales_oplog": 1,
    }

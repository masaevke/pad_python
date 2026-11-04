"""Запуск CLI без внешних сервисов и жизненный цикл DB-соединения."""

from dataclasses import asdict, replace
import json
import logging
from pathlib import Path
import sys
from typing import Literal
from unittest.mock import ANY, MagicMock

import pytest

import pg2core_generator as generator
from pg2core_types import Config, Metadata


def write_cli_inputs(
    tmp_path: Path, demo_config: Config, demo_metadata: Metadata
) -> tuple[Path, Path, Path]:
    """Подготовить временные входные JSON и отдельный каталог результата."""
    output = tmp_path / "output"
    config = replace(
        demo_config,
        generate_flows=False,
        apply_ddl_to_adb=False,
        create_flows_via_api=False,
        output_dir=str(output),
    )
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(asdict(config)), encoding="utf-8")
    metadata_path = tmp_path / "metadata.json"
    metadata_path.write_text(
        json.dumps({name: asdict(table) for name, table in demo_metadata.items()}),
        encoding="utf-8",
    )
    return config_path, metadata_path, output


def cli_arguments(config_path: Path, metadata_path: Path, output: Path) -> list[str]:
    return [
        "pg2core",
        "--config", str(config_path),
        "--metadata", str(metadata_path),
        "--output", str(output),
    ]


def test_main_generates_offline_plan(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    demo_config: Config,
    demo_metadata: Metadata,
) -> None:
    config_path, metadata_path, output = write_cli_inputs(tmp_path, demo_config, demo_metadata)
    monkeypatch.setattr(sys, "argv", cli_arguments(config_path, metadata_path, output))

    assert generator.main() == 0

    for name in ("raw.sql", "raw_comments.sql", "metadata.json", "selectors.proposed.yml"):
        assert (output / name).is_file()
    assert "create table if not exists" in (output / "raw.sql").read_text(encoding="utf-8")
    exported = json.loads((output / "metadata.json").read_text(encoding="utf-8"))
    assert set(exported) == set(demo_config.pg_tables_to_convert)
    for name in demo_config.pg_tables_to_convert:
        assert exported[name]["primary_key"] == demo_metadata[name].primary_key
    model_files = list((output / "project" / "models").rglob("*.sql"))
    assert len(model_files) == 3 * len(demo_config.pg_tables_to_convert)
    reports = list(output.glob("execution_*.json"))
    assert len(reports) == 1
    report = json.loads(reports[0].read_text(encoding="utf-8"))
    assert report["tables"] == demo_config.pg_tables_to_convert
    assert report["steps"][-1] == {"step": "complete", "status": "ok"}


def test_main_reports_missing_metadata_without_writing_plan(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
    demo_config: Config,
    demo_metadata: Metadata,
) -> None:
    config_path, metadata_path, output = write_cli_inputs(tmp_path, demo_config, demo_metadata)
    metadata_path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(sys, "argv", cli_arguments(config_path, metadata_path, output))

    with caplog.at_level(logging.ERROR, logger="pg2core"):
        assert generator.main() == 1

    table = demo_config.pg_tables_to_convert[0]
    assert f"Ошибка метаданных: Метаданные таблицы не найдены: {table}" in caplog.messages
    assert all(record.exc_info is None for record in caplog.records)
    assert "Traceback" not in caplog.text
    assert not output.exists()


def test_dry_run_overrides_config_and_action_flags(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    demo_config: Config,
    demo_metadata: Metadata,
) -> None:
    config_path, metadata_path, output = write_cli_inputs(tmp_path, demo_config, demo_metadata)
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config.update(apply_ddl_to_adb=True, create_flows_via_api=True)
    config_path.write_text(json.dumps(config), encoding="utf-8")
    arguments = cli_arguments(config_path, metadata_path, output)
    arguments.extend(["--apply-ddl", "--create-flows", "--dry-run"])
    monkeypatch.setattr(sys, "argv", arguments)
    adb_client = MagicMock(name="AdbClient")
    dataflow_client = MagicMock(name="DataflowApiClient")
    monkeypatch.setattr(generator, "AdbClient", adb_client)
    monkeypatch.setattr(generator, "DataflowApiClient", dataflow_client)

    assert generator.main() == 0

    adb_client.assert_not_called()
    dataflow_client.assert_not_called()
    assert (output / "raw.sql").is_file()


@pytest.mark.parametrize("prefix", ["pg", "adb"], ids=["postgres", "adb"])
@pytest.mark.parametrize("fails", [False, True], ids=["success", "exception"])
def test_connection_delegates_transaction_and_always_closes(
    monkeypatch: pytest.MonkeyPatch,
    demo_config: Config,
    prefix: Literal["pg", "adb"],
    fails: bool,
) -> None:
    config = replace(
        demo_config,
        pg_host="pg.example.test",
        pg_port=5433,
        pg_database="source_test",
        pg_user_env="PG2CORE_TEST_PG_USER",
        pg_password_env="PG2CORE_TEST_PG_PASSWORD",
        pg_sslmode="require",
        adb_host="adb.example.test",
        adb_port=6433,
        adb_database="target_test",
        adb_user_env="PG2CORE_TEST_ADB_USER",
        adb_password_env="PG2CORE_TEST_ADB_PASSWORD",
        adb_prompt_password=False,
        adb_sslmode="verify-full",
    )
    monkeypatch.setenv(config.pg_user_env, "source_reader")
    monkeypatch.setenv(config.pg_password_env, "pg-test-password")
    monkeypatch.setenv(config.adb_user_env, "target_writer")
    monkeypatch.setenv(config.adb_password_env, "adb-test-password")
    connection = MagicMock(name="connection")
    connection.__enter__.return_value = connection
    connection.__exit__.return_value = False
    connect = MagicMock(name="connect", return_value=connection)
    monkeypatch.setattr(generator.psycopg2, "connect", connect)

    if fails:
        failure = RuntimeError("work failed")
        with pytest.raises(RuntimeError, match="work failed"):
            with generator.connection(config, prefix) as opened:
                assert opened is connection
                raise failure
        connection.__exit__.assert_called_once_with(RuntimeError, failure, ANY)
        assert connection.__exit__.call_args.args[2] is not None
    else:
        with generator.connection(config, prefix) as opened:
            assert opened is connection
        connection.__exit__.assert_called_once_with(None, None, None)

    if prefix == "pg":
        connect.assert_called_once_with(
            host="pg.example.test", port=5433, dbname="source_test",
            user="source_reader", password="pg-test-password",
            sslmode="require", connect_timeout=10,
        )
    else:
        connect.assert_called_once_with(
            host="adb.example.test", port=6433, dbname="target_test",
            user="target_writer", password="adb-test-password",
            sslmode="verify-full", connect_timeout=10,
        )
    connection.__enter__.assert_called_once_with()
    connection.close.assert_called_once_with()

"""Общие данные и изоляция тестов от внешних сервисов."""

import copy
import json
from pathlib import Path
from typing import Any

import pytest
import requests

import pg2core_generator as generator
from pg2core_types import Config, Metadata, config_from_dict, metadata_from_dict


EXAMPLES = Path(__file__).resolve().parents[1] / "examples"


@pytest.fixture
def demo_config() -> Config:
    values = copy.deepcopy(generator.config)
    values.update(json.loads((EXAMPLES / "config.json").read_text(encoding="utf-8")))
    return config_from_dict(values)


@pytest.fixture
def demo_metadata() -> Metadata:
    values = json.loads((EXAMPLES / "metadata.json").read_text(encoding="utf-8"))
    return metadata_from_dict(values)


@pytest.fixture(autouse=True)
def prevent_external_services(monkeypatch: pytest.MonkeyPatch) -> None:
    def blocked(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("Тест попытался обратиться к реальной БД или API")

    monkeypatch.setattr(generator.psycopg2, "connect", blocked)
    monkeypatch.setattr(requests.sessions.Session, "request", blocked)
    monkeypatch.setattr(generator, "load_secrets", lambda: None)

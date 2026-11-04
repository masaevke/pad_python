"""Генератор PostgreSQL -> UP/JSONB -> dbt prep/core
Порядок работы:
1. Общий oplog-ный поток уже должен существовать и надо будет задать его топик - создай его
2. Установить проект из папки avtomat: python -m pip install -e .
   Заполнить параметры в config, а логины/пароли/API-креды - в .env рядом с этим .py.
   Шаблон для скачивания из Git: .env.example; переименовать его в .env.
3. Выполнить python pg2core_generator.py --dry-run и проверить output_dir.
4. Для применения: python pg2core_generator.py --apply-ddl --create-flows.
   Либо включить одноименные флаги в config и запустить без аргументов.
   --create-flows проверяет готовый oplog-топик и создает отсутствующие snapshot-топики.
   Срок хранения новых snapshot-топиков задается dataflow_snapshot_ttl_days.
   Существующие топики и их TTL не изменяются; ключу API нужны права чтения/создания топиков.
5. Перенести локальные модели вручную в проект на gitlab, сделать CHANGELOG, dbt deps/build и MR.

Модели всегда локальные: prep/core/view, source/properties, docs/selectors.
Макросы не генерируются: используются postgres_snapshot_macros, snapshot_missing_keys,
snapshot_orphan_keys и generate_schema_name, уже подключенные в целевом dbt-проекте.
Докблоки бизнес-полей генерируются один раз для core и переиспользуются остальными слоями.
Служебные поля ссылаются на common__* из docs/common.md целевого проекта; файл не генерируется.
Raw-DDL исполняется под ADB_USER из .env; затем обязательный гармонизатор.
Схемы raw/prep/core должны быть созданы заранее; утилита схемы не создает.
raw_comments.sql содержит комментарии raw-таблиц из фиксированного шаблона;
при --apply-ddl они применяются после создания таблиц, перед гармонизатором, в той же транзакции.
Генерация не заменяет сверку данных валидатором.
Файл .env нельзя коммитить или передавать другим людям.
При запуске .env ищется рядом со скриптом, независимо от текущей папки.
Уже заданные переменные окружения имеют приоритет над .env; ${...} не подставляется.
В результатах секретов нет.
"""

import argparse
import copy
from contextlib import contextmanager
import datetime as dt
import getpass
import json
import logging
import os
from pathlib import Path
import re
import sys
from uuid import UUID

from dotenv import load_dotenv
import psycopg2
import requests
import yaml


# =============================================================================
# ОБЛАСТЬ ДЛЯ ИЗМЕНЕНИЯ
# Заполните пустые строки и списки. Примеры есть только в комментариях.
# Технические значения по умолчанию (порты, TLS, stage, тип UP) можно оставить.
# Секретов в config нет: заполните .env рядом со скриптом по шаблону .env.example.
config = {
    # ==================== РЕЖИМ РАБОТЫ И ЛОКАЛЬНЫЙ РЕЗУЛЬТАТ ====================
    "generate_flows": True,  # True - сохранять JSON потоков локально.
    "apply_ddl_to_adb": False,  # True - создать raw-таблицы в готовой схеме и выполнить гармонизатор.
    "create_flows_via_api": False,  # True - создать потоки через API, не только JSON.
    "output_dir": "./pg2core_output",  # Локальный результат; не каталог dbt-проекта.

    # ==================== ИСТОЧНИК PG И МИГРИРУЮЩИЕ ТАБЛИЦЫ ====================
    "pg_host": "",  # Хост PostgreSQL; например "billy-pp-vbhca1".
    "pg_port": 5432,  # Порт локального чтения метаданных, проверить доступ; порты snapshot/oplog в DF могут отличаться.
    "pg_database": "",  # БД PostgreSQL; например "orders".
    "pg_schema_name": "",  # Исходная схема; например "public" (MetaInfo.sn = public.bills).
    "pg_tables_to_convert": [],  # Таблицы без схемы; например ["bills"].
    "pg_user_env": "PG_SOURCE_USER",  # Логин читателя из .env; пусто - текущий пользователь ОС.
    "pg_password_env": "PG_SOURCE_PASSWORD",  # Пароль читателя из .env; не попадает в результат.
    "pg_sslmode": "require",  # Режим TLS; "require" - требовать SSL. Выбрать по правилам подключения.
    "metadata_file": None,  # None - читать каталог PG; пример без подключения: "./metadata.json".
    "source_id": "",  # Идентификатор источника; например "billing_orders".

    # ==================== ПАРАМЕТРЫ ДЛЯ СОЗДАНИЯ ПОТОКОВ DATAFLOW ====================
    "dataflow_workspace_id": "",  # Пространство; например "billing"; также --workspace.
    "dataflow_topic_prefix": None,  # None - workspace; например "billing": топик billing/orders_bills_snapshot для БД orders и таблицы bills.
    "dataflow_flow_prefix": "",  # Префикс имени потока; например "billy_orders" -> billy_orders_bills_to_adb. Не зависит от source_id.
    "dataflow_snapshot_ttl_days": 7,  # Срок хранения НОВЫХ snapshot-топиков в днях; согласуйте перед запуском. Существующие не меняются.
    "dataflow_oplog_topic": "",  # Готовый общий oplog; например "billing/orders".
    "dataflow_pg_port": 8000,  # Порт snapshot-читателя; например 8000 для billy-pp-vbhca1.
    "dataflow_pg_credential": "",  # Имя PG credential в DF; например "kontur_billing_PP_v2hca", не пароль.
    "dataflow_adb_credential": "",  # Имя ADB credential в DF; например "adb-prod", не локальная учетка.
    "dataflow_stream_credential": "",  # Имя Stream credential; например "billing/*".
    "dataflow_target_database": None,  # None - adb_database; в потоке "db_dataflow". Должна совпадать с adb_database.
    "dataflow_endpoint": "Prod",  # Технический endpoint Stream; это НЕ target dbt.
    "dataflow_flow_type_id": "cbc7a8f6-19f7-468b-af26-eec13b02ed35",  # postgres-to-pg-table-UP.
    "dataflow_owner_id": "",  # Свой UUID владельца; в выгрузке "125a33a8-91c0-4090-8cd9-e7b54c5ac0c4".
    "dataflow_custom_team_id": None,  # UUID команды Bokrug; в выгрузке не указан, можно оставить None.
    "dataflow_yt_task_link": "",  # Задача миграции; например "https://yt.skbkontur.ru/issue/PD-7218".
    "dataflow_api_url": "https://dataflow.kontur.host",  # Адрес API инфраструктуры.
    "dataflow_identity_url": "https://identity.kontur.ru/connect/token",  # Identity.
    "dataflow_client_id_env": "DATAFLOW_CLIENT_ID",  # Client ID из .env; например "pd_core_dataflow".
    "dataflow_identity_secret_env": "DATAFLOW_CLIENT_SECRET",  # Client secret из .env.
    "dataflow_token_env": "DATAFLOW_TOKEN",  # Альтернатива client credentials: Bearer-токен из .env.
    "dataflow_request_timeout": 30,  # Таймаут HTTP в секундах; POST не повторяется.
    "stream_state_path": "st",  # Путь State reader: st либо ist; в JSON потока указан "ist", проверить свой reader.

    # ==================== ПАРАМЕТРЫ ДЛЯ СОЗДАНИЯ ОБЪЕКТОВ В ADB (DDL) ====================
    "adb_host": "",  # Хост ADB/Greenplum; например "adb-prod".
    "adb_port": 6432,  # Порт ADB; например 6432, используется также в target потока.
    "adb_database": "",  # БД создания raw; в target потока "db_dataflow", должна совпадать с dataflow_target_database.
    "adb_user_env": "ADB_USER",  # Обязательный логин ADB из .env, без подстановки пользователя ОС.
    "adb_password_env": "ADB_PASSWORD",  # Пароль указанного пользователя ADB из .env.
    "adb_prompt_password": False,  # Например True - запросить пароль пользователя ADB_USER в консоли.
    "adb_sslmode": "require",  # "require" - требовать SSL; без явного пароля работают настроенные libpq/GSS/.pgpass.
    "adb_raw_schema": "",  # Сырой слой; например "core_stg_billing_orders".
    "adb_prep_schema": "",  # Prep-схема; например "core_stg_prep_billing_orders".
    "adb_core_schema": "",  # Core-схема; например "core_billing_orders".
    "partition_start": None,  # None - текущий UTC-месяц; например "2026-10-01".
    "partition_months": 18,  # Например 15: от 2026-10-01 до 2028-01-01; DEFAULT создается отдельно.
    # Raw: AO column, zstd/4, DISTRIBUTED BY (PK). Prep/core создает dbt.
    # После DDL ОБЯЗАТЕЛЕН imdb.f_harmonize_object_grants для каждой raw-таблицы.

    # ==================== ПАРАМЕТРЫ ДЛЯ ГЕНЕРАЦИИ МОДЕЛЕЙ DBT ====================
    "dbt_project_name": "",  # Имя проекта для tags/selectors; например "billing_pg".
    "model_prefix": "",  # Префикс имен; например "billing_orders" -> billing_orders__bills.
    "model_directory": "",  # Подкаталог models/sources/docs; например "orders".
    "core_alias_suffix": "_new",  # Например bills_new и vw_bills_new; "" - без суффикса.
    # Ключ из PK; при отсутствии PK укажите primary_key в table_overrides.
    # Пример: {"bills": {"primary_key": ["id"],
    #                   "delete_key_paths": {"id": "[0].id"},
    #                   "delete_entity_path": "[0]", "snapshot_topic": "billing/orders_bills_snapshot",
    #                   "core_expressions": {"amount": "(_entity ->> 'amount')::numeric(18,2)"}}}
    "table_overrides": {},  # Для bills с PK id можно оставить {}; пример выше - необязательные переопределения.
    # Модели, sources, docs, properties и selectors сохраняются ЛОКАЛЬНО.
    # Макросы не копируются: переиспользуются существующие макросы целевого проекта.
    # Перенос в существующий проект, CHANGELOG, dbt deps/build и MR делает пользователь.

}
# КОНЕЦ ОБЛАСТИ ДЛЯ ИЗМЕНЕНИЯ
# =============================================================================

LOG = logging.getLogger("pg2core")
IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")
RAW_FIELDS = {
    "_is_deleted": "boolean", "_entity": "jsonb", "_op_type": "text",
    "_byte_offset": "bigint", "_skip_count": "bigint", "_ldts": "timestamptz",
    "_event_source": "smallint",
}
PREP_FIELDS = {**RAW_FIELDS, "_prev_ldts": "timestamptz", "_dbt_invocation_id": "text"}
CORE_FIELDS = {k: v for k, v in PREP_FIELDS.items()
               if k not in ("_entity", "_op_type", "_event_source", "_dbt_invocation_id")}


class MetadataValidationError(ValueError):
    """В метаданных отсутствует описание запрошенной таблицы."""


def model_identifier(value):
    # Логическое имя dbt-модели: физическое имя отдельно задается через alias.
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise ValueError(f"Нужно имя в lower_snake_case: {value!r}")
    return value


def identifier(value):
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value) or len(value.encode()) > 63:
        raise ValueError(f"Нужно имя в lower_snake_case длиной до 63 байт: {value!r}")
    return value


def qi(value):
    return '"' + identifier(value) + '"'


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def ydump(value):
    class DocsDumper(yaml.SafeDumper):
        pass

    def represent_string(dumper, text):
        # Двойные кавычки вокруг doc-ссылки сохраняют читаемую запись doc('имя').
        style = '"' if text.startswith("{{ doc(") and text.endswith(" }}") else None
        return dumper.represent_scalar("tag:yaml.org,2002:str", text, style=style)

    DocsDumper.add_representer(str, represent_string)
    return yaml.dump(value, Dumper=DocsDumper, allow_unicode=True, sort_keys=False, width=100)


def doc(name):
    return "{{ doc('" + name + "') }}"


def normalize_type(value):
    value = re.sub(r",\s*", ",", value)
    value = re.sub(r"^varchar\(", "character varying(", value)
    value = re.sub(r"^char\(", "character(", value)
    aliases = {"int2": "smallint", "int4": "integer", "int8": "bigint", "bool": "boolean",
               "float4": "real", "float8": "double precision", "varchar": "character varying",
               "timestamp": "timestamp without time zone", "timestamptz": "timestamp with time zone"}
    return aliases.get(value, value)


def safe_type(value):
    value = normalize_type(value.strip().lower())
    scalar = ("uuid", "text", "boolean", "smallint", "integer", "bigint", "real",
              "double precision", "date", "json", "jsonb", "bytea", "character varying", "character", "interval")
    if value in scalar or re.fullmatch(
        r"(?:numeric|decimal)(?:\(\d+(?:,\s*\d+)?\))?|"
        r"(?:character varying|character|varchar|char)\(\d+\)|"
        r"(?:timestamp|time)(?:\(\d+\))? (?:with|without) time zone", value
    ):
        return value
    raise ValueError(f"Тип {value!r} требует явного поддерживаемого data_type/core_expression")


def load_secrets(env_path=None):
    """Читаем .env рядом со скриптом; не печатаем значения и не перезаписываем окружение."""
    path = Path(env_path) if env_path is not None else Path(__file__).resolve().with_name(".env")
    load_dotenv(dotenv_path=path, override=False, interpolate=False, encoding="utf-8-sig")


@contextmanager
def connection(cfg, prefix):
    user = os.environ.get(cfg[prefix + "_user_env"])
    if not user:
        if prefix == "adb":
            raise ValueError("Заполните " + cfg["adb_user_env"] + " в .env")
        user = getpass.getuser()
    password = os.environ.get(cfg[prefix + "_password_env"])
    if prefix == "adb" and cfg["adb_prompt_password"] and not password:
        password = getpass.getpass(f"Пароль ADB для {user}: ")
    if not cfg[prefix + "_host"]:
        raise ValueError(f"Заполните {prefix}_host")
    kwargs = dict(
        host=cfg[prefix + "_host"], port=cfg[prefix + "_port"],
        dbname=cfg[prefix + "_database"], user=user,
        sslmode=cfg[prefix + "_sslmode"], connect_timeout=10,
    )
    # Без явного пароля libpq может использовать .pgpass/GSS/SSPI, если они настроены.
    if password:
        kwargs["password"] = password
    LOG.info("Подключение %s: host=%s port=%s database=%s user=%s sslmode=%s",
             prefix.upper(), kwargs["host"], kwargs["port"], kwargs["dbname"], user, kwargs["sslmode"])
    conn = psycopg2.connect(**kwargs)
    try:
        with conn:
            yield conn
    finally:
        conn.close()


class PostgresMetadataReader:
    def __init__(self, cfg):
        self.cfg = cfg

    def read(self):
        if self.cfg["metadata_file"]:
            return json.loads(Path(self.cfg["metadata_file"]).read_text(encoding="utf-8-sig"))
        result = {}
        with connection(self.cfg, "pg") as conn:
            conn.set_session(readonly=True)
            with conn.cursor() as cur:
                cur.execute("set local statement_timeout = 60000")
                for table in self.cfg["pg_tables_to_convert"]:
                    cur.execute("""
                        select a.attname, format_type(a.atttypid, a.atttypmod),
                               col_description(c.oid, a.attnum)
                        from pg_class c join pg_namespace n on n.oid = c.relnamespace
                        join pg_attribute a on a.attrelid = c.oid
                        where n.nspname = %s and c.relname = %s
                          and c.relkind in ('r', 'p') and a.attnum > 0 and not a.attisdropped
                        order by a.attnum
                    """, (self.cfg["pg_schema_name"], table))
                    columns = [{"name": n, "data_type": t, "description": d or n}
                               for n, t, d in cur.fetchall()]
                    cur.execute("""
                        select a.attname from pg_index i
                        join pg_class c on c.oid = i.indrelid
                        join pg_namespace n on n.oid = c.relnamespace
                        cross join lateral unnest(i.indkey) with ordinality as k(attnum, ord)
                        join pg_attribute a on a.attrelid = c.oid and a.attnum = k.attnum
                        where n.nspname = %s and c.relname = %s and i.indisprimary
                          and k.ord <= i.indnkeyatts
                        order by k.ord
                    """, (self.cfg["pg_schema_name"], table))
                    result[table] = {"columns": columns, "primary_key": [r[0] for r in cur.fetchall()]}
        return result


class DataflowApiClient:
    """Не изменяет существующие потоки и не повторяет POST после таймаута."""

    def __init__(self, cfg):
        self.cfg = cfg
        self.session = requests.Session()
        token = os.environ.get(cfg["dataflow_token_env"])
        if not token:
            client_id = os.environ.get(cfg["dataflow_client_id_env"])
            secret = os.environ.get(cfg["dataflow_identity_secret_env"])
            if not client_id or not secret:
                raise ValueError("Заполните DATAFLOW_CLIENT_ID и DATAFLOW_CLIENT_SECRET либо DATAFLOW_TOKEN в .env")
            response = self.session.post(cfg["dataflow_identity_url"], data={
                "client_id": client_id, "client_secret": secret,
                "grant_type": "client_credentials",
            }, timeout=cfg["dataflow_request_timeout"])
            if response.status_code != 200:
                try:
                    body = response.json()
                except ValueError:
                    body = {}
                details = f"Identity: HTTP {response.status_code}"
                if isinstance(body, dict):
                    for key in ("error", "error_description"):
                        value = body.get(key)
                        if isinstance(value, str):
                            # Не выводим ответ целиком: он может содержать токены/секреты.
                            for env_key in ("pg_password_env", "adb_password_env",
                                            "dataflow_identity_secret_env", "dataflow_token_env"):
                                credential = os.environ.get(cfg[env_key], "")
                                if credential:
                                    value = value.replace(credential, "***")
                            details += f"; {key}={value[:500]}"
                raise RuntimeError(details)
            token = response.json()["access_token"]
        self.session.headers.update({"Authorization": "Bearer " + token, "Accept": "application/json"})

    def request(self, method, path, **kwargs):
        response = self.session.request(method, self.cfg["dataflow_api_url"].rstrip("/") + path,
                                        timeout=self.cfg["dataflow_request_timeout"], **kwargs)
        if not response.ok:
            # Ответ может содержать секреты; не выводим его в лог/отчет.
            raise RuntimeError(f"Dataflow {method} {path}: HTTP {response.status_code}")
        return response.json() if response.content and response.text.strip() else None

    def preflight_topics(self, flows):
        # Список читаем постранично: отсутствие топика не определяем по 403/404.
        snapshots = set()
        oplogs = set()
        for flow in flows:
            snapshots.add(flow["content"]["SnapshotBufferConfig"]["Topic"])
            oplogs.add(flow["content"]["OplogBufferConfig"]["Topic"])
        if snapshots & oplogs:
            raise ValueError("Snapshot-топик не должен совпадать с oplog-топиком")
        grouped = {}
        for full_name in sorted(snapshots | oplogs):
            parts = full_name.split("/")
            if (len(parts) != 2 or not re.fullmatch(r"[a-zA-Z0-9_-]+", parts[0])
                    or not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", parts[1])
                    or len(parts[1]) > 150):
                raise ValueError("Ожидается топик пространство/имя: " + full_name)
            grouped.setdefault(parts[0], set()).add(parts[1])
        available = set()
        for workspace in sorted(grouped):
            skip = 0
            while True:
                page = self.request("GET", "/api/topics", params={
                    "WorkspaceId": workspace, "skip": skip, "take": 100})
                if (not isinstance(page, dict) or not isinstance(page.get("items"), list)
                        or not isinstance(page.get("totalCount"), int)):
                    raise ValueError("Некорректный ответ API /api/topics")
                items = page["items"]
                for item in items:
                    available.add(workspace + "/" + item["topicName"])
                skip += len(items)
                if skip >= page["totalCount"]:
                    break
                if not items:
                    raise ValueError("API /api/topics вернул пустую промежуточную страницу")
        missing_oplogs = oplogs - available
        if missing_oplogs:
            raise ValueError("Не найдены готовые oplog-топики: " + ", ".join(sorted(missing_oplogs)))
        return [(name, name in available) for name in sorted(snapshots)]

    def create_snapshot_topic(self, full_name):
        workspace, name = full_name.split("/")
        return self.request("POST", "/api/topics", json={
            "workspaceId": workspace,
            "topicInfo": {"topicName": name, "meta": {
                "description": "Snapshot: " + full_name,
                "ttlValue": self.cfg["dataflow_snapshot_ttl_days"]}}})

    def preflight(self, flows):
        current = self.request("GET", "/api/flows/all", params={
            "WorkspaceId": self.cfg["dataflow_workspace_id"],
            "FlowTypeId": self.cfg["dataflow_flow_type_id"],
        })
        if not isinstance(current, list):
            raise ValueError("API /api/flows/all вернул не список")
        existing = {}
        for flow in flows:
            matches = [x for x in current if x["flowName"] == flow["flowName"]]
            if len(matches) > 1:
                raise ValueError("Неоднозначное имя потока: " + flow["flowName"])
            if not matches:
                continue
            remote = self.request("GET", "/api/flows/" + matches[0]["id"])
            if (remote.get("isArchived") or remote.get("content") != flow["content"]
                    or remote.get("workspaceId") != flow["workspaceId"]
                    or remote.get("flowTypeId") != flow["flowTypeId"]):
                raise ValueError("Поток уже существует с другим конфигом: " + flow["flowName"])
            existing[flow["flowName"]] = matches[0]["id"]
        return existing

    def create_flow(self, flow):
        return self.request("POST", "/api/flows", json=flow)


class AdbClient:
    def __init__(self, cfg):
        self.cfg = cfg

    @staticmethod
    def inspect(cur, table):
        cur.execute("""select a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull
                       from pg_attribute a join pg_class c on c.oid = a.attrelid
                       join pg_namespace n on n.oid = c.relnamespace
                       where n.nspname = %s and c.relname = %s and c.relkind in ('r', 'p')
                         and a.attnum > 0 and not a.attisdropped order by a.attnum""",
                    (table["raw_schema"], table["name"]))
        found = cur.fetchall()
        expected = {**{c["name"]: c["data_type"] for c in table["columns"] if c["name"] in table["keys"]},
                    **RAW_FIELDS}
        if found and ({n: normalize_type(t) for n, t, _ in found} !=
                      {n: normalize_type(t) for n, t in expected.items()} or not all(v for _, _, v in found)):
            raise ValueError(f"Существующий raw {table['raw_schema']}.{table['name']} не совпадает с планом")
        return bool(found)

    def prepare(self, tables, ddl, apply, comments):
        if apply and len(comments) != len(tables):
            raise ValueError("Для каждой raw-таблицы нужен блок комментариев")
        with connection(self.cfg, "adb") as conn:
            with conn.cursor() as cur:
                # Задаем таймаут SQL-командой: прокси может запрещать startup options.
                cur.execute("set local statement_timeout = 60000")
                cur.execute("select current_user, session_user")
                current_user, session_user = cur.fetchone()
                if current_user != os.environ.get(self.cfg["adb_user_env"]) or session_user != current_user:
                    raise ValueError("ADB-сессия должна работать под логином из " + self.cfg["adb_user_env"] + ", без смены роли")
                LOG.info("ADB: current_user=%s", current_user)
                exists = []
                for t in tables:
                    LOG.info("ADB: проверка raw %s.%s", t["raw_schema"], t["name"])
                    exists.append(self.inspect(cur, t))
                if not apply and not all(exists):
                    raise ValueError("Сначала создайте raw-таблицы: --apply-ddl или согласованный DDL вручную")
                if apply:
                    # Весь raw-DDL одного плана применяется одной транзакцией, без DROP/TRUNCATE.
                    for statement in ddl:
                        LOG.info("ADB: выполнение DDL: %s", statement.strip().splitlines()[0])
                        cur.execute(statement)
                    for i, t in enumerate(tables):
                        LOG.info("ADB: проверка raw после DDL %s.%s", t["raw_schema"], t["name"])
                        self.inspect(cur, t)
                        LOG.info("ADB: применение комментариев %s.%s", t["raw_schema"], t["name"])
                        cur.execute(comments[i])
                        LOG.info("ADB: гармонизатор: схема=%s, таблица=%s", t["raw_schema"], t["name"])
                        cur.execute("select imdb.f_harmonize_object_grants(%s, %s)",
                                    (t["raw_schema"], t["name"]))


class PostgresToCoreConverter:
    def __init__(self, cfg):
        self.cfg = copy.deepcopy(cfg)
        self.files = {}
        self.tables = []
        self.flows = []
        self.ddl = []
        self.selectors = []

    def validate(self):
        c = self.cfg
        required = ["pg_database", "pg_schema_name", "source_id", "model_prefix", "model_directory",
                    "dbt_project_name", "adb_database", "adb_raw_schema", "adb_prep_schema", "adb_core_schema"]
        if c["generate_flows"]:
            required += ["pg_host", "adb_host", "dataflow_workspace_id", "dataflow_oplog_topic",
                         "dataflow_pg_credential", "dataflow_adb_credential", "dataflow_stream_credential",
                         "dataflow_owner_id", "dataflow_flow_prefix"]
        missing = [key for key in required if not c[key]]
        if missing:
            raise ValueError("Заполните параметры config: " + ", ".join(missing))
        for key in ("generate_flows", "apply_ddl_to_adb", "create_flows_via_api", "adb_prompt_password"):
            if not isinstance(c[key], bool):
                raise ValueError(key + ": нужно true/false, а не строка")
        for k in ("source_id", "model_prefix", "model_directory", "pg_schema_name", "dbt_project_name",
                  "adb_raw_schema", "adb_prep_schema", "adb_core_schema"):
            identifier(c[k])
        tables = c["pg_tables_to_convert"]
        if not isinstance(tables, list) or not tables or len(set(tables)) != len(tables):
            raise ValueError("Нужен непустой список таблиц без повторов")
        for t in tables:
            identifier(t)
        if set(c["table_overrides"]) - set(tables):
            raise ValueError("table_overrides содержит таблицы, не включенные в pg_tables_to_convert")
        if len({c[k] for k in ("adb_raw_schema", "adb_prep_schema", "adb_core_schema")}) != 3:
            raise ValueError("Raw, prep и core должны иметь разные схемы")
        if c["generate_flows"] and not re.fullmatch(r"[a-zA-Z0-9_-]+", c["dataflow_workspace_id"]):
            raise ValueError("Укажите dataflow_workspace_id")
        if not isinstance(c["partition_months"], int) or not 1 <= c["partition_months"] <= 120:
            raise ValueError("partition_months: от 1 до 120")
        if not isinstance(c["core_alias_suffix"], str):
            raise ValueError("Некорректный core_alias_suffix")
        if c["create_flows_via_api"] and not c["generate_flows"]:
            raise ValueError("create_flows_via_api требует generate_flows")
        if c["generate_flows"]:
            identifier(c["dataflow_flow_prefix"])
            ttl = c["dataflow_snapshot_ttl_days"]
            if isinstance(ttl, bool) or not isinstance(ttl, int) or ttl < 1:
                raise ValueError("dataflow_snapshot_ttl_days: положительное целое число дней")
            for k in ("pg_host", "adb_host", "dataflow_pg_credential", "dataflow_adb_credential",
                      "dataflow_stream_credential", "dataflow_oplog_topic", "dataflow_owner_id"):
                if not c[k]:
                    raise ValueError("Заполните " + k)
            for key in ("dataflow_flow_type_id", "dataflow_owner_id", "dataflow_custom_team_id"):
                if c[key]:
                    UUID(c[key])
            if c["stream_state_path"] not in ("st", "ist"):
                raise ValueError("stream_state_path: поддерживаются st или ist; проверьте ваш reader")
        if (c["dataflow_target_database"] or c["adb_database"]) != c["adb_database"]:
            raise ValueError("ADB DDL и Dataflow должны указывать одну БД; межбазовый mapping не автоматизирован")

    def add_file(self, path, content):
        if path in self.files:
            raise ValueError("Коллизия генерируемых файлов: " + path)
        self.files[path] = content.rstrip() + "\n"

    def table_metadata(self, name, meta):
        override = self.cfg["table_overrides"].get(name, {})
        columns = copy.deepcopy(meta.get("columns", []))
        keys = override.get("primary_key", meta.get("primary_key", []))
        if not isinstance(keys, list) or not keys or len(keys) != len(set(keys)):
            raise ValueError(name + ": нужен primary_key (непустой список без повторов)")
        names = [x["name"] for x in columns]
        if not columns or len(names) != len(set(names)) or not set(keys) <= set(names):
            raise ValueError(name + ": неверные колонки или ключ отсутствует")
        for col in columns:
            identifier(col["name"])
            if col["name"].startswith("_"):
                raise ValueError("Коллизия со служебным полем: " + col["name"])
            col["data_type"] = safe_type(col["data_type"])
            col["description"] = col.get("description") or col["name"]
        for key in keys:
            t = next(x["data_type"] for x in columns if x["name"] == key)
            if t not in ("uuid", "text", "smallint", "integer", "bigint", "character varying", "character") and not re.fullmatch(r"character(?: varying)?\(\d+\)", t):
                raise ValueError(f"Ключ {key}: неподдерживаемый тип {t}; нужен проверенный UP-конвертер")
        if set(override.get("core_expressions", {})) - set(names):
            raise ValueError("core_expressions содержит неизвестные поля")
        alias = identifier(name + self.cfg["core_alias_suffix"])
        model = model_identifier(self.cfg["model_prefix"] + "__" + name)
        return {"name": name, "columns": columns, "keys": keys, "override": override,
                "model": model, "prep_model": model_identifier(model + "_snapshot"),
                "view_model": model_identifier(self.cfg["model_prefix"] + "__vw_" + name),
                "alias": alias, "view_alias": identifier("vw_" + alias),
                "raw_schema": self.cfg["adb_raw_schema"]}

    def raw_ddl(self, t):
        start = dt.date.fromisoformat(self.cfg["partition_start"]) if self.cfg["partition_start"] else dt.datetime.now(dt.timezone.utc).date().replace(day=1)
        if start.day != 1:
            raise ValueError("partition_start должен быть первым днем месяца")
        months = start.year * 12 + start.month - 1 + self.cfg["partition_months"]
        end = dt.date(months // 12, months % 12 + 1, 1)
        fields = [(c["name"], c["data_type"]) for c in t["columns"] if c["name"] in t["keys"]]
        fields += list(RAW_FIELDS.items())
        body = ",\n    ".join(f"{qi(n)} {typ} not null" for n, typ in fields)
        distribution = ", ".join(qi(k) for k in t["keys"])
        return (f"create table if not exists {qi(t['raw_schema'])}.{qi(t['name'])} (\n    {body}\n)\n"
                "with (appendonly=true, orientation=column, compresstype=zstd, compresslevel=4)\n"
                f"distributed by ({distribution})\npartition by range (_ldts) (\n"
                f"    start ('{start} 00:00:00+00'::timestamptz) inclusive\n"
                f"    end ('{end} 00:00:00+00'::timestamptz) exclusive\n"
                "    every (interval '1 month'), default partition other\n);\n")

    def raw_comments(self, t):
        relation = qi(t["raw_schema"]) + "." + qi(t["name"])
        description = (
            "Сырые snapshot/oplog-события таблицы " + self.cfg["pg_schema_name"] + "." + t["name"]
            + " из PostgreSQL. Тело события хранится в JSONB-поле `_entity`; "
            "таблица содержит историю поступивших событий, включая удаления.")
        # Фиксированные тексты из шаблона; комментарии PostgreSQL не используются.
        columns = [(key, key) for key in t["keys"]] + [
            ("_is_deleted", "Признак операции удаления. `true` — запись удалена, `false` — запись активна."),
            ("_entity", "Сырой JSONB записи/сущности."),
            ("_op_type", "Тип операции события. Для PG через snapshot/oplog: `Insert`, `InsertOrUpdate`, `Update`, `Delete`, `Deleted`."),
            ("_byte_offset", "Byte offset события в промежуточном хранилище/stream. Используется для порядка обработки и инкрементальной загрузки stream-подобных источников. Не заменяет `_src_offset`."),
            ("_skip_count", "Количество пропущенных строк/событий с начала offset-а в рамках батча. Используется вместе с `_byte_offset` для стабильной сортировки событий."),
            ("_ldts", "Время записи/загрузки строки на текущем слое. Тип `timestamptz`."),
            ("_event_source", "Код источника события внутри PG-потока: `0 = snapshot`, `1 = oplog`."),
        ]
        statements = ["COMMENT ON TABLE " + relation + " IS\n" + literal(description) + ";"]
        statements += ["COMMENT ON COLUMN " + relation + "." + qi(name) + " IS\n"
                       + literal(text) + ";" for name, text in columns]
        return "\n\n".join(statements)

    def mapping(self, t, snapshot_topic, oplog_topic):
        def extract(field, paths, source_type="JObject"):
            return {"FieldName": field, "SourceType": source_type,
                    "ValueExtractionConfigs": [{"Path": p, "TargetName": n} for p, n in paths]}
        def mapping(items):
            return {"Type": "Mapping", "MappingConfig": items}
        def field(target, source=None, **kwargs):
            return {"TargetName": target, "FieldSourceName": source or target, **kwargs}
        keys = t["keys"]
        paths = t["override"].get("delete_key_paths", {})
        ops = [
            {"Type": "ReadEvent"},
            {"Type": "FieldsWhiteList", "Fields": ["FieldList", "KeyFields", "Type", "MetaInfo"]},
            {"Type": "Extract", "ExtractionConfigs": [extract("MetaInfo", [("sn", "_meta_sn")])]},
            {"Type": "Filter", "Filter": {"_meta_sn": self.cfg["pg_schema_name"] + "." + t["name"]}},
            {"Type": "Extract", "ExtractionConfigs": [
                extract("FieldList", [(k, "key_raw_" + str(i)) for i, k in enumerate(keys)] + [("$", "entity_raw")]),
                extract("KeyFields", [(paths.get(k, "[0]." + k), "key_deleted_" + str(i)) for i, k in enumerate(keys)]
                        + [(t["override"].get("delete_entity_path", "[0]"), "entity_deleted")])]},
            {"Type": "MetaExtract", "ValueExtractionConfigs": [
                {"Path": "ti", "TargetName": "_source_topic"},
                {"Path": self.cfg["stream_state_path"], "TargetName": "State"}]},
            {"Type": "Extract", "ExtractionConfigs": [extract("State", [
                ("ByteOffset", "_byte_offset"), ("SkipCount", "_skip_count")], "Object")]},
            mapping([field("entity_raw_json", "entity_raw", Converter="JsonToString"),
                     field("entity_deleted_json", "entity_deleted", Converter="JsonToString")]),
            mapping([{"Converter": "Coalesce", "TargetName": k,
                      "FieldSourceNames": ["key_deleted_" + str(i), "key_raw_" + str(i)]}
                     for i, k in enumerate(keys)] + [
                         {"Converter": "Coalesce", "TargetName": "_entity",
                          "FieldSourceNames": ["entity_deleted_json", "entity_raw_json"]}]),
        ]
        converted = [field(k, **({"Converter": "StringToGuid"} if
                    next(c["data_type"] for c in t["columns"] if c["name"] == k) == "uuid" else {})) for k in keys]
        ops += [mapping(converted + [
            field("_is_deleted", "Type", Converter="KeyToValue", MapValues={
                "Insert": False, "InsertOrUpdate": False, "Update": False, "Delete": True, "Deleted": True}),
            field("_entity"), field("_op_type", "Type"),
            field("_event_source", "_source_topic", Converter="KeyToValue", MapValues={snapshot_topic: 0, oplog_topic: 1}),
            field("_byte_offset"), field("_skip_count"), {"Converter": "CurrentDate", "TargetName": "_ldts"}]),
            {"Type": "FieldsWhiteList", "Fields": keys + list(RAW_FIELDS), "IgnoreCase": False},
            {"Type": "EventToAbstractOperation"}]
        return {"Operators": ops}

    def flow(self, t):
        c = self.cfg
        prefix = c["dataflow_topic_prefix"] or c["dataflow_workspace_id"]
        snapshot = t["override"].get("snapshot_topic", prefix + "/" + c["pg_database"] + "_" + t["name"] + "_snapshot")
        oplog = c["dataflow_oplog_topic"]
        if snapshot == oplog:
            raise ValueError("Snapshot и oplog должны иметь разные topics")
        name = c["dataflow_flow_prefix"] + "_" + t["name"] + "_to_adb"
        if len(name) > 150:
            raise ValueError("Слишком длинное имя потока")
        payload = {
            "workspaceId": c["dataflow_workspace_id"], "flowTypeId": c["dataflow_flow_type_id"], "flowName": name,
            "meta": {"owner": c["dataflow_owner_id"], "description": c["dataflow_yt_task_link"],
                     "tags": [c["source_id"], "up", "jsonb"]},
            "dispatchInfo": {"waitSecondsBeforeNextIteration": 1800}, "decompositionParameters": {},
            "content": {
                "SourceInfo": {"ReadBatchSize": 10000, "IsCrossDomain": False,
                    "CredentialName": c["dataflow_pg_credential"], "PgProperties": {
                        "PgServerName": c["pg_host"], "PgPort": c["dataflow_pg_port"],
                        "PgDatabaseName": c["pg_database"], "PgSchemaName": c["pg_schema_name"],
                        "PgTableName": t["name"], "PgCommandTimeout": 30}},
                "TargetInfo": {"PgTargetConfiguration": {"ServerProperties": {
                    "ServerName": c["adb_host"], "Port": c["adb_port"],
                    "DatabaseName": c["dataflow_target_database"] or c["adb_database"], "TableName": t["raw_schema"] + "." + t["name"],
                    "CommandsTimeoutInSeconds": 600, "SessionTimeoutInSeconds": 3600},
                    "IgnoredColumns": [], "UniqueColumnNames": [], "IsHistoryTarget": False,
                    "HistoryTargetConfiguration": {"IsDeletedColumnName": "_is_deleted",
                        "VersionColumnName": "df_version", "InsertTimeColumnName": "df_valid_ts"},
                    "DataDistributionRule": "AsTable", "UnknownColumnRule": "Skip", "CaseSensitiveKeyComparer": True},
                    "IsMergeRequired": False, "BufferBatchSize": 50000,
                    "OnSaveFailRetryStrategy": {"AttemptsCount": 3, "RetryDelayInMinutes": 1},
                    "CredentialName": c["dataflow_adb_credential"]},
                "SnapshotBufferConfig": {"BatchSize": 10000, "TimeoutInSeconds": 90,
                    "OnSaveFailRetryStrategy": {"AttemptsCount": 3, "RetryDelayInMinutes": 3},
                    "Topic": snapshot, "Endpoint": c["dataflow_endpoint"], "CredentialName": c["dataflow_stream_credential"]},
                "OplogBufferConfig": {"BatchSize": 1000, "Topic": oplog, "CredentialName": c["dataflow_stream_credential"]},
                "MappingInfo": self.mapping(t, snapshot, oplog),
            },
        }
        if c["dataflow_custom_team_id"]:
            payload["customBokrugTeamId"] = c["dataflow_custom_team_id"]
        return payload

    def models(self, t):
        c = self.cfg
        keys = t["keys"]
        key_sql = ", ".join(qi(k) for k in keys)
        source = "source(" + repr(c["adb_raw_schema"]) + ", " + repr(t["name"]) + ")"
        prep_sql = ("{{ postgres_snapshot_macros(" + source + ") }}\n\nselect\n    now()::timestamptz as _ldts,\n    "
                    + key_sql + ",\n    _is_deleted, _entity, _op_type, _byte_offset, _skip_count,\n"
                    "    _ldts as _prev_ldts, _event_source,\n    '{{ invocation_id }}'::text as _dbt_invocation_id\nfrom enriched_events\n")
        selections = []
        for col in t["columns"]:
            name = col["name"]
            if name in keys:
                expr = qi(name)
            else:
                op = "->" if col["data_type"] in ("json", "jsonb") else "->>"
                expr = f"(_entity {op} {literal(name)})::{col['data_type']}"
            expr = t["override"].get("core_expressions", {}).get(name, expr)
            selections.append(expr + " as " + qi(name))
        core_sql = ("with source_data as (\n    select * from {{ ref('" + t["prep_model"] + "') }}\n"
                    "    {% if is_incremental() %}\n    where _ldts > (select coalesce(max(_prev_ldts),\n"
                    "        '1900-01-01'::timestamptz) from {{ this }})\n    {% endif %}\n)\nselect\n    "
                    + ",\n    ".join(selections) + ",\n    _is_deleted, _byte_offset, _skip_count,\n"
                    "    _ldts as _prev_ldts, now()::timestamptz as _ldts\nfrom source_data\n")
        view_sql = ("select " + ", ".join(qi(x["name"]) for x in t["columns"])
                    + "\nfrom {{ ref('" + t["model"] + "') }}\nwhere not _is_deleted\n")
        def props(name, schema, alias, fields, layer):
            cfg = {"materialized": "safe_view" if layer == "view" else "incremental",
                   "schema": schema, "alias": alias, "tags": [c["dbt_project_name"],
                       "stage" if layer == "prep" else c["adb_core_schema"]],
                   "persist_docs": {"relation": True, "columns": True}}
            if layer != "prep":
                cfg["access"] = "public"
            if layer == "core":
                cfg["contract"] = {"enforced": True}
            if layer != "view":
                cfg.update({"unique_key": keys[0] if len(keys) == 1 else keys,
                            "incremental_strategy": "delete+insert", "on_schema_change": "fail",
                            "distributed_by": ", ".join(keys)})
                not_null = keys + list(PREP_FIELDS if layer == "prep" else CORE_FIELDS)
                cfg["post_hook"] = ["{% if not is_incremental() %} alter table {{ this }} "
                                    + ", ".join("alter column " + qi(n) + " set not null" for n in not_null)
                                    + " {% endif %}"]
            model = {"name": name, "description": doc(name), "config": cfg, "columns": fields}
            tests = []
            if layer != "view" and len(keys) > 1:
                tests.append({"dbt_utils.unique_combination_of_columns": {"arguments": {"combination_of_columns": keys}}})
            if layer == "prep":
                tests += [{n: {"arguments": {"source_table": source, "key_columns": keys},
                                "config": {"tags": ["stage_test"], "severity": "warn"}}}
                          for n in ("snapshot_missing_keys", "snapshot_orphan_keys")]
            if tests:
                model["data_tests"] = tests
            return ydump({"models": [model]})
        def col_props(col, layer):
            name = col["name"]
            service = name.startswith("_")
            block = "common_" + name if service else t["model"] + "__" + name
            item = {"name": name, "data_type": col["data_type"], "description": doc(block)}
            if layer != "view" and (service or name in keys):
                item["data_tests"] = ["not_null"]
                if len(keys) == 1 and name in keys:
                    item["data_tests"].append("unique")
                if name == "_event_source":
                    item["data_tests"].append({"accepted_values": {"arguments": {"values": [0, 1]}}})
            return item
        prep_cols = [x for x in t["columns"] if x["name"] in keys] + [
            {"name": n, "data_type": typ} for n, typ in PREP_FIELDS.items()]
        core_cols = t["columns"] + [{"name": n, "data_type": typ} for n, typ in CORE_FIELDS.items()]
        base = "models/" + c["model_directory"] + "/"
        for name, schema, alias, sql, fields, layer in [
            (t["prep_model"], c["adb_prep_schema"], t["name"], prep_sql, prep_cols, "prep"),
            (t["model"], c["adb_core_schema"], t["alias"], core_sql, core_cols, "core"),
            (t["view_model"], c["adb_core_schema"], t["view_alias"], view_sql, t["columns"], "view")]:
            self.add_file(base + schema + "/" + name + ".sql", sql)
            self.add_file(base + schema + "/properties/" + name + ".yml",
                          props(name, schema, alias, [col_props(x, layer) for x in fields], layer))
        source_cols = []
        for col in [x for x in t["columns"] if x["name"] in keys] + [
                {"name": n, "data_type": typ} for n, typ in RAW_FIELDS.items()]:
            item = {"name": col["name"], "data_type": col["data_type"],
                    "description": doc("common_" + col["name"] if col["name"].startswith("_")
                                       else t["model"] + "__" + col["name"]),
                    "data_tests": [{"not_null": {"config": {"tags": [c["dbt_project_name"] + "_source_p0"],
                                                 "where": "_ldts >= now() - interval '14 days'"}}}]}
            if col["name"] == "_event_source":
                item["data_tests"].append({"accepted_values": {"arguments": {"values": [0, 1]},
                    "config": {"tags": [c["dbt_project_name"] + "_source_p0"],
                               "where": "_ldts >= now() - interval '14 days'"}}})
            source_cols.append(item)
        self.add_file("sources/" + c["model_directory"] + "/_" + t["model"] + ".yml", ydump({"sources": [{
            "name": c["adb_raw_schema"], "database": "{{ target.database }}", "schema": c["adb_raw_schema"],
            "tables": [{"name": t["name"], "description": doc(t["model"] + "_stage"), "columns": source_cols,
                        "config": {"loaded_at_field": "_ldts", "freshness": {
                            "warn_after": {"count": 25, "period": "hour"},
                            "error_after": {"count": 49, "period": "hour"}}}}]}]}))
        blocks = [(t["model"] + "_stage",
                   "Сырые snapshot/oplog-события таблицы " + c["pg_schema_name"] + "." + t["name"]
                   + " из PostgreSQL. Тело события хранится в JSONB-поле `_entity`; "
                   "таблица содержит историю поступивших событий, включая удаления."),
                  (t["model"], "Типизированные данные, включая удаленные записи."),
                  (t["prep_model"], "Актуальное состояние JSONB; Delete сохраняется с последним известным телом."),
                  (t["view_model"], "Бизнес-поля активных записей; удаленные записи исключены.")]
        blocks += [(t["model"] + "__" + col["name"], col["description"]) for col in t["columns"]]
        self.add_file("docs/" + c["model_directory"] + "/" + t["model"] + ".md", "\n\n".join(
            "{% docs " + n + " %}\n" + str(desc).replace("{%", "&#123;%").replace("{{", "&#123;{")
            + "\n{% enddocs %}" for n, desc in blocks))

    def plan(self, metadata):
        self.validate()
        self.ddl = []
        for name in self.cfg["pg_tables_to_convert"]:
            if name not in metadata:
                raise MetadataValidationError("Метаданные таблицы не найдены: " + name)
            t = self.table_metadata(name, metadata[name])
            self.tables.append(t)
            self.ddl.append(self.raw_ddl(t))
            self.models(t)
            if self.cfg["generate_flows"]:
                self.flows.append(self.flow(t))
        model_names = [t[k] for t in self.tables for k in ("model", "prep_model", "view_model")]
        aliases = [t[k] for t in self.tables for k in ("alias", "view_alias")]
        if len(model_names) != len(set(model_names)) or len(aliases) != len(set(aliases)):
            raise ValueError("Коллизия имен моделей или core/view aliases между таблицами")
        p = self.cfg["dbt_project_name"]
        self.selectors = [
            {"name": p, "definition": {"method": "tag", "value": p}},
            {"name": p + "_stage", "description": "Prep-модели и тесты prep-слоя", "definition": {"union": [
                {"method": "tag", "value": "stage"},
                {"method": "tag", "value": "stage_test"}]}},
            {"name": p + "_" + self.cfg["adb_core_schema"], "definition": {
                "method": "tag", "value": self.cfg["adb_core_schema"]}},
            {"name": p + "_source_p0", "definition": {
                "method": "tag", "value": p + "_source_p0"}},
        ]
        return self



    def save_plan(self, output):
        output.mkdir(parents=True, exist_ok=True)
        artifacts = {"project/" + name: value for name, value in self.files.items()}
        artifacts["raw.sql"] = "\n\n".join(self.ddl) + "\n"
        artifacts["raw_comments.sql"] = "\n\n".join(self.raw_comments(t) for t in self.tables) + "\n"
        artifacts["selectors.proposed.yml"] = ydump({"selectors": self.selectors})
        artifacts["metadata.json"] = json.dumps({t["name"]: {
            "columns": t["columns"], "primary_key": t["keys"]} for t in self.tables}, ensure_ascii=False, indent=2) + "\n"
        for flow in self.flows:
            artifacts["flows/" + flow["flowName"] + ".json"] = json.dumps(flow, ensure_ascii=False, indent=2) + "\n"
        for name, content in artifacts.items():
            path = output / name
            if path.exists() and path.read_text(encoding="utf-8-sig") != content:
                raise ValueError("Другой план уже существует; выберите новый output_dir: " + str(path))
        for name, content in artifacts.items():
            path = output / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8", newline="\n")




def run(cfg):
    load_secrets()
    converter = PostgresToCoreConverter(cfg)
    converter.validate()
    converter.plan(PostgresMetadataReader(cfg).read())
    output = Path(cfg["output_dir"]).resolve()
    converter.save_plan(output)
    report = {"workspace": cfg["dataflow_workspace_id"],
              "tables": [t["name"] for t in converter.tables], "steps": []}
    report_path = output / ("execution_" + dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%f") + ".json")
    def record(step, **details):
        report["steps"].append({"step": step, **details})
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    try:
        api = DataflowApiClient(cfg) if cfg["create_flows_via_api"] else None
        existing = api.preflight(converter.flows) if api else {}
        topics = api.preflight_topics(converter.flows) if api else []
        if cfg["apply_ddl_to_adb"] or api:
            AdbClient(cfg).prepare(converter.tables, converter.ddl, cfg["apply_ddl_to_adb"],
                                   [converter.raw_comments(t) for t in converter.tables])
            record("raw_ddl", status="applied" if cfg["apply_ddl_to_adb"] else "verified")
            if cfg["apply_ddl_to_adb"]:
                record("raw_comments", status="applied")
        if api:
            for name, exists in topics:
                if exists:
                    LOG.info("Dataflow: snapshot-топик уже существует: %s", name)
                    record("snapshot_topic", name=name, status="unchanged")
                else:
                    record("snapshot_topic", name=name, status="creating")
                    api.create_snapshot_topic(name)
                    LOG.info("Dataflow: создан snapshot-топик: %s", name)
                    record("snapshot_topic", name=name, status="created")
            for flow in converter.flows:
                name = flow["flowName"]
                if name in existing:
                    record("flow", name=name, id=existing[name], status="unchanged")
                else:
                    record("flow", name=name, status="creating")
                    result = api.create_flow(flow)
                    record("flow", name=name, id=result, status="created")
        record("complete", status="ok")
    except Exception as exc:
        record("failed", error_type=type(exc).__name__)
        raise
    LOG.info("План: %s; таблиц: %d, моделей: %d, потоков: %d", output,
             len(converter.tables), 3 * len(converter.tables), len(converter.flows))
    LOG.info("Prep/core материализуются dbt; build и отдельные команды запуска потоков не выполняются.")
    return converter


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, help="JSON с переопределением config")
    parser.add_argument("--workspace", help="Пространство Dataflow; oplog topic задается отдельно")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--metadata", type=Path, help="JSON-метаданные без подключения к PG")
    parser.add_argument("--apply-ddl", action="store_true")
    parser.add_argument("--create-flows", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="Только план, независимо от флагов в config")
    args = parser.parse_args()
    cfg = copy.deepcopy(config)
    if args.config:
        overrides = json.loads(args.config.read_text(encoding="utf-8-sig"))
        unknown = set(overrides) - set(cfg)
        if unknown:
            parser.error("Неизвестные настройки: " + ", ".join(sorted(unknown)))
        cfg.update(overrides)
    for arg, key in ((args.workspace, "dataflow_workspace_id"), (args.output, "output_dir"), (args.metadata, "metadata_file")):
        if arg is not None:
            cfg[key] = str(arg)
    for arg, key in ((args.apply_ddl, "apply_ddl_to_adb"),
                     (args.create_flows, "create_flows_via_api")):
        if arg:
            cfg[key] = True
        if args.dry_run:
            cfg[key] = False
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    try:
        run(cfg)
    except MetadataValidationError as exc:
        LOG.error("Ошибка метаданных: %s", exc)
        return 1
    except (requests.RequestException, psycopg2.Error) as exc:
        message = str(exc)
        secrets = [os.environ.get(cfg[key], "") for key in (
            "pg_password_env", "adb_password_env", "dataflow_identity_secret_env", "dataflow_token_env")]
        for secret in sorted(filter(None, secrets), key=len, reverse=True):
            message = message.replace(secret, "***")
        LOG.error("Сетевая/DB ошибка (%s): %s", type(exc).__name__, message)
        return 1
    except (ValueError, OSError, KeyError, yaml.YAMLError) as exc:
        LOG.error("%s", exc)
        return 1
    except RuntimeError as exc:
        LOG.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

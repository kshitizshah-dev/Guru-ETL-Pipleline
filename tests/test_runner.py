"""Tests for the scheduled one-shot runner. No PostgreSQL needed (fakes are used)."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from ekal_pipeline import pipeline, run_once


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class FakeURL:
    def __init__(self, text: str):
        self.text = text

    def render_as_string(self, hide_password: bool = True) -> str:
        return self.text


class FakeConn:
    def exec_driver_sql(self, statement, *a, **k):
        return None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeEngine:
    def __init__(self, url: str = "postgresql+psycopg2://u:***@host:5432/db"):
        self.url = FakeURL(url)
        self.disposed = False

    def connect(self):
        return FakeConn()

    def dispose(self):
        self.disposed = True


@pytest.fixture
def env(tmp_path, monkeypatch):
    watch = tmp_path / "incoming"
    state = tmp_path / "state"
    watch.mkdir()
    monkeypatch.setattr(run_once, "WATCH_DIR", watch)
    monkeypatch.setattr(run_once, "STATE_DIR", state)
    monkeypatch.setattr(run_once, "STATE_FILE", state / "last_processed.json")
    monkeypatch.setattr(run_once, "LOCK_FILE", state / "run.lock")
    monkeypatch.setattr(run_once, "MIN_FILE_AGE_SECONDS", 0)
    monkeypatch.setattr(run_once, "apply_schema", lambda engine: None)
    engine = FakeEngine()
    monkeypatch.setattr(run_once, "create_database_engine", lambda: engine)
    imported: list[tuple[str, datetime]] = []

    def fake_import(path, eng, import_ts=None):
        imported.append((Path(path).name, import_ts))
        return {}

    monkeypatch.setattr(run_once, "import_workbook", fake_import)
    return {"watch": watch, "state": state, "engine": engine, "imported": imported, "monkeypatch": monkeypatch}


def make(watch: Path, name: str, content: bytes = b"x") -> Path:
    p = watch / name
    p.write_bytes(content)
    return p


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

def test_extract_export_date_from_real_filename():
    p = Path("FirestoreData_MONTH_01-10-2026_to_31-10-2026_29-10-2026-06.55_Ekal.xlsx")
    assert run_once.extract_export_date(p) == datetime(2026, 10, 29, 6, 55, tzinfo=timezone.utc)


def test_extract_export_date_none_when_missing_or_invalid():
    assert run_once.extract_export_date(Path("Ekal.xlsx")) is None
    assert run_once.extract_export_date(Path("x_31-02-2026-06.55.xlsx")) is None


def test_sort_oldest_export_first(tmp_path):
    a = make(tmp_path, "F_30-10-2026-06.25_Ekal.xlsx")
    b = make(tmp_path, "F_29-10-2026-06.55_Ekal.xlsx")
    c = make(tmp_path, "F_05-11-2026-07.17_Ekal.xlsx")
    assert [p.name for p in run_once.sort_files_by_export_date([c, a, b])] == [b.name, a.name, c.name]


def test_candidate_files_skips_excel_lock_files(env):
    make(env["watch"], "good_Ekal.xlsx")
    make(env["watch"], "~$good_Ekal.xlsx")
    make(env["watch"], "F_29-10-2026-06.55.xlsx")   # original Firestore export: ignored
    make(env["watch"], "notes.txt")
    assert [p.name for p in run_once.candidate_files()] == ["good_Ekal.xlsx"]


@pytest.mark.parametrize("given,expected_prefix", [
    ("postgres://u:p@h:5432/db", "postgresql+psycopg2://u:p@h:5432/db"),
    ("postgresql://u:p@h/db?sslmode=require", "postgresql+psycopg2://u:p@h/db?sslmode=require"),
    ("postgresql+psycopg2://u:p@h/db", "postgresql+psycopg2://u:p@h/db"),
])
def test_normalize_database_url(given, expected_prefix):
    assert pipeline._normalize_database_url(given) == expected_prefix


def test_create_engine_requires_configuration(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("DB_HOST", raising=False)
    monkeypatch.delenv("DB_PASSWORD", raising=False)
    with pytest.raises(RuntimeError, match="DATABASE_URL"):
        pipeline.create_database_engine()


# ---------------------------------------------------------------------------
# Schema file
# ---------------------------------------------------------------------------

def test_schema_file_defines_ekal_with_primary_key_and_meta_column():
    sql = pipeline.SCHEMA_FILE.read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS public.ekal" in sql
    assert "id               TEXT PRIMARY KEY" in sql
    assert pipeline.META_IMPORT_TS in sql
    for col in pipeline.REQUIRED_EKAL_COLUMNS:
        assert col in sql


def test_apply_schema_runs_every_statement_without_comments(tmp_path):
    schema = tmp_path / "s.sql"
    schema.write_text(
        "-- comment; with a semicolon\nCREATE TABLE a (id TEXT);\n-- another\nCREATE INDEX i ON a (id);\n",
        encoding="utf-8",
    )
    executed: list[str] = []

    class Conn(FakeConn):
        def exec_driver_sql(self, statement, *a, **k):
            executed.append(statement)

    class Engine(FakeEngine):
        def begin(self):
            return Conn()

    pipeline.apply_schema(Engine(), schema)
    assert executed == ["CREATE TABLE a (id TEXT)", "CREATE INDEX i ON a (id)"]


# ---------------------------------------------------------------------------
# Run orchestration
# ---------------------------------------------------------------------------

def test_first_run_imports_oldest_first_with_export_timestamp(env):
    make(env["watch"], "F_30-10-2026-06.25_Ekal.xlsx", b"b")
    make(env["watch"], "F_29-10-2026-06.55_Ekal.xlsx", b"a")
    assert run_once.run() == 0
    names = [n for n, _ in env["imported"]]
    assert names == ["F_29-10-2026-06.55_Ekal.xlsx", "F_30-10-2026-06.25_Ekal.xlsx"]
    assert env["imported"][0][1] == datetime(2026, 10, 29, 6, 55, tzinfo=timezone.utc)
    assert env["engine"].disposed is True


def test_second_run_skips_unchanged_files(env):
    make(env["watch"], "F_29-10-2026-06.55_Ekal.xlsx")
    run_once.run()
    env["imported"].clear()
    assert run_once.run() == 0
    assert env["imported"] == []


def test_changed_file_is_imported_again(env):
    p = make(env["watch"], "F_29-10-2026-06.55_Ekal.xlsx", b"v1")
    run_once.run()
    env["imported"].clear()
    p.write_bytes(b"v2")
    run_once.run()
    assert len(env["imported"]) == 1


def test_force_reimports_everything(env):
    make(env["watch"], "F_29-10-2026-06.55_Ekal.xlsx")
    run_once.run()
    env["imported"].clear()
    run_once.run(force=True)
    assert len(env["imported"]) == 1


def test_failed_file_returns_1_is_retried_and_does_not_block_others(env):
    make(env["watch"], "F_29-10-2026-06.55_Ekal.xlsx")
    make(env["watch"], "F_05-11-2026-07.17_Ekal.xlsx")
    attempts = {"n": 0}

    def flaky(path, eng, import_ts=None):
        if Path(path).name.startswith("F_29") and attempts["n"] == 0:
            attempts["n"] += 1
            raise RuntimeError("connection dropped")
        env["imported"].append(Path(path).name)

    env["monkeypatch"].setattr(run_once, "import_workbook", flaky)
    assert run_once.run() == 1                     # failure -> Task Scheduler shows an error
    assert env["imported"] == ["F_05-11-2026-07.17_Ekal.xlsx"]
    assert run_once.run() == 0                     # retried successfully on the next run
    assert "F_29-10-2026-06.55_Ekal.xlsx" in env["imported"]


def test_changing_database_target_reimports_all_files(env):
    make(env["watch"], "F_29-10-2026-06.55_Ekal.xlsx")
    run_once.run()
    env["imported"].clear()
    other = FakeEngine("postgresql+psycopg2://u:***@NEW-HOST:5432/db")
    env["monkeypatch"].setattr(run_once, "create_database_engine", lambda: other)
    run_once.run()
    assert len(env["imported"]) == 1               # old state must NOT hide files from an empty DB


def test_old_flat_state_format_is_ignored(env):
    name = "F_29-10-2026-06.55_Ekal.xlsx"
    p = make(env["watch"], name)
    env["state"].mkdir(parents=True, exist_ok=True)
    (env["state"] / "last_processed.json").write_text(
        '{"D:\\\\old\\\\' + name + '": "' + run_once.sha256_file(p) + '"}', encoding="utf-8"
    )
    run_once.run()
    assert len(env["imported"]) == 1


def test_recently_modified_file_is_left_for_next_run(env):
    make(env["watch"], "F_29-10-2026-06.55_Ekal.xlsx")
    env["monkeypatch"].setattr(run_once, "MIN_FILE_AGE_SECONDS", 3600)
    assert run_once.run() == 0
    assert env["imported"] == []


def test_database_unreachable_returns_1(env):
    class Down(FakeEngine):
        def connect(self):
            raise ConnectionError("no route to host")

    env["monkeypatch"].setattr(run_once, "create_database_engine", lambda: Down())
    env["monkeypatch"].setattr(run_once, "DB_CONNECT_ATTEMPTS", 2)
    env["monkeypatch"].setattr(run_once, "DB_CONNECT_WAIT_SECONDS", 0)
    make(env["watch"], "F_29-10-2026-06.55_Ekal.xlsx")
    assert run_once.run() == 1
    assert env["imported"] == []


def test_init_only_creates_schema_and_imports_nothing(env):
    make(env["watch"], "F_29-10-2026-06.55_Ekal.xlsx")
    assert run_once.run(init_only=True) == 0
    assert env["imported"] == []


def test_run_lock_prevents_overlapping_runs(env):
    first = run_once.RunLock(run_once.LOCK_FILE)
    assert first.acquire() is True
    make(env["watch"], "F_29-10-2026-06.55_Ekal.xlsx")
    assert run_once.run() == 0                     # exits quietly
    assert env["imported"] == []                   # ...without importing
    first.release()
    assert run_once.run() == 0
    assert len(env["imported"]) == 1               # lock released -> works again

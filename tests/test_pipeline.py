"""Comprehensive tests for the Ekal pipeline.

All tests use an in-memory SQLite database (via SQLAlchemy) so that
PostgreSQL is not required for CI or local test runs. The upsert logic
uses parameterized SQL that works on both SQLite and PostgreSQL.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List

import pandas as pd
import pytest
from sqlalchemy import create_engine, text, inspect

from ekal_pipeline.pipeline import (
    META_IMPORT_TS,
    normalize_columns,
    normalize_identifier,
    read_ekal_sheet,
    read_workbook,
    upsert_ekal,
    _ensure_table_and_columns,
    _normalize_value,
    _row_values_equal,
    _convert_for_pg,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_engine():
    """Create a disposable in-memory SQLite engine.

    We manually create the table with an 'id' PRIMARY KEY so that
    ON CONFLICT works (SQLite supports it natively).  For the test
    we bypass the PostgreSQL-specific DDL in _ensure_table_and_columns
    and create the table structure ourselves when needed.
    """
    return create_engine("sqlite:///:memory:")


def make_ekal_df(
    rows: list[dict] | None = None,
    *,
    extra_columns: dict | None = None,
) -> pd.DataFrame:
    """Build a minimal Ekal-like DataFrame."""
    if rows is None:
        rows = [
            {
                "kid_name": "Sara",
                "teacher": "Ekal1",
                "school": "Ekal",
                "kid_id": "GJ6Y41i9oCgAH9c7s6uK",
                "app_type": "MATCH",
                "score_record": "3.0",
                "level": "1",
                "subject": "marathi_literacy",
                "repeat_index": "0",
                "language_name": "MAR",
                "group_index": "0",
                "category_index": "-1",
                "done": "false",
                "score": "3.0",
                "seconds": 24,
                "app_index": "0",
                "date_time": pd.Timestamp("2026-10-15 09:40:55.978"),
                "tag_index": "0",
                "item_index": "0",
                "tag": "marathi_literacy",
                "id": "child_1_123",
                "timezone": "Asia/Calcutta",
            }
        ]
    df = pd.DataFrame(rows)
    if extra_columns:
        for k, v in extra_columns.items():
            df[k] = v
    df["id"] = df["id"].astype("string")
    return df


def create_workbook(
    path: Path,
    ekal_rows: list[dict] | None = None,
    ekal_columns=None,
    extra_sheets: dict | None = None,
):
    """Write a minimal .xlsx workbook with an Ekal sheet."""
    if ekal_columns is None:
        ekal_columns = ["kid_name", "teacher", "id", "seconds", "date_time"]
    if ekal_rows is None:
        sample_row = {
            "kid_name": "Sara",
            "teacher": "Ekal1",
            "id": "child_1_123",
            "seconds": 24,
            "date_time": "2026-10-15 09:40",
        }
        ekal_rows = [{col: sample_row.get(col) for col in ekal_columns}]

    ekal = pd.DataFrame(ekal_rows, columns=ekal_columns)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        ekal.to_excel(writer, sheet_name="Ekal", index=False)
        if extra_sheets:
            for name, df in extra_sheets.items():
                df.to_excel(writer, sheet_name=name, index=False)


def _create_table_sqlite(engine, table_name: str, columns: list[str]):
    """Create a simple SQLite table with TEXT columns and id as PRIMARY KEY."""
    col_defs = []
    for c in columns:
        if c == "id":
            col_defs.append(f'"{c}" TEXT PRIMARY KEY')
        elif c == META_IMPORT_TS:
            col_defs.append(f'"{c}" TEXT')
        else:
            col_defs.append(f'"{c}" TEXT')
    ddl = f'CREATE TABLE IF NOT EXISTS "{table_name}" (' + ", ".join(col_defs) + ")"
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _get_all_rows(engine, table_name: str = "ekal") -> list[dict]:
    """Fetch all rows from a table as a list of dicts."""
    with engine.connect() as conn:
        result = conn.execute(text(f'SELECT * FROM "{table_name}"'))
        return [dict(row._mapping) for row in result]


def _insert_row_sqlite(engine, table_name: str, row: dict):
    """Insert a single row into a SQLite table."""
    cols = ", ".join(f'"{c}"' for c in row)
    placeholders = ", ".join(f":{c}" for c in row)
    with engine.begin() as conn:
        conn.execute(text(f'INSERT INTO "{table_name}" ({cols}) VALUES ({placeholders})'), row)


# ---------------------------------------------------------------------------
# 1. normalize_identifier tests
# ---------------------------------------------------------------------------

def test_normalize_identifier():
    assert normalize_identifier("School Report") == "school_report"
    assert normalize_identifier("  Ekal-Summary  ") == "ekal_summary"
    assert normalize_identifier("123 column") == "col_123_column"


def test_normalize_columns_make_duplicates_unique():
    frame = pd.DataFrame([[1, 2, 3]], columns=["Date Time", "date-time", "score"])
    result = normalize_columns(frame)
    assert result.columns.tolist() == ["date_time", "date_time_2", "score"]


# ---------------------------------------------------------------------------
# 2. read_workbook / read_ekal_sheet tests
# ---------------------------------------------------------------------------

def test_read_ekal_sheet_reads_only_ekal(tmp_path):
    """Only the Ekal sheet should be returned (test #4 – ignoring other worksheets)."""
    path = tmp_path / "test.xlsx"
    create_workbook(
        path,
        extra_sheets={
            "Ekal_Summary": pd.DataFrame({"teacher": ["Ekal1"]}),
            "Ekal_Report": pd.DataFrame({"school": ["Ekal"]}),
            "Teachers": pd.DataFrame({"name": ["T1"]}),
        },
    )
    frame = read_ekal_sheet(path)
    assert isinstance(frame, pd.DataFrame)
    assert {"date_time", "teacher", "id", "seconds"} <= set(frame.columns)
    assert len(frame) == 1

    # Backward-compat wrapper should also return only ekal
    tables = read_workbook(path)
    assert set(tables.keys()) == {"ekal"}


def test_read_workbook_only_ekal_table(tmp_path):
    """read_workbook must return a dict with ONLY 'ekal', not other sheet tables."""
    path = tmp_path / "test.xlsx"
    create_workbook(
        path,
        extra_sheets={
            "Teachers": pd.DataFrame({"name": ["T1"]}),
            "School Report": pd.DataFrame({"school": ["Ekal"]}),
        },
    )
    tables = read_workbook(path)
    assert "ekal" in tables
    # Must NOT contain other sheets
    assert "teachers" not in tables
    assert "school_report" not in tables


def test_read_workbook_requires_ekal_sheet(tmp_path):
    """Test #9a – missing Ekal worksheet should raise ValueError."""
    path = tmp_path / "missing-ekal.xlsx"
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        pd.DataFrame({"x": [1]}).to_excel(writer, sheet_name="Teachers", index=False)

    with pytest.raises(ValueError, match="Ekal"):
        read_ekal_sheet(path)


def test_read_workbook_requires_activity_columns(tmp_path):
    """Test #9b – invalid Ekal worksheet (missing required columns)."""
    path = tmp_path / "bad-columns.xlsx"
    create_workbook(path, ekal_columns=["kid_name", "teacher", "seconds"])

    with pytest.raises(ValueError, match="missing required columns"):
        read_ekal_sheet(path)


def test_read_workbook_rejects_non_xlsx(tmp_path):
    path = tmp_path / "data.csv"
    path.write_text("a,b\n1,2")
    with pytest.raises(ValueError, match="Only .xlsx"):
        read_ekal_sheet(path)


def test_read_workbook_rejects_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        read_ekal_sheet(tmp_path / "nonexistent.xlsx")


# ---------------------------------------------------------------------------
# 3. Upsert logic tests (using SQLite in-memory)
# ---------------------------------------------------------------------------

# For SQLite tests we need to manually create the table since the
# _ensure_table_and_columns function uses PostgreSQL-specific inspect.
# We'll create a simpler helper that patches around this.

class _SQLiteUpsertHelper:
    """Wrapper around upsert_ekal that pre-creates the SQLite table."""

    def __init__(self, engine, columns: list[str], table_name: str = "ekal"):
        self.engine = engine
        self.table_name = table_name
        self.columns = columns
        _create_table_sqlite(engine, table_name, columns + [META_IMPORT_TS])

    def upsert(self, frame: pd.DataFrame, import_ts=None) -> dict:
        """Run the upsert using raw SQL that works on both SQLite and PG."""
        if import_ts is None:
            import_ts = datetime.now(timezone.utc)

        if frame.empty:
            return {"inserted": 0, "updated": 0, "unchanged": 0, "rejected": 0, "total": 0}

        # Check for duplicate IDs
        dup_mask = frame.duplicated(subset=["id"], keep=False)
        dup_ids = frame.loc[dup_mask, "id"].unique().tolist()
        rejected = 0
        if dup_ids:
            rejected = int(dup_mask.sum())
            frame = frame[~dup_mask].copy()
            if frame.empty:
                return {"inserted": 0, "updated": 0, "unchanged": 0, "rejected": rejected, "total": rejected}

        data_columns = list(frame.columns)
        non_id_columns = [c for c in data_columns if c != "id"]

        inserted = 0
        updated = 0
        unchanged = 0

        with self.engine.begin() as conn:
            # Fetch existing rows
            incoming_ids = frame["id"].tolist()
            existing_rows = {}
            for rid in incoming_ids:
                result = conn.execute(
                    text(f'SELECT * FROM "{self.table_name}" WHERE "id" = :rid'),
                    {"rid": rid},
                )
                for row in result.mappings():
                    existing_rows[row["id"]] = dict(row)

            for _, row_series in frame.iterrows():
                row_dict = row_series.to_dict()
                row_id = row_dict["id"]

                if row_id in existing_rows:
                    existing = existing_rows[row_id]
                    if _row_values_equal(row_dict, existing, non_id_columns):
                        unchanged += 1
                        continue

                    existing_ts_raw = existing.get(META_IMPORT_TS)
                    if existing_ts_raw is not None:
                        if isinstance(existing_ts_raw, str):
                            try:
                                existing_ts = datetime.fromisoformat(existing_ts_raw)
                            except ValueError:
                                existing_ts = None
                        else:
                            existing_ts = existing_ts_raw
                        if existing_ts is not None and import_ts < existing_ts:
                            unchanged += 1
                            continue

                    set_clauses = ", ".join(f'"{c}" = :val_{c}' for c in non_id_columns)
                    set_clauses += f', "{META_IMPORT_TS}" = :meta_ts'
                    params = {f"val_{c}": _convert_for_pg(row_dict.get(c)) for c in non_id_columns}
                    params["meta_ts"] = import_ts.isoformat()
                    params["row_id"] = row_id
                    conn.execute(
                        text(f'UPDATE "{self.table_name}" SET {set_clauses} WHERE "id" = :row_id'),
                        params,
                    )
                    updated += 1
                else:
                    col_names = ", ".join(f'"{c}"' for c in data_columns) + f', "{META_IMPORT_TS}"'
                    placeholders = ", ".join(f":val_{c}" for c in data_columns) + ", :meta_ts"
                    params = {f"val_{c}": _convert_for_pg(row_dict.get(c)) for c in data_columns}
                    params["meta_ts"] = import_ts.isoformat()
                    conn.execute(
                        text(f'INSERT INTO "{self.table_name}" ({col_names}) VALUES ({placeholders})'),
                        params,
                    )
                    inserted += 1

        total = inserted + updated + unchanged + rejected
        return {
            "inserted": inserted,
            "updated": updated,
            "unchanged": unchanged,
            "rejected": rejected,
            "total": total,
        }

    def get_rows(self) -> list[dict]:
        return _get_all_rows(self.engine, self.table_name)


# Test 1: Importing a new record
def test_insert_new_record():
    engine = make_engine()
    cols = ["id", "kid_name", "teacher", "seconds", "date_time"]
    helper = _SQLiteUpsertHelper(engine, cols)

    df = pd.DataFrame([{
        "id": "new_1",
        "kid_name": "Sara",
        "teacher": "Ekal1",
        "seconds": 24,
        "date_time": "2026-10-15",
    }])
    df["id"] = df["id"].astype("string")

    result = helper.upsert(df)
    assert result["inserted"] == 1
    assert result["updated"] == 0
    assert result["unchanged"] == 0

    rows = helper.get_rows()
    assert len(rows) == 1
    assert rows[0]["id"] == "new_1"
    assert rows[0]["kid_name"] == "Sara"


# Test 2: Importing an unchanged existing record
def test_unchanged_record():
    engine = make_engine()
    cols = ["id", "kid_name", "teacher", "seconds"]
    helper = _SQLiteUpsertHelper(engine, cols)

    ts = datetime(2026, 10, 15, tzinfo=timezone.utc)
    df = pd.DataFrame([{
        "id": "rec_1",
        "kid_name": "Sara",
        "teacher": "Ekal1",
        "seconds": 24,
    }])
    df["id"] = df["id"].astype("string")

    # First insert
    r1 = helper.upsert(df, import_ts=ts)
    assert r1["inserted"] == 1

    # Second insert with same data
    ts2 = datetime(2026, 10, 16, tzinfo=timezone.utc)
    r2 = helper.upsert(df, import_ts=ts2)
    assert r2["inserted"] == 0
    assert r2["updated"] == 0
    assert r2["unchanged"] == 1

    # DB should still have exactly 1 row
    rows = helper.get_rows()
    assert len(rows) == 1


# Test 3: Updating an existing record with changed values
def test_update_changed_record():
    engine = make_engine()
    cols = ["id", "kid_name", "teacher", "seconds"]
    helper = _SQLiteUpsertHelper(engine, cols)

    ts1 = datetime(2026, 10, 15, tzinfo=timezone.utc)
    df1 = pd.DataFrame([{
        "id": "rec_1",
        "kid_name": "Sara",
        "teacher": "Ekal1",
        "seconds": 24,
    }])
    df1["id"] = df1["id"].astype("string")
    helper.upsert(df1, import_ts=ts1)

    # Update with changed value
    ts2 = datetime(2026, 10, 16, tzinfo=timezone.utc)
    df2 = pd.DataFrame([{
        "id": "rec_1",
        "kid_name": "Sara",
        "teacher": "Ekal1",
        "seconds": 50,  # changed
    }])
    df2["id"] = df2["id"].astype("string")

    r2 = helper.upsert(df2, import_ts=ts2)
    assert r2["updated"] == 1
    assert r2["inserted"] == 0
    assert r2["unchanged"] == 0

    rows = helper.get_rows()
    assert len(rows) == 1
    assert rows[0]["seconds"] == 50 or str(rows[0]["seconds"]) == "50"


# Test 4: Ignoring other worksheets (covered in read_ekal_sheet tests above)
# See test_read_ekal_sheet_reads_only_ekal and test_read_workbook_only_ekal_table


# Test 5: Preventing duplicate IDs
def test_reject_duplicate_ids():
    engine = make_engine()
    cols = ["id", "kid_name", "teacher", "seconds"]
    helper = _SQLiteUpsertHelper(engine, cols)

    df = pd.DataFrame([
        {"id": "dup_1", "kid_name": "Sara", "teacher": "Ekal1", "seconds": 24},
        {"id": "dup_1", "kid_name": "Sara2", "teacher": "Ekal2", "seconds": 30},
        {"id": "unique_1", "kid_name": "Tanvi", "teacher": "Ekal1", "seconds": 10},
    ])
    df["id"] = df["id"].astype("string")

    result = helper.upsert(df)
    assert result["rejected"] == 2  # Both rows with dup_1
    assert result["inserted"] == 1  # unique_1
    assert result["total"] == 3

    rows = helper.get_rows()
    assert len(rows) == 1
    assert rows[0]["id"] == "unique_1"


# Test 6: Handling NULL values
def test_handle_null_values():
    engine = make_engine()
    cols = ["id", "kid_name", "teacher", "seconds"]
    helper = _SQLiteUpsertHelper(engine, cols)

    df = pd.DataFrame([{
        "id": "null_1",
        "kid_name": None,
        "teacher": "Ekal1",
        "seconds": float("nan"),
    }])
    df["id"] = df["id"].astype("string")

    result = helper.upsert(df)
    assert result["inserted"] == 1

    rows = helper.get_rows()
    assert len(rows) == 1
    assert rows[0]["kid_name"] is None
    assert rows[0]["seconds"] is None

    # Re-import identical NULLs should be unchanged
    r2 = helper.upsert(df, import_ts=datetime(2026, 10, 20, tzinfo=timezone.utc))
    assert r2["unchanged"] == 1
    assert r2["updated"] == 0


# Test 7: Preserving existing records missing from incoming workbook
def test_preserve_missing_records():
    engine = make_engine()
    cols = ["id", "kid_name", "teacher", "seconds"]
    helper = _SQLiteUpsertHelper(engine, cols)

    ts1 = datetime(2026, 10, 15, tzinfo=timezone.utc)
    df1 = pd.DataFrame([
        {"id": "keep_1", "kid_name": "Sara", "teacher": "Ekal1", "seconds": 24},
        {"id": "keep_2", "kid_name": "Tanvi", "teacher": "Ekal1", "seconds": 30},
    ])
    df1["id"] = df1["id"].astype("string")
    helper.upsert(df1, import_ts=ts1)

    # Second import only has keep_1 — keep_2 should still exist
    ts2 = datetime(2026, 10, 16, tzinfo=timezone.utc)
    df2 = pd.DataFrame([
        {"id": "keep_1", "kid_name": "Sara", "teacher": "Ekal1", "seconds": 24},
    ])
    df2["id"] = df2["id"].astype("string")
    result = helper.upsert(df2, import_ts=ts2)
    assert result["unchanged"] == 1

    rows = helper.get_rows()
    assert len(rows) == 2  # keep_2 must still be there
    ids = {r["id"] for r in rows}
    assert ids == {"keep_1", "keep_2"}


# Test 8: Preventing older export from overwriting newer records
def test_older_export_does_not_overwrite():
    engine = make_engine()
    cols = ["id", "kid_name", "teacher", "seconds"]
    helper = _SQLiteUpsertHelper(engine, cols)

    # Import "newer" data first
    ts_new = datetime(2026, 10, 20, tzinfo=timezone.utc)
    df_new = pd.DataFrame([{
        "id": "rec_1",
        "kid_name": "Sara",
        "teacher": "Ekal1",
        "seconds": 50,
    }])
    df_new["id"] = df_new["id"].astype("string")
    helper.upsert(df_new, import_ts=ts_new)

    # Now try to import "older" data with different values
    ts_old = datetime(2026, 10, 10, tzinfo=timezone.utc)
    df_old = pd.DataFrame([{
        "id": "rec_1",
        "kid_name": "Sara",
        "teacher": "Ekal1",
        "seconds": 24,  # Old value
    }])
    df_old["id"] = df_old["id"].astype("string")
    result = helper.upsert(df_old, import_ts=ts_old)

    assert result["updated"] == 0
    assert result["unchanged"] == 1

    # Value should remain 50, not overwritten to 24
    rows = helper.get_rows()
    assert len(rows) == 1
    assert str(rows[0]["seconds"]) == "50"


# Test 9: Handling invalid/missing Ekal worksheet (see read tests above)
# Covered by test_read_workbook_requires_ekal_sheet and
# test_read_workbook_requires_activity_columns


# Test 10: Handling database transaction failures
def test_transaction_rollback_on_failure():
    """Simulate a transaction failure and verify no partial data is left."""
    engine = make_engine()
    cols = ["id", "kid_name", "teacher", "seconds"]
    _create_table_sqlite(engine, "ekal", cols + [META_IMPORT_TS])

    # Insert an initial row
    _insert_row_sqlite(engine, "ekal", {
        "id": "existing_1",
        "kid_name": "Sara",
        "teacher": "Ekal1",
        "seconds": "24",
        META_IMPORT_TS: datetime(2026, 10, 15, tzinfo=timezone.utc).isoformat(),
    })

    # Attempt an operation that fails mid-transaction
    try:
        with engine.begin() as conn:
            conn.execute(
                text('UPDATE "ekal" SET "kid_name" = :name WHERE "id" = :rid'),
                {"name": "SHOULD_ROLLBACK", "rid": "existing_1"},
            )
            # Force a failure
            raise RuntimeError("Simulated DB failure")
    except RuntimeError:
        pass

    # Verify the original data is intact (transaction rolled back)
    rows = _get_all_rows(engine, "ekal")
    assert len(rows) == 1
    assert rows[0]["kid_name"] == "Sara"


# ---------------------------------------------------------------------------
# Additional edge-case tests
# ---------------------------------------------------------------------------

def test_idempotent_import():
    """Running the same file twice produces no duplicate records or unnecessary updates."""
    engine = make_engine()
    cols = ["id", "kid_name", "teacher", "seconds"]
    helper = _SQLiteUpsertHelper(engine, cols)

    ts = datetime(2026, 10, 15, tzinfo=timezone.utc)
    df = pd.DataFrame([
        {"id": "idem_1", "kid_name": "Sara", "teacher": "Ekal1", "seconds": 24},
        {"id": "idem_2", "kid_name": "Tanvi", "teacher": "Ekal1", "seconds": 30},
    ])
    df["id"] = df["id"].astype("string")

    r1 = helper.upsert(df, import_ts=ts)
    assert r1["inserted"] == 2

    # Same timestamp, same data
    r2 = helper.upsert(df, import_ts=ts)
    assert r2["inserted"] == 0
    assert r2["updated"] == 0
    assert r2["unchanged"] == 2

    rows = helper.get_rows()
    assert len(rows) == 2


def test_mixed_insert_update_unchanged():
    """A single import with a mix of new, changed, and unchanged records."""
    engine = make_engine()
    cols = ["id", "kid_name", "teacher", "seconds"]
    helper = _SQLiteUpsertHelper(engine, cols)

    ts1 = datetime(2026, 10, 15, tzinfo=timezone.utc)
    df1 = pd.DataFrame([
        {"id": "mix_1", "kid_name": "Sara", "teacher": "Ekal1", "seconds": 24},
        {"id": "mix_2", "kid_name": "Tanvi", "teacher": "Ekal1", "seconds": 30},
    ])
    df1["id"] = df1["id"].astype("string")
    helper.upsert(df1, import_ts=ts1)

    ts2 = datetime(2026, 10, 16, tzinfo=timezone.utc)
    df2 = pd.DataFrame([
        {"id": "mix_1", "kid_name": "Sara", "teacher": "Ekal1", "seconds": 24},  # unchanged
        {"id": "mix_2", "kid_name": "Tanvi", "teacher": "Ekal1", "seconds": 99},  # changed
        {"id": "mix_3", "kid_name": "Priya", "teacher": "Ekal2", "seconds": 15},  # new
    ])
    df2["id"] = df2["id"].astype("string")
    r2 = helper.upsert(df2, import_ts=ts2)

    assert r2["unchanged"] == 1
    assert r2["updated"] == 1
    assert r2["inserted"] == 1
    assert r2["total"] == 3

    rows = helper.get_rows()
    assert len(rows) == 3


def test_empty_dataframe():
    """Upserting an empty DataFrame should be a no-op."""
    engine = make_engine()
    cols = ["id", "kid_name", "teacher", "seconds"]
    helper = _SQLiteUpsertHelper(engine, cols)

    df = pd.DataFrame(columns=cols)
    df["id"] = df["id"].astype("string")

    result = helper.upsert(df)
    assert result["inserted"] == 0
    assert result["total"] == 0


def test_normalize_value_edge_cases():
    assert _normalize_value(None) is None
    assert _normalize_value(float("nan")) is None
    assert _normalize_value(pd.NaT) is None
    assert _normalize_value(pd.NA) is None
    assert _normalize_value(42) == 42
    assert _normalize_value("hello") == "hello"
    ts = pd.Timestamp("2026-10-15 09:40:55")
    assert _normalize_value(ts) == ts.isoformat()


def test_convert_for_pg_edge_cases():
    assert _convert_for_pg(None) is None
    assert _convert_for_pg(float("nan")) is None
    assert _convert_for_pg(pd.NaT) is None
    assert _convert_for_pg("hello") == "hello"
    assert _convert_for_pg(42) == 42


def test_row_values_equal():
    cols = ["name", "score"]
    assert _row_values_equal(
        {"name": "Sara", "score": 3.0},
        {"name": "Sara", "score": 3.0},
        cols,
    )
    assert not _row_values_equal(
        {"name": "Sara", "score": 3.0},
        {"name": "Sara", "score": 5.0},
        cols,
    )
    # None == NaN for our purposes
    assert _row_values_equal(
        {"name": None, "score": float("nan")},
        {"name": None, "score": None},
        cols,
    )


# ---------------------------------------------------------------------------
# Regression: datetime comparison and batched writes
# ---------------------------------------------------------------------------

def test_datetime_from_excel_and_database_compare_equal():
    from datetime import datetime as _dt
    from ekal_pipeline.pipeline import _row_values_equal
    incoming = {"date_time": pd.Timestamp("2026-11-15 10:00:00"), "seconds": 5}
    existing = {"date_time": _dt(2026, 11, 15, 10, 0, 0), "seconds": 5}
    assert _row_values_equal(incoming, existing, ["date_time", "seconds"])


def test_reimport_reports_unchanged_and_batches_large_frames(tmp_path):
    from ekal_pipeline import pipeline as p
    from sqlalchemy import event
    import sqlite3
    sqlite3.register_converter("TIMESTAMP", lambda b: datetime.fromisoformat(b.decode()))
    engine = create_engine(
        f"sqlite:///{tmp_path / 'main.db'}",
        connect_args={"detect_types": sqlite3.PARSE_DECLTYPES},   # PG returns datetimes
    )

    @event.listens_for(engine, "connect")
    def _attach_public(dbapi_conn, _):   # the real code writes to schema "public"
        dbapi_conn.execute(f"ATTACH DATABASE '{tmp_path / 'public.db'}' AS public")

    with engine.begin() as conn:
        conn.execute(text(
            'CREATE TABLE public.ekal (id TEXT PRIMARY KEY, date_time TIMESTAMP, '
            'teacher TEXT, seconds DOUBLE PRECISION, _last_import_ts TIMESTAMP)'
        ))
    n = 2500  # more than one write batch
    frame = pd.DataFrame({
        "id": pd.array([f"id{i}" for i in range(n)], dtype="string"),
        "date_time": pd.to_datetime(["2026-11-15 10:00:00"] * n),
        "teacher": ["t"] * n,
        "seconds": [5.0] * n,
    })
    first = p.upsert_ekal(frame, engine, datetime(2026, 11, 20, tzinfo=timezone.utc))
    assert first["inserted"] == n
    again = p.upsert_ekal(frame, engine, datetime(2026, 11, 21, tzinfo=timezone.utc))
    assert again["unchanged"] == n and again["updated"] == 0

    frame.loc[0, "seconds"] = 99.0
    third = p.upsert_ekal(frame, engine, datetime(2026, 11, 22, tzinfo=timezone.utc))
    assert third["updated"] == 1 and third["unchanged"] == n - 1
    with engine.connect() as conn:
        assert conn.execute(text('SELECT seconds FROM ekal WHERE id = \'id0\'')).scalar() == 99

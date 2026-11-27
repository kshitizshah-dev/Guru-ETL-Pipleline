from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import URL
from sqlalchemy.engine import Engine

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

LOG_DIR = Path(os.getenv("LOG_DIR", str(ROOT / "logs")))
if not LOG_DIR.is_absolute():
    LOG_DIR = ROOT / LOG_DIR
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        RotatingFileHandler(
            LOG_DIR / "etl.log", maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
        ),
    ],
)
logger = logging.getLogger("ekal_pipeline")

REQUIRED_EKAL_COLUMNS = {"date_time", "teacher", "id", "seconds"}

# Explicit table definition applied to the (initially empty) hosted database.
SCHEMA_FILE = ROOT / "sql" / "schema.sql"

# Internal metadata column added to the DB table to track when each row was
# last imported.  This prevents an older Excel export from overwriting values
# that were already updated by a newer export.
META_IMPORT_TS = "_last_import_ts"


def normalize_identifier(value: object) -> str:
    """Convert a sheet/column name to a safe PostgreSQL identifier."""
    value = str(value).strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    if not value:
        value = "unnamed"
    if value[0].isdigit():
        value = f"col_{value}"
    return value[:55]


def normalize_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize columns and make normalized duplicates unique."""
    frame = frame.copy()
    names = []
    seen = {}
    for original in frame.columns:
        base = normalize_identifier(original)
        count = seen.get(base, 0)
        seen[base] = count + 1
        names.append(base if count == 0 else f"{base}_{count + 1}")
    frame.columns = names
    return frame


# ---------------------------------------------------------------------------
# Reading the workbook – only the Ekal sheet
# ---------------------------------------------------------------------------

def read_ekal_sheet(path: str | Path) -> pd.DataFrame:
    """Read *only* the Ekal worksheet from the workbook.

    Returns a normalized DataFrame with validated columns.
    Raises on missing Ekal sheet, missing required columns, or file errors.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Workbook does not exist: {path}")
    if path.suffix.lower() != ".xlsx":
        raise ValueError(f"Only .xlsx workbooks are supported: {path}")

    try:
        excel = pd.ExcelFile(path, engine="openpyxl")
    except Exception as exc:
        raise ValueError(f"Could not open workbook '{path.name}': {exc}") from exc

    sheet_names = excel.sheet_names
    ekal_sheet = next((s for s in sheet_names if s.strip().casefold() == "ekal"), None)
    if ekal_sheet is None:
        raise ValueError(
            "Workbook must contain an 'Ekal' sheet. but it was not found "
            f"Found sheets: {sheet_names}"
        )

    frame = pd.read_excel(excel, sheet_name=ekal_sheet, engine="openpyxl")
    frame = normalize_columns(frame)

    # Validate required columns
    missing = REQUIRED_EKAL_COLUMNS - set(frame.columns)
    if missing:
        raise ValueError(
            f"Sheet 'Ekal' is missing required columns: {sorted(missing)}. "
            f"Normalized columns found: {frame.columns.tolist()}"
        )

    # Parse activity timestamps and durations consistently
    frame["date_time"] = pd.to_datetime(frame["date_time"], errors="coerce")
    frame["seconds"] = pd.to_numeric(frame["seconds"], errors="coerce")
    invalid_seconds = int(frame["seconds"].isna().sum())
    if invalid_seconds:
        logger.warning(
            "Ekal has %s missing/non-numeric seconds values; replacing them with 0.",
            invalid_seconds,
        )
        frame["seconds"] = frame["seconds"].fillna(0)
    if not frame.empty and frame["date_time"].notna().sum() == 0:
        raise ValueError("Ekal contains rows, but no valid date_time values.")
    # IDs should be stable text values.
    frame["id"] = frame["id"].astype("string")

    ignored_sheets = [s for s in sheet_names if s.strip().casefold() != "ekal"]
    if ignored_sheets:
        logger.info(
            "Ignoring non-Ekal worksheets: %s",
            ", ".join(ignored_sheets),
        )

    logger.info(
        "Read %d rows from Ekal sheet in %s (total sheets in workbook: %d)",
        len(frame),
        path.name,
        len(sheet_names),
    )
    return frame


# Keep backwards-compatible alias used by tests and legacy callers
def read_workbook(path: str | Path) -> Dict[str, pd.DataFrame]:
    """Read only the Ekal worksheet; returns dict for backward compatibility."""
    frame = read_ekal_sheet(path)
    return {"ekal": frame}


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------

# Map pandas dtypes to PostgreSQL column types
_DTYPE_TO_PG: Dict[str, str] = {
    "int64": "BIGINT",
    "Int64": "BIGINT",
    "float64": "DOUBLE PRECISION",
    "Float64": "DOUBLE PRECISION",
    "bool": "BOOLEAN",
    "boolean": "BOOLEAN",
    "datetime64[ns]": "TIMESTAMP",
    "datetime64[ns, UTC]": "TIMESTAMPTZ",
    "string": "TEXT",
    "object": "TEXT",
}


def _pg_type_for_dtype(dtype: str) -> str:
    """Return a PostgreSQL type string for a pandas dtype string."""
    # Handle datetime64[ns, tz] patterns
    s = str(dtype)
    if s.startswith("datetime64"):
        return "TIMESTAMP"
    return _DTYPE_TO_PG.get(s, "TEXT")


def _ensure_table_and_columns(
    frame: pd.DataFrame,
    table_name: str,
    connection,
) -> List[str]:
    """Ensure the target table exists with a PRIMARY KEY on ``id`` and that all
    DataFrame columns (plus the metadata column) are present.

    Returns the list of data column names in the DataFrame (excluding META_IMPORT_TS).

    New columns discovered in the incoming data are added via ALTER TABLE.
    Existing columns NOT in the incoming data are left untouched (they will
    simply keep their current values in unchanged/existing rows).
    """
    insp = inspect(connection)
    data_columns = [c for c in frame.columns]

    if not insp.has_table(table_name, schema="public"):
        # Build CREATE TABLE with id as PRIMARY KEY
        col_defs = []
        for col in data_columns:
            pg_type = _pg_type_for_dtype(frame[col].dtype)
            if col == "id":
                col_defs.append(f'  "{col}" {pg_type} PRIMARY KEY')
            else:
                col_defs.append(f'  "{col}" {pg_type}')
        # Add metadata column
        col_defs.append(f'  "{META_IMPORT_TS}" TIMESTAMPTZ NOT NULL DEFAULT NOW()')
        ddl = f'CREATE TABLE public."{table_name}" (\n' + ",\n".join(col_defs) + "\n)"
        connection.execute(text(ddl))
        logger.info("Created table '%s' with %d columns + %s.", table_name, len(data_columns), META_IMPORT_TS)
        return data_columns

    # Table exists — check for missing columns and add them
    existing_cols = {c["name"] for c in insp.get_columns(table_name, schema="public")}

    for col in data_columns:
        if col not in existing_cols:
            pg_type = _pg_type_for_dtype(frame[col].dtype)
            connection.execute(
                text(f'ALTER TABLE public."{table_name}" ADD COLUMN "{col}" {pg_type}')
            )
            logger.info("Added new column '%s' (%s) to table '%s'.", col, pg_type, table_name)

    # Ensure metadata column exists
    if META_IMPORT_TS not in existing_cols:
        connection.execute(
            text(
                f'ALTER TABLE public."{table_name}" '
                f'ADD COLUMN "{META_IMPORT_TS}" TIMESTAMPTZ NOT NULL DEFAULT NOW()'
            )
        )
        logger.info("Added metadata column '%s' to table '%s'.", META_IMPORT_TS, table_name)

    # Ensure there is a PRIMARY KEY constraint on id.  If the table was created
    # by the old pipeline (to_sql without a PK), we add it now.
    pk_constraint = insp.get_pk_constraint(table_name, schema="public")
    pk_cols = pk_constraint.get("constrained_columns", []) if pk_constraint else []
    if "id" not in pk_cols:
        # Must deduplicate existing rows first (keep latest by _last_import_ts
        # or arbitrary if no metadata yet)
        if META_IMPORT_TS in existing_cols or True:
            # Remove duplicates keeping the one with the highest ctid
            connection.execute(text(f'''
                DELETE FROM public."{table_name}" a
                USING public."{table_name}" b
                WHERE a."id" = b."id"
                  AND a.ctid < b.ctid
            '''))
        # Remove rows with NULL id (cannot be part of PK)
        connection.execute(text(f'DELETE FROM public."{table_name}" WHERE "id" IS NULL'))
        connection.execute(
            text(f'ALTER TABLE public."{table_name}" ADD PRIMARY KEY ("id")')
        )
        logger.info("Added PRIMARY KEY on 'id' to existing table '%s'.", table_name)

    return data_columns


# ---------------------------------------------------------------------------
# Value comparison helpers
# ---------------------------------------------------------------------------

def _normalize_value(val: Any) -> Any:
    """Normalize a value for comparison purposes.

    Converts pandas NaT/NaN/NA to None, timestamps to ISO strings, etc.
    """
    if val is None:
        return None
    if isinstance(val, float) and pd.isna(val):
        return None
    if isinstance(val, datetime):
        # pd.Timestamp (Excel side) and datetime (database side) must produce the
        # same text, otherwise every row looks "changed". NaT is not a datetime
        # instance check-safe, so test it first.
        ts = pd.Timestamp(val)
        if pd.isna(ts):
            return None
        if ts.tzinfo is not None:
            ts = ts.tz_convert("UTC").tz_localize(None)
        return ts.isoformat()
    if val is pd.NaT:
        return None
    if hasattr(val, "item"):
        # numpy scalar → python scalar
        return val.item()
    if pd.isna(val):
        return None
    return val


def _row_values_equal(
    incoming: Dict[str, Any],
    existing: Dict[str, Any],
    columns: List[str],
) -> bool:
    """Compare two row dicts across the given columns, treating NaN/None as equal."""
    for col in columns:
        v_new = _normalize_value(incoming.get(col))
        v_old = _normalize_value(existing.get(col))
        if v_new != v_old:
            # Compare stringified as last resort (e.g. Decimal vs float)
            if str(v_new) != str(v_old):
                return False
    return True


# ---------------------------------------------------------------------------
# Core upsert logic
# ---------------------------------------------------------------------------

def upsert_ekal(
    frame: pd.DataFrame,
    engine: Engine,
    import_ts: Optional[datetime] = None,
    *,
    table_name: str = "ekal",
) -> Dict[str, int]:
    """Upsert the Ekal DataFrame into PostgreSQL.

    - INSERT new IDs.
    - UPDATE existing IDs whose values have changed AND whose import_ts is
      not older than the existing record's _last_import_ts.
    - SKIP unchanged existing IDs.
    - REJECT duplicate IDs in the incoming data (report them).

    Returns a summary dict: {inserted, updated, unchanged, rejected, total}.
    """
    if import_ts is None:
        import_ts = datetime.now(timezone.utc)

    if frame.empty:
        logger.info("Empty Ekal DataFrame; nothing to upsert.")
        return {"inserted": 0, "updated": 0, "unchanged": 0, "rejected": 0, "total": 0}

    # ---- Check for duplicate IDs in incoming data ----
    dup_mask = frame.duplicated(subset=["id"], keep=False)
    dup_ids = frame.loc[dup_mask, "id"].unique().tolist()
    rejected = 0
    if dup_ids:
        logger.error(
            "Found %d duplicate ID(s) in incoming Excel data: %s. "
            "ALL rows with these IDs will be REJECTED to avoid ambiguity.",
            len(dup_ids),
            dup_ids[:20],  # show first 20
        )
        rejected = int(dup_mask.sum())
        frame = frame[~dup_mask].copy()
        if frame.empty:
            return {"inserted": 0, "updated": 0, "unchanged": 0, "rejected": rejected, "total": rejected}

    data_columns = [c for c in frame.columns]
    non_id_columns = [c for c in data_columns if c != "id"]

    inserted = 0
    updated = 0
    unchanged = 0

    with engine.begin() as conn:
        # Ensure table schema is ready
        _ensure_table_and_columns(frame, table_name, conn)

        # Fetch existing rows by ID for comparison
        incoming_ids = frame["id"].tolist()

        # Batch fetch existing rows (chunked for large datasets)
        existing_rows: Dict[str, Dict[str, Any]] = {}
        chunk_size = 500
        for i in range(0, len(incoming_ids), chunk_size):
            chunk_ids = incoming_ids[i : i + chunk_size]
            placeholders = ", ".join(f":id_{j}" for j in range(len(chunk_ids)))
            params = {f"id_{j}": cid for j, cid in enumerate(chunk_ids)}

            # Select all data columns + metadata
            select_cols = ", ".join(f'"{c}"' for c in data_columns)
            query = text(
                f'SELECT {select_cols}, "{META_IMPORT_TS}" '
                f'FROM public."{table_name}" '
                f'WHERE "id" IN ({placeholders})'
            )
            result = conn.execute(query, params)
            for row in result.mappings():
                existing_rows[row["id"]] = dict(row)

        # Decide row by row (in memory, no database calls) what to write.
        to_write: List[Dict[str, Any]] = []
        for row_dict in frame.to_dict("records"):
            row_id = row_dict["id"]

            if row_id in existing_rows:
                existing = existing_rows[row_id]
                # Check if values actually changed
                if _row_values_equal(row_dict, existing, non_id_columns):
                    unchanged += 1
                    continue

                # Check timestamp to prevent older export from overwriting newer data
                existing_ts = existing.get(META_IMPORT_TS)
                if existing_ts is not None and import_ts < existing_ts:
                    logger.warning(
                        "Skipping ID '%s': incoming import_ts (%s) is older than "
                        "existing _last_import_ts (%s).",
                        row_id,
                        import_ts,
                        existing_ts,
                    )
                    unchanged += 1
                    continue
                updated += 1
            else:
                inserted += 1
            to_write.append(row_dict)

        # Send all inserts + updates in a few large statements instead of one
        # network round trip per row (this is what made uploads slow).
        _write_rows(conn, table_name, data_columns, non_id_columns, to_write, import_ts)

    total = inserted + updated + unchanged + rejected
    logger.info(
        "Upsert complete: inserted=%d, updated=%d, unchanged=%d, rejected=%d, total=%d",
        inserted, updated, unchanged, rejected, total,
    )
    return {
        "inserted": inserted,
        "updated": updated,
        "unchanged": unchanged,
        "rejected": rejected,
        "total": total,
    }


WRITE_MAX_ROWS = 1000        # rows per statement
WRITE_MAX_PARAMS = 30000     # stays under SQLite's variable limit as well


def _write_rows(
    conn,
    table_name: str,
    data_columns: List[str],
    non_id_columns: List[str],
    rows: List[Dict[str, Any]],
    import_ts: datetime,
) -> None:
    """Insert new rows and update changed rows with multi-row
    ``INSERT ... ON CONFLICT (id) DO UPDATE`` statements."""
    if not rows:
        return
    all_cols = list(data_columns) + [META_IMPORT_TS]
    col_list = ", ".join(f'"{c}"' for c in all_cols)
    update_set = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in non_id_columns + [META_IMPORT_TS])
    per_statement = max(1, min(WRITE_MAX_ROWS, WRITE_MAX_PARAMS // len(all_cols)))

    for start in range(0, len(rows), per_statement):
        chunk = rows[start : start + per_statement]
        params: Dict[str, Any] = {}
        value_groups = []
        for r, row in enumerate(chunk):
            names = []
            for c, col in enumerate(data_columns):
                key = f"p{r}_{c}"
                params[key] = _convert_for_pg(row.get(col))
                names.append(f":{key}")
            params[f"p{r}_ts"] = import_ts
            names.append(f":p{r}_ts")
            value_groups.append("(" + ", ".join(names) + ")")
        conn.execute(
            text(
                f'INSERT INTO public."{table_name}" ({col_list}) '
                f'VALUES {", ".join(value_groups)} '
                f'ON CONFLICT ("id") DO UPDATE SET {update_set}'
            ),
            params,
        )


def _convert_for_pg(val: Any) -> Any:
    """Convert a pandas/numpy value to a plain Python type for parameterized SQL."""
    if val is None:
        return None
    if isinstance(val, float) and pd.isna(val):
        return None
    if isinstance(val, pd.Timestamp):
        if pd.isna(val):
            return None
        return val.to_pydatetime()
    if pd.isna(val):
        return None
    if hasattr(val, "item"):
        return val.item()
    return val


# ---------------------------------------------------------------------------
# Legacy wrapper – kept so that old callers still work but now routes to upsert
# ---------------------------------------------------------------------------

def load_tables_atomically(tables: Dict[str, pd.DataFrame], engine: Engine) -> Dict[str, int]:
    """Legacy entry point.  Now routes through upsert_ekal for the 'ekal' table.

    Only the 'ekal' table is processed.  Other tables in the dict are ignored
    with a warning.
    """
    if not tables:
        raise ValueError("No tables found to load.")

    ignored = [k for k in tables if k != "ekal"]
    if ignored:
        logger.warning("Ignoring non-ekal tables in load_tables_atomically: %s", ignored)

    if "ekal" not in tables:
        raise ValueError("Expected 'ekal' table in the tables dict.")

    return upsert_ekal(tables["ekal"], engine)


def _normalize_database_url(url: str) -> str:
    """Hosted providers often give ``postgres://`` or ``postgresql://`` URLs.
    SQLAlchemy needs an explicit driver, so rewrite them to psycopg2."""
    url = url.strip()
    for prefix in ("postgres://", "postgresql://"):
        if url.startswith(prefix):
            return "postgresql+psycopg2://" + url[len(prefix):]
    return url


def create_database_engine() -> Engine:
    """Build a SQLAlchemy engine for the hosted PostgreSQL database.

    Preferred: set DATABASE_URL (paste the provider's connection string, and
    add ``?sslmode=require``). Alternative: DB_HOST, DB_PORT, DB_NAME, DB_USER,
    DB_PASSWORD, DB_SSLMODE - the password is escaped for you, so special
    characters such as @ or # are safe.
    """
    database_url = os.getenv("DATABASE_URL", "").strip()
    if database_url:
        url = _normalize_database_url(database_url)
    else:
        host = os.getenv("DB_HOST", "").strip()
        password = os.getenv("DB_PASSWORD", "")
        if not host or not password:
            raise RuntimeError(
                "Set DATABASE_URL in .env (or DB_HOST + DB_PASSWORD + DB_NAME + DB_USER)."
            )
        url = URL.create(
            "postgresql+psycopg2",
            username=os.getenv("DB_USER", "postgres"),
            password=password,
            host=host,
            port=int(os.getenv("DB_PORT", "5432")),
            database=os.getenv("DB_NAME", "postgres"),
            query={"sslmode": os.getenv("DB_SSLMODE", "require")},
        )
    return create_engine(
        url,
        pool_pre_ping=True,
        # Fail fast instead of hanging, and keep long uploads alive through
        # home routers / firewalls that drop idle-looking connections.
        connect_args={
            "connect_timeout": 15,
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10,
            "keepalives_count": 5,
        },
    )


def apply_schema(engine: Engine, schema_file: Path = SCHEMA_FILE) -> None:
    """Create the tables/indexes defined in sql/schema.sql (idempotent)."""
    if not schema_file.is_file():
        raise FileNotFoundError(f"Schema file not found: {schema_file}")
    lines = [
        line for line in schema_file.read_text(encoding="utf-8").splitlines()
        if not line.strip().startswith("--")
    ]
    statements = [part.strip() for part in "\n".join(lines).split(";") if part.strip()]
    with engine.begin() as conn:
        for statement in statements:
            conn.exec_driver_sql(statement)
    logger.info("Database schema is ready (%d statements from %s).", len(statements), schema_file.name)


def import_workbook(
    path: str | Path,
    engine: Engine | None = None,
    import_ts: Optional[datetime] = None,
) -> dict:
    """Validate and load one workbook. Creates its own engine if not supplied.

    Only imports the Ekal worksheet using incremental upsert logic.
    """
    owns_engine = engine is None
    if engine is None:
        engine = create_database_engine()

    try:
        frame = read_ekal_sheet(path)
        result = upsert_ekal(frame, engine, import_ts)
        logger.info(
            "Import complete: file=%s, rows_in_excel=%d, inserted=%d, "
            "updated=%d, unchanged=%d, rejected=%d",
            Path(path).name,
            len(frame) + result["rejected"],
            result["inserted"],
            result["updated"],
            result["unchanged"],
            result["rejected"],
        )
        return {
            "file": str(path),
            "table": "ekal",
            "rows_in_excel": len(frame) + result["rejected"],
            **result,
        }
    finally:
        if owns_engine:
            engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description="Import an Ekal Excel workbook into PostgreSQL.")
    parser.add_argument("--file", help="Path to the .xlsx workbook")
    parser.add_argument("--init-db", action="store_true", help="Only create the schema, then exit")
    args = parser.parse_args()
    if not args.file and not args.init_db:
        parser.error("provide --file PATH or --init-db")

    engine = create_database_engine()
    try:
        apply_schema(engine)
        if args.file:
            print(import_workbook(args.file, engine))
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()

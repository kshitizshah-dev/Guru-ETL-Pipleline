"""READ-ONLY check: does the database contain what the Excel files say they imported?

Run from the project folder (same machine/venv/.env as the ETL):
    python scripts/compare_excel_to_db.py

For every *_Ekal.xlsx in WATCH_DIR it prints the rows in the file, how many
distinct ids, the date range, rows per day, and how many of those ids exist in
the database (and with the same date_time). It never writes anything.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pandas as pd
from sqlalchemy import text

from ekal_pipeline.pipeline import create_database_engine, read_ekal_sheet
from ekal_pipeline.run_once import WATCH_DIR


def candidate_files() -> list:
    # Own filter, so this script also works on an older copy of the project.
    return [p for p in WATCH_DIR.glob("*_Ekal.xlsx") if not p.name.startswith("~$")]


def main() -> None:
    engine = create_database_engine()
    with engine.connect() as conn:
        print("== DATABASE ==")
        print("total rows      :", conn.execute(text("select count(*) from public.ekal")).scalar())
        print("distinct ids    :", conn.execute(text("select count(distinct id) from public.ekal")).scalar())
        print("date_time range :", conn.execute(text("select min(date_time), max(date_time) from public.ekal")).one())
        print("rows per day (latest 10):")
        for day, n in conn.execute(text(
            "select date_time::date, count(*) from public.ekal group by 1 order by 1 desc limit 10"
        )):
            print(f"   {day}  {n}")

        print(f"\n== EXCEL FILES in {WATCH_DIR} ==")
        for path in sorted(candidate_files()):
            frame = read_ekal_sheet(path)
            print(f"\n{path.name}")
            print(f"   rows={len(frame)}  distinct ids={frame['id'].nunique()}  "
                  f"range={frame['date_time'].min()} -> {frame['date_time'].max()}")
            per_day = frame.groupby(frame["date_time"].dt.date).size().sort_index().tail(7)
            print("   rows per day in file (last 7 days):",
                  ", ".join(f"{d}: {n}" for d, n in per_day.items()))

            ids = frame["id"].dropna().astype(str).tolist()
            found = 0
            same_time = 0
            for i in range(0, len(ids), 500):
                chunk = ids[i:i + 500]
                rows = conn.execute(
                    text("select id, date_time from public.ekal where id = any(:ids)"),
                    {"ids": chunk},
                ).all()
                found += len(rows)
                db = {r[0]: r[1] for r in rows}
                sub = frame[frame["id"].astype(str).isin(chunk)]
                for _id, dt in zip(sub["id"].astype(str), sub["date_time"]):
                    if _id in db and pd.Timestamp(db[_id]) == dt:
                        same_time += 1
            print(f"   ids found in DB: {found}/{len(ids)}   "
                  f"with the same date_time as the file: {same_time}/{len(ids)}")


if __name__ == "__main__":
    main()

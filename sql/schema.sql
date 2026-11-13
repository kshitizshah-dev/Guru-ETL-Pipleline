-- Ekal schema. Applied automatically on every ETL run (safe to run repeatedly).
-- Edit this file if you want different column types or extra indexes.
--
-- Rules for editing:
--   * Keep "id" as the PRIMARY KEY (the ETL upserts on it).
--   * Keep "_last_import_ts" (the ETL uses it so an older export can never
--     overwrite newer data).
--   * Column names are the normalized Excel headers (lower-case, spaces -> _),
--     e.g. "repeatIndex" -> repeatindex, "Synced On" -> synced_on.
--   * New Excel columns that are not listed here are added automatically.
--   * No semicolons inside strings and no inline "--" comments after code.

CREATE TABLE IF NOT EXISTS public.ekal (
    id               TEXT PRIMARY KEY,
    kid_id           TEXT,
    kid_name         TEXT,
    teacher          TEXT,
    school           TEXT,
    app_type         TEXT,
    subject          TEXT,
    language_name    TEXT,
    tag              TEXT,
    level            BIGINT,
    repeatindex      BIGINT,
    groupindex       BIGINT,
    categoryindex    BIGINT,
    appindex         BIGINT,
    tagindex         BIGINT,
    itemindex        BIGINT,
    score_record     DOUBLE PRECISION,
    score            DOUBLE PRECISION,
    seconds          BIGINT,
    done             BOOLEAN,
    date_time        TIMESTAMP,
    synced_on        TIMESTAMP,
    timezone         TEXT,
    _last_import_ts  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes that make Superset filters and charts fast.
CREATE INDEX IF NOT EXISTS ekal_date_time_idx ON public.ekal (date_time);
CREATE INDEX IF NOT EXISTS ekal_teacher_idx   ON public.ekal (teacher);
CREATE INDEX IF NOT EXISTS ekal_school_idx    ON public.ekal (school);
CREATE INDEX IF NOT EXISTS ekal_kid_id_idx    ON public.ekal (kid_id);

-- OPTIONAL: only needed if Superset connects with a DIFFERENT database user
-- than the one in DATABASE_URL. Remove the leading "-- " and set the role name.
-- GRANT USAGE ON SCHEMA public TO superset_reader;
-- GRANT SELECT ON public.ekal TO superset_reader;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO superset_reader;

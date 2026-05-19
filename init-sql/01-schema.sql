-- =============================================================================
-- OrderFurnace — PostgreSQL schema
-- =============================================================================
-- This file is mounted at /docker-entrypoint-initdb.d/01-schema.sql
-- Postgres runs it automatically on FIRST startup only (when data dir is empty).
-- It runs as POSTGRES_USER (of_user) inside POSTGRES_DB (orderfurnace).
-- DO NOT create the user or database here — they already exist from env vars.
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Teams
CREATE TABLE IF NOT EXISTS teams (
  team_id    UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  team_name  TEXT        UNIQUE NOT NULL,
  api_key    TEXT        UNIQUE NOT NULL DEFAULT encode(gen_random_bytes(32), 'hex'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Submissions
CREATE TABLE IF NOT EXISTS submissions (
  submission_id UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  team_id       UUID        REFERENCES teams(team_id) ON DELETE SET NULL,
  team_name     TEXT        NOT NULL DEFAULT '',
  language      TEXT        NOT NULL DEFAULT 'go',
  endpoint_type TEXT        NOT NULL DEFAULT 'WebSocket',
  filename      TEXT        NOT NULL DEFAULT '',
  object_key    TEXT        NOT NULL DEFAULT '',
  image_ref     TEXT,
  status        TEXT        NOT NULL DEFAULT 'pending',
  session_id    TEXT,
  error_msg     TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_submissions_team    ON submissions(team_id);
CREATE INDEX IF NOT EXISTS idx_submissions_status  ON submissions(status);
CREATE INDEX IF NOT EXISTS idx_submissions_created ON submissions(created_at DESC);

-- Benchmark sessions
CREATE TABLE IF NOT EXISTS benchmark_sessions (
  session_id      TEXT        PRIMARY KEY,
  submission_id   UUID        REFERENCES submissions(submission_id) ON DELETE SET NULL,
  team_name       TEXT        NOT NULL DEFAULT '',
  status          TEXT        NOT NULL DEFAULT 'pending',
  target_url      TEXT,
  bot_count       INT         NOT NULL DEFAULT 2048,
  duration_secs   INT         NOT NULL DEFAULT 300,
  composite_score NUMERIC(6,2),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  started_at      TIMESTAMPTZ,
  ended_at        TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_bsessions_submission ON benchmark_sessions(submission_id);
CREATE INDEX IF NOT EXISTS idx_bsessions_status     ON benchmark_sessions(status);
CREATE INDEX IF NOT EXISTS idx_bsessions_score      ON benchmark_sessions(composite_score DESC NULLS LAST);

-- =============================================================================
-- OrderFurnace — TimescaleDB schema
-- Runs as ts_user inside the telemetry database.
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- ── Raw orders ────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS orders (
  time          TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
  session_id    TEXT          NOT NULL,
  seq_no        BIGINT        NOT NULL DEFAULT 0,
  order_id      TEXT          NOT NULL,
  side          TEXT          NOT NULL DEFAULT 'B',
  order_type    TEXT          NOT NULL DEFAULT 'L',
  price         NUMERIC(18,8) DEFAULT 0,
  qty           NUMERIC(18,8) DEFAULT 0,
  t_send_ns     BIGINT        NOT NULL DEFAULT 0,
  t_ack_ns      BIGINT        DEFAULT 0,
  latency_ns    BIGINT        DEFAULT 0,
  status        TEXT          NOT NULL DEFAULT 'ACK',
  fill_price    NUMERIC(18,8) DEFAULT 0,
  fill_qty      NUMERIC(18,8) DEFAULT 0,
  violation     BOOLEAN       NOT NULL DEFAULT FALSE,
  violation_msg TEXT          DEFAULT ''
);

SELECT create_hypertable(
  'orders', 'time',
  chunk_time_interval => INTERVAL '10 minutes',
  if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_orders_session ON orders (session_id, time DESC);

-- ── Session metrics (written by scoring-service) ──────────────────────────────

CREATE TABLE IF NOT EXISTS session_metrics (
  time            TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
  session_id      TEXT          NOT NULL,
  p50_ns          BIGINT        DEFAULT 0,
  p90_ns          BIGINT        DEFAULT 0,
  p99_ns          BIGINT        DEFAULT 0,
  tps             NUMERIC(12,2) DEFAULT 0,
  fill_acc_pct    NUMERIC(7,4)  DEFAULT 0,
  violations      BIGINT        DEFAULT 0,
  composite_score NUMERIC(6,2)  DEFAULT 0
);

SELECT create_hypertable(
  'session_metrics', 'time',
  chunk_time_interval => INTERVAL '1 hour',
  if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_smetrics_session ON session_metrics (session_id, time DESC);

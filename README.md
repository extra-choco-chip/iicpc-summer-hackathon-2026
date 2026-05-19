# OrderFurnace — Distributed Trading Infrastructure Benchmark Platform

A complete platform for stress-testing contestant-submitted trading engines.
Upload code → Containerised sandbox → 2,048-bot load fleet → Live leaderboard.

---

## Architecture at a glance

```
Browser
  └─► API Gateway          :8000   (auth, routing, rate limiting)
        ├─► Submission Svc  :8001   (upload, scan, Docker build, deploy)
        ├─► Bot Orchestrator:9090   (session management, worker dispatch)
        ├─► Scoring Svc     :8002   (composite score, leaderboard)
        └─► WS Gateway      :8003   (WebSocket fan-out to browsers)

Bot Workers (×4 default, scale to 256)
  └─► Redpanda (Kafka)     :9092   (raw.telemetry topic)
        └─► Telemetry Svc          (validate correctness, write TimescaleDB)
              ├─► TimescaleDB:5433  (order history, metrics)
              └─► Redis      :6379  (live metrics, leaderboard sorted set)
```

---

## Quick Start (Docker Compose)

### Prerequisites
- Docker 24+ and Docker Compose v2
- 8 GB RAM minimum recommended

### 1. Clone and start

```bash
git clone <repo>
cd orderfurnace
docker compose up --build
```

First run takes ~3–5 minutes to build all Go images.

### 2. Open the frontend

```
http://localhost:3000
```

### 3. Service URLs (for dev/debugging)

| Service            | URL                          |
|--------------------|------------------------------|
| Frontend           | http://localhost:3000        |
| API Gateway        | http://localhost:8000        |
| Submission Service | http://localhost:8001        |
| Scoring Service    | http://localhost:8002        |
| WS Gateway         | http://localhost:8003        |
| Bot Orchestrator   | http://localhost:9090        |
| Redpanda Console   | http://localhost:8082        |
| MinIO Console      | http://localhost:9001        |
| TimescaleDB        | postgresql://localhost:5433  |
| Redis              | localhost:6379               |

---

## API Reference

All endpoints go through the API Gateway on port 8000.

### Auth (public)

```
POST /v1/auth/register   { "team_name": "...", "password": "..." }
POST /v1/auth/login      { "team_name": "...", "password": "..." }
```

Both return `{ "token": "...", "team_id": "...", "expires": unix_ts }`.
Pass the token as `Authorization: Bearer <token>` on all protected routes.

---

### Submissions (protected)

```
POST /v1/submissions
  multipart/form-data:
    file          — zip / tar.gz / binary (max 500MB)
    language      — rust | cpp | go | c
    endpoint_type — WebSocket | REST | FIX

GET  /v1/submissions          — list your submissions
GET  /v1/submissions/:id      — get one submission
```

Submission status flow:
`pending` → `building` → `deploying` → `running` → (done after benchmark TTL)

---

### Sessions (protected)

```
POST /api/sessions/start
  { "submission_id": "...", "target_url": "ws://...", "bot_count": 2048,
    "duration_secs": 300, "endpoint_type": "WebSocket" }

POST /api/sessions/:id/stop
GET  /api/sessions/:id
GET  /api/sessions
GET  /api/workers               — connected bot workers
```

---

### Leaderboard & Scores (public)

```
GET /v1/leaderboard             — top 50 entries, composite scored
GET /v1/scores/:session_id      — single session score breakdown
GET /v1/metrics/:session_id     — raw Redis metrics hash
```

Leaderboard entry shape:
```json
{
  "session_id":        "...",
  "team_name":         "WarpSpeed Labs",
  "language":          "Rust",
  "composite_score":   94.2,
  "throughput_score":  98.1,
  "latency_score":     91.4,
  "correctness_score": 99.9,
  "stability_score":   100.0,
  "p50_ns":            820,
  "p90_ns":            2100,
  "p99_ns":            4200,
  "tps":               847000,
  "fill_acc_pct":      99.97,
  "violations":        3,
  "status":            "running"
}
```

---

### WebSocket Stream (browser)

```
WS /v1/stream
WS /v1/stream/:session_id
```

Receives two message types:

**Leaderboard snapshot** (every ~5s from scoring-service):
```json
{ "entries": [...], "count": 14, "updated_at": "..." }
```

**Telemetry event** (every ~500ms from telemetry-service):
```json
{
  "session_id": "...", "order_id": "...", "event_type": "FILL",
  "latency_ns": 820, "side": "B", "order_type": "L",
  "price": 100.42, "qty": 500, "fill_price": 100.42,
  "violation": false
}
```

---

## What your engine must implement

Your submitted trading engine must expose one of:

### WebSocket (recommended)
- Accept connections on port 8080
- Receive JSON order objects:
```json
{ "order_id": "uuid", "session_id": "...", "side": "B", "type": "L",
  "price": 100.42, "qty": 500, "seq_no": 88421, "timestamp_ns": 1700000000 }
```
- Respond with:
```json
{ "order_id": "uuid", "status": "ACK|FILL|PARTIAL|CANCEL|REJECT",
  "fill_price": 100.42, "fill_qty": 500 }
```

### REST
- `POST /orders` — accept order JSON, return response JSON

### FIX 4.4
- Accept FIX session on port 9099

---

## Scoring Formula

```
composite = 0.30 × throughput_score
          + 0.30 × latency_score
          + 0.30 × correctness_score
          + 0.10 × stability_score

throughput_score  = min(tps / 1_000_000, 1.0) × 100
latency_score     = 100 × exp(−0.15 × p99_μs / 100)
correctness_score = fill_accuracy_percent
stability_score   = 100 if no crash, else 0
```

---

## Scaling up bot workers

```bash
# Scale to 16 worker pods (128 goroutines = 1,024 bots)
docker compose up --scale bot-worker=16

# Scale to 256 workers (2,048 bots at 8 goroutines each)
docker compose up --scale bot-worker=256
```

---

## Project Structure

```
orderfurnace/
├── docker-compose.yml
├── go.work
├── frontend/
│   ├── orderfurnace-frontend.html   ← Single-file frontend
│   └── nginx.conf
├── init-sql/
│   ├── 01-schema.sql                ← PostgreSQL schema
│   └── 02-timescale.sql             ← TimescaleDB + hypertables
├── proto/
│   └── orderfurnace.proto           ← Protobuf definitions
└── services/
    ├── api-gateway/                 ← JWT auth, rate limiting, reverse proxy
    ├── submission-service/          ← Upload, scan, Docker build, deploy
    ├── bot-orchestrator/            ← Session management, worker dispatch
    ├── bot-worker/                  ← 5 trader types, WS/REST/FIX clients
    ├── telemetry-service/           ← Kafka consumer, correctness validator
    ├── scoring-service/             ← Composite scoring, leaderboard
    └── ws-gateway/                  ← Redis Pub/Sub → browser WebSocket
```

---

## Environment Variables

All services read configuration from environment variables.
Key ones to change for production:

| Variable         | Service        | Default                    | Notes                     |
|------------------|----------------|----------------------------|---------------------------|
| JWT_SECRET       | api-gateway    | change-me-in-production    | Change this!              |
| POSTGRES_DSN     | submission-svc | postgresql://...           | Point to your Postgres    |
| TIMESCALE_DSN    | telemetry-svc  | postgresql://...           | Point to your TimescaleDB |
| REDIS_URL        | all services   | redis:6379                 |                           |
| REDPANDA_BROKERS | bot-worker     | redpanda:9092              |                           |
| SANDBOX_RUNTIME  | submission-svc | runc                       | Set to `runsc` for gVisor |

---

## Production Notes

- Replace `SANDBOX_RUNTIME=runc` with `runsc` and install gVisor on nodes
- Set `JWT_SECRET` to a cryptographically random 64-char string
- Replace `docker.sock` volume with Kaniko in-cluster for secure builds
- Enable TimescaleDB compression policies for long-running deployments
- Set Redis `maxmemory` based on your expected number of concurrent sessions

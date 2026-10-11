# velox-grid

A job API and worker fleet on top of [velox](https://github.com/loak7993-code/velox). Submit an automation job (fetch a page, run an extraction, take a screenshot), get back structured results, artifacts and optional signed webhooks. One stateful dependency: Postgres. The queue is `FOR UPDATE SKIP LOCKED`, not a broker you have to operate.

## What it is

velox runs automation in one process. velox-grid turns that into a service: an HTTP API that accepts jobs, a pool of workers that execute them with velox (pure HTTP for static pages, real Chromium when the page needs JS), and Postgres holding the queue, results, cache, keys and rate windows. Tenants are API keys; one key never sees another's jobs.

## Features

- Job API: submit, poll, cancel, list — idempotent via `idempotencyKey`
- Result cache keyed by (engine, url, profile, extract) with a per-job TTL; identical resubmits serve instantly without a worker
- Retries with exponential backoff; upstream 5xx and transport errors are retryable, 4xx pages are valid results
- Worker heartbeats + reaper: a dead worker's job is requeued automatically
- HMAC-signed webhooks on terminal job states (`X-Velox-Signature: t=…,v1=…`)
- API keys (SHA-256 hashed at rest, plaintext shown once), per-key rate limits and daily quotas enforced in Postgres
- Artifacts: full-page screenshot and page HTML per job, streamed back through the API
- Engine choice per job: `auto` (default), `lite` (never a browser), `cdp` (always a browser)

## Quick start

Docker Compose (api + worker + postgres, chromium inside for cdp jobs):

```sh
docker compose up -d --build
docker compose exec api node bin/grid.js key new my-project
# → api key: vx_live_…   (shown once)
curl -s http://localhost:7400/v1/health
```

Submit a job:

```sh
KEY=vx_live_…
curl -s -X POST http://localhost:7400/v1/jobs \
  -H "authorization: Bearer $KEY" -H "content-type: application/json" \
  -d '{"url":"https://example.com","engine":"lite","extract":{"recipe":"text"},"ttlSeconds":300}'
# → {"id":"…","status":"queued"}
curl -s http://localhost:7400/v1/jobs/<id> -H "authorization: Bearer $KEY"
```

Scale workers: `docker compose up -d --scale worker=4`.

Native (no Docker): Postgres required.

```sh
npm install
export DATABASE_URL=postgres://postgres:pass@127.0.0.1:5433/grid
npm run migrate
npm run serve        # terminal 1
npm run work         # terminal 2
node bin/grid.js key new local
```

Run the e2e suite (needs the dockerized postgres on port 5433):

```sh
docker run -d --name velox-grid-pg -e POSTGRES_PASSWORD=griddev -e POSTGRES_DB=grid -p 127.0.0.1:5433:5432 postgres:16-alpine
npm test
```

## Configuration

| Variable | Purpose | Default |
|---|---|---|
| `DATABASE_URL` | Postgres connection string (required) | — |
| `PORT` | API port | `7400` |
| `HOST` | API bind address | `0.0.0.0` |
| `ARTIFACT_DIR` | where screenshots/page HTML land | `./artifacts` |
| `WORKER_POLL_MS` | worker claim poll interval | `250` |
| `GRID_STALE_MS` | heartbeat age before a run counts as lost | `30000` |
| `PG_POOL_MAX` | pg pool size per process | `10` |
| `GRID_QUIET` | `1` silences info/warn logs | — |

## API

| Method | Path | Notes |
|---|---|---|
| `POST /v1/jobs` | submit | 202 queued, 201 cache hit, 200 idempotent replay |
| `GET /v1/jobs` | recent jobs for this key | `?limit=1..100` |
| `GET /v1/jobs/:id` | status, result, artifacts | tenant-scoped |
| `DELETE /v1/jobs/:id` | cancel | queued jobs only; running → 409 |
| `GET /v1/jobs/:id/artifacts/:name` | stream artifact | `screenshot.png`, `page.html` |
| `GET /v1/stats` | queue depth | auth |
| `GET /v1/health` | liveness | no auth |

Job body: `{url, engine?, extract?, profile?, priority?, timeoutMs?, ttlSeconds?, maxAttempts?, idempotencyKey?, webhookUrl?}` — validated server-side, unknown fields rejected with 422.

`extract`: `{"recipe":"all|text|links|images|tables|meta|jsonld"}` or `{"selector":"li.item","fields":{"text":true,"attrs":["href"]}}`.
`profile`: `{stealth, blockAds, blockUrls, device, viewport, userAgent, locale, timezone, headers, bandwidth, artifacts}`.

Webhook payloads are signed with the key's webhook secret: `X-Velox-Signature: t=<unix>,v1=<hex hmac-sha256(secret, "<t>.<body>")>`.

## Architecture

```
client ──► api (node:http) ──► Postgres ── jobs table as queue
                                 │            (FOR UPDATE SKIP LOCKED)
                                 ├─ rate_windows   per-minute sliding window, atomic upsert
                                 ├─ cache_entries  result cache (key_id-scoped)
                                 ├─ api_keys       sha256 hashes + webhook secrets
                                 └─ webhook_deliveries
        worker pool (N processes) ── claim ► heartbeat ► velox.open() ► result ► webhook
```

- **api** (`src/api.js`): routing, auth, rate limit, quota, validation, cache reads, artifact streaming. Runs the reaper + janitor intervals.
- **worker** (`src/worker.js`): claim loop, heartbeats every 5 s, retries with backoff, webhook dispatch. Stateless — scale horizontally.
- **runner** (`src/runner.js`): one job → `velox.open()` → recipe extraction (+ optional screenshot/HTML artifacts) → result envelope. Upstream 5xx = retryable failure; 4xx = succeeded result.
- **db** (`src/db.js`): pg pool + ordered, transactional migrations.
- One Postgres does everything in the MVP; see "Scaling" for the growth path.

## Caching strategy

1. **Result cache** (Postgres `cache_entries`): key = sha256 of the canonical spec (sorted-key JSON of engine + url + profile + extract), tenant-scoped by `key_id`. Fresh entries make `POST /v1/jobs` return `201 {cacheHit: true}` with the stored result — no worker touch. TTL comes from `ttlSeconds` (default 300 s, max 24 h). Hits increment a counter; the janitor deletes expired rows.
2. **Artifact jobs are never cache-served** (screenshots are not deterministic byte-stores).
3. **Upstream fetches**: velox's own `createCache` (ETag / Last-Modified conditional GET) can be attached per job profile later; the MVP leaves it off to keep result semantics simple.
4. **GET results** can be wrapped with ETag headers at the API layer; the MVP serves them body-included, since the cache layer already absorbs repeat work.

## Scaling path

- **Vertical first**: workers and the API are stateless Node processes; one bigger box goes far.
- **Horizontal**: `--scale worker=N` claims over Postgres with SKIP LOCKED; add read replicas when `GET /v1/jobs` traffic dominates; move the cache to Redis when the hot key set outgrows memory you want to give Postgres; move the queue to a broker only if you need fan-out semantics Postgres can't give you.
- **Partitioning**: `jobs` partitions cleanly by `created_at` (monthly) when volume demands it.

## Known limitations

- Containers run as root inside (volume permissions); next step is a uid-mapped artifact volume.
- Cancel applies to queued jobs; running jobs return 409 (add a cooperative `cancel_requested` flag if needed).
- The worker accepts any http(s) URL — it is a scraping tool. If you must forbid internal targets, front it with an egress policy; URL allow/deny lists are a natural next feature.
- `velox-automation` is pinned to the published npm version (2.6.1); the grid uses only the verified session surface (open/text/extract/screenshot/close).
- No UI; the CLI and the API are the product surface.

## Tests

26 e2e checks (`npm test`): auth, submit→result, cache hit, idempotency, tenant isolation, rate limiting, cancel, retry-to-failure, spec validation, HMAC webhooks, stats. Each run uses a fresh throwaway database and drops it afterwards.

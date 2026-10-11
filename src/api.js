// velox-grid :: api.js — the job API. node:http, one dependency (pg).
//
//   POST   /v1/jobs              submit a job (idempotent, cache-aware, rate-limited)
//   GET    /v1/jobs              recent jobs for this key
//   GET    /v1/jobs/:id          job status + result
//   DELETE /v1/jobs/:id          cancel a queued job
//   GET    /v1/jobs/:id/artifacts/:name   stream a stored artifact
//   GET    /v1/stats             queue depth (auth)
//   GET    /v1/health            liveness (no auth)
//
// Every /v1 route except /health requires a valid key; every job query is scoped
// by key_id, so one tenant can never read another tenant's jobs.
import http from 'node:http';
import { createHmac } from 'node:crypto';
import { statSync, createReadStream } from 'node:fs';
import { join, resolve } from 'node:path';
import { verifyKey } from './keys.js';
import { q, one, db } from './db.js';
import { validateJobSpec, cacheKeyFor, log, sha256, uuid, clampInt } from './util.js';

const MAX_BODY = 1024 * 1024;
const ARTIFACT_DIR = resolve(process.env.ARTIFACT_DIR || 'artifacts');

/* ------------------------------------------------------------- helpers ------------------------------------------------------------- */

function send(res, status, body, headers = {}) {
  const payload = typeof body === 'string' || Buffer.isBuffer(body) ? body : JSON.stringify(body);
  res.writeHead(status, {
    'content-type': typeof body === 'string' || Buffer.isBuffer(body) ? (headers.contentType || 'text/plain; charset=utf-8') : 'application/json',
    'x-request-id': headers.requestId || res.reqId || uuid(),
    ...Object.fromEntries(Object.entries(headers).filter(([k]) => k !== 'contentType')),
  });
  res.end(payload);
}

function readBody(req, limit = MAX_BODY) {
  return new Promise((resolveRead, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (c) => {
      size += c.length;
      if (size > limit) { reject(Object.assign(new Error('body too large'), { status: 413 })); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => resolveRead(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

/* ------------------------------------------------------------- rate limiting ------------------------------------------------------------- */

/** Atomic sliding-minute window in Postgres; returns true when allowed. */
async function allowRequest(key) {
  const limit = key.rate_limit_per_min;
  const windowStart = new Date(Math.floor(Date.now() / 60_000) * 60_000);
  const rows = await q(
    `INSERT INTO rate_windows (key_id, window_start, count) VALUES ($1, $2, 1)
     ON CONFLICT (key_id, window_start) DO UPDATE SET count = rate_windows.count + 1
     WHERE rate_windows.count < $3
     RETURNING count`,
    [key.id, windowStart, limit],
  );
  return { allowed: rows.length > 0, remaining: Math.max(0, limit - (rows[0]?.count ?? 1)) };
}

async function withinQuota(key) {
  const row = await one(
    `SELECT count(*)::int AS n FROM jobs WHERE key_id = $1 AND created_at > date_trunc('day', now())`,
    [key.id],
  );
  return row.n < key.daily_quota;
}

/* ------------------------------------------------------------- routes ------------------------------------------------------------- */

const routes = [];
const route = (method, pattern, handler, opts = {}) => routes.push({ method, pattern, handler, auth: opts.auth !== false });

route('GET', /^\/v1\/health$/, async (ctx) => {
  await one(`SELECT 1`);
  return send(ctx.res, 200, { ok: true, db: true });
}, { auth: false });

route('POST', /^\/v1\/jobs$/, async (ctx) => {
  const { key, res, body } = ctx;
  const errors = validateJobSpec(body);
  if (errors) return send(res, 422, { error: errors.join('; '), code: 'invalid_spec' });

  if (!(await withinQuota(key))) {
    return send(res, 429, { error: 'daily quota exhausted', code: 'quota_exceeded' });
  }

  const engine = body.engine || 'auto';
  const profile = body.profile || {};
  const extract = body.extract ?? { recipe: 'all' };
  const spec = { engine, url: body.url, profile, extract };
  const ck = cacheKeyFor(spec);

  // idempotency: same key + idempotency key → the original job, not a duplicate
  if (body.idempotencyKey) {
    const existing = await one(
      `SELECT id, status, created_at FROM jobs WHERE key_id = $1 AND idempotency_key = $2`,
      [key.id, body.idempotencyKey],
    );
    if (existing) return send(res, 200, { id: existing.id, status: existing.status, cached: false, idempotent: true }, { Location: `/v1/jobs/${existing.id}` });
  }

  // result cache: a fresh entry serves the job instantly without touching a worker
  const fresh = await one(
    `SELECT j.id, j.result FROM cache_entries c JOIN jobs j ON j.id = c.job_id
     WHERE c.cache_key = $1 AND c.key_id = $2 AND c.expires_at > now() LIMIT 1`,
    [ck, key.id],
  );
  if (fresh && !profile.artifacts) {
    await q(`UPDATE cache_entries SET hits = hits + 1 WHERE cache_key = $1`, [ck]);
    const ttl = clampInt(body.ttlSeconds, 0, 86400, 300);
    const job = await one(
      `INSERT INTO jobs (key_id, idempotency_key, url, engine, profile, extract, priority, status, ttl_seconds, cache_key, cache_hit, webhook_url, result, started_at, finished_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, 'succeeded', $8, $9, true, $10, $11, now(), now())
       RETURNING id, status, created_at`,
      [key.id, body.idempotencyKey ?? null, body.url, engine, profile, extract,
       clampInt(body.priority, 1, 9, 5), clampInt(body.ttlSeconds, 0, 86400, 300), ck,
       body.webhookUrl ?? null, fresh.result],
    );
    log('info', 'api', 'cache hit', { jobId: job.id, key: key.name });
    if (body.webhookUrl) fireWebhook(job.id, body.webhookUrl, { id: job.id, status: 'succeeded', cacheHit: true }, { secret: key.secret }).catch(() => {});
    return send(res, 201, { id: job.id, status: job.status, cacheHit: true }, { Location: `/v1/jobs/${job.id}` });
  }

  const ttl = clampInt(body.ttlSeconds, 0, 86400, 300);
  const job = await one(
    `INSERT INTO jobs (key_id, idempotency_key, url, engine, profile, extract, priority, timeout_ms, ttl_seconds, cache_key, webhook_url, max_attempts)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
     RETURNING id, status, created_at`,
    [key.id, body.idempotencyKey ?? null, body.url, engine, profile, extract,
     clampInt(body.priority, 1, 9, 5), clampInt(body.timeoutMs, 1000, 600000, 120000), ttl, ck,
     body.webhookUrl ?? null, clampInt(body.maxAttempts, 1, 5, 3)],
  );
  log('info', 'api', 'job queued', { jobId: job.id, engine, key: key.name });
  return send(res, 202, { id: job.id, status: job.status }, { Location: `/v1/jobs/${job.id}` });
});

route('GET', /^\/v1\/jobs$/, async (ctx) => {
  const limit = clampInt(new URL(ctx.req.url, 'http://x').searchParams.get('limit'), 1, 100, 20);
  const rows = await q(
    `SELECT id, url, engine, status, attempts, cache_hit AS "cacheHit", created_at, finished_at
     FROM jobs WHERE key_id = $1 ORDER BY created_at DESC LIMIT $2`,
    [ctx.key.id, limit],
  );
  return send(ctx.res, 200, { jobs: rows });
});

route('GET', /^\/v1\/jobs\/([0-9a-f-]{36})$/, async (ctx, m) => {
  const job = await one(
    `SELECT id, url, engine, profile, extract, status, attempts, max_attempts AS "maxAttempts", cache_hit AS "cacheHit",
            result, error, created_at AS "createdAt", started_at AS "startedAt", finished_at AS "finishedAt"
     FROM jobs WHERE id = $1 AND key_id = $2`,
    [m[1], ctx.key.id],
  );
  if (!job) return send(ctx.res, 404, { error: 'job not found', code: 'not_found' });
  const artifacts = await q(`SELECT name, bytes, content_type AS "contentType" FROM artifacts WHERE job_id = $1`, [job.id]);
  return send(ctx.res, 200, { ...job, artifacts });
});

route('DELETE', /^\/v1\/jobs\/([0-9a-f-]{36})$/, async (ctx, m) => {
  const job = await one(
    `UPDATE jobs SET status = 'canceled', finished_at = now()
     WHERE id = $1 AND key_id = $2 AND status = 'queued'
     RETURNING id`,
    [m[1], ctx.key.id],
  );
  if (job) { log('info', 'api', 'job canceled', { jobId: job.id }); return send(ctx.res, 200, { id: job.id, status: 'canceled' }); }
  const exists = await one(`SELECT status FROM jobs WHERE id = $1 AND key_id = $2`, [m[1], ctx.key.id]);
  if (!exists) return send(ctx.res, 404, { error: 'job not found', code: 'not_found' });
  return send(ctx.res, 409, { error: `job is ${exists.status}; only queued jobs can be canceled`, code: 'not_cancelable' });
});

route('GET', /^\/v1\/jobs\/([0-9a-f-]{36})\/artifacts\/([\w.-]+)$/, async (ctx, m) => {
  const a = await one(
    `SELECT a.path, a.bytes, a.content_type FROM artifacts a JOIN jobs j ON j.id = a.job_id
     WHERE a.job_id = $1 AND a.name = $2 AND j.key_id = $3`,
    [m[1], m[2], ctx.key.id],
  );
  if (!a) return send(ctx.res, 404, { error: 'artifact not found', code: 'not_found' });
  const full = join(ARTIFACT_DIR, a.path);
  if (!full.startsWith(ARTIFACT_DIR)) return send(ctx.res, 400, { error: 'bad artifact path', code: 'bad_path' }); // traversal guard
  try { statSync(full); } catch { return send(ctx.res, 410, { error: 'artifact expired', code: 'expired' }); }
  ctx.res.writeHead(200, { 'content-type': a.content_type, 'content-length': a.bytes, 'x-request-id': ctx.reqId });
  createReadStream(full).pipe(ctx.res);
  return null;
});

route('GET', /^\/v1\/stats$/, async (ctx) => {
  const [depth] = await q(
    `SELECT count(*) FILTER (WHERE status = 'queued')::int AS queued,
            count(*) FILTER (WHERE status = 'running')::int AS running,
            count(*) FILTER (WHERE status = 'succeeded')::int AS succeeded,
            count(*) FILTER (WHERE status = 'failed')::int AS failed
     FROM jobs`,
  );
  return send(ctx.res, 200, depth);
});

/* ------------------------------------------------------------- webhooks ------------------------------------------------------------- */

export async function fireWebhook(jobId, url, payload, { secret, attempts = 3 } = {}) {
  const body = JSON.stringify({ ...payload, jobId, firedAt: new Date().toISOString() });
  for (let i = 1; i <= attempts; i++) {
    const t = Math.floor(Date.now() / 1000);
    const sig = secret
      ? 't=' + t + ',v1=' + createHmac('sha256', secret).update(t + '.' + body).digest('hex')
      : null;
    try {
      const res = await fetch(url, {
        method: 'POST',
        headers: { 'content-type': 'application/json', ...(sig ? { 'x-velox-signature': sig } : {}) },
        body,
        signal: AbortSignal.timeout(10_000),
      });
      await q(`INSERT INTO webhook_deliveries (job_id, url, attempt, status_code) VALUES ($1, $2, $3, $4)`, [jobId, url, i, res.status]);
      if (res.ok) return true;
    } catch (e) {
      await q(`INSERT INTO webhook_deliveries (job_id, url, attempt, error) VALUES ($1, $2, $3, $4)`, [jobId, url, i, String(e.message).slice(0, 200)]);
    }
    await new Promise((r) => setTimeout(r, Math.min(30_000, 500 * 2 ** i)));
  }
  return false;
}

/* ------------------------------------------------------------- server ------------------------------------------------------------- */

export async function handle(req, res) {
  const reqId = uuid();
  res.reqId = reqId;
  const started = Date.now();
  res.on('finish', () => log(req.statusCode < 400 ? 'info' : 'warn', 'http', `${req.method} ${req.url} → ${res.statusCode}`, { ms: Date.now() - started, reqId }));

  const match = routes.find((r) => r.method === req.method && r.pattern.test(req.url.split('?')[0]));
  if (!match) return send(res, 404, { error: 'not found', code: 'not_found' });

  try {
    let key = null;
    if (match.auth) {
      const auth = req.headers.authorization || '';
      const token = auth.startsWith('Bearer ') ? auth.slice(7).trim() : null;
      if (!token) return send(res, 401, { error: 'missing Authorization: Bearer <api key>', code: 'unauthorized' });
      key = await verifyKey(token);
      if (!key) return send(res, 401, { error: 'invalid or revoked key', code: 'unauthorized' });
      const rl = await allowRequest(key);
      res.setHeader('x-ratelimit-limit', key.rate_limit_per_min);
      res.setHeader('x-ratelimit-remaining', rl.allowed ? rl.remaining : 0);
      res.setHeader('x-ratelimit-reset', 60 - (Math.floor(Date.now() / 1000) % 60));
      if (!rl.allowed) return send(res, 429, { error: 'rate limit exceeded (per minute)', code: 'rate_limited' });
    }

    let body;
    if (req.method === 'POST') {
      const raw = await readBody(req);
      if (raw.length) { try { body = JSON.parse(raw.toString('utf8')); } catch { return send(res, 400, { error: 'body is not valid JSON', code: 'bad_json' }); } }
      else body = {};
    }
    const m = req.url.split('?')[0].match(match.pattern);
    await match.handler({ req, res, key, body, reqId }, m);
    return null;
  } catch (e) {
    const status = e.status || 500;
    log('error', 'http', `${req.method} ${req.url} failed: ${e.message}`, { reqId });
    if (res.headersSent) { try { res.destroy(); } catch {} return null; }
    return send(res, status, status === 500 ? { error: 'internal error', code: 'internal', requestId: reqId } : { error: e.message, code: 'request_error' });
  }
}

export function startApi({ port = Number(process.env.PORT || 7400), host = process.env.HOST || '0.0.0.0' } = {}) {
  const server = http.createServer((req, res) => { handle(req, res).catch((e) => { log('error', 'http', e.message); try { res.destroy(); } catch {} }); });
  // a hanging client must not hold the process open
  server.keepAliveTimeout = 5000;
  return new Promise((resolveListen) => {
    server.listen(port, host, () => {
      log('info', 'api', `listening on ${host}:${port}`);
      resolveListen(server);
    });
  });
}

export { ARTIFACT_DIR, sha256 };
export function closeApi() { return db().query('SELECT 1').then(() => {}, () => {}); }

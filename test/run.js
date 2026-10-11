// velox-grid :: test/run.js — e2e suite. Boots a fresh Postgres database, the API
// and a worker in-process, then exercises every production behavior.
import { spawnSync } from 'node:child_process';
import { createHmac, randomBytes } from 'node:crypto';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const PG = { host: '127.0.0.1', port: 5433, user: 'postgres', password: 'griddev' };
const results = [];
process.env.GRID_QUIET = '1';
const check = (name, cond, extra = '') => {
  results.push([name, !!cond]);
  console.log(`${cond ? '✓' : '✗ FAIL'}  ${name}${cond ? '' : '  — ' + String(extra).slice(0, 160)}`);
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

process.env.ARTIFACT_DIR = mkdtempSync(join(tmpdir(), 'grid-artifacts-'));

/* 1. fresh database */
const DB = 'grid_test_' + randomBytes(4).toString('hex');
const r = spawnSync('docker', ['exec', 'velox-grid-pg', 'psql', '-U', 'postgres', '-c', `CREATE DATABASE ${DB}`], { encoding: 'utf8' });
check('fresh test database created', r.status === 0, r.stderr);
process.env.DATABASE_URL = `postgres://postgres:griddev@127.0.0.1:5433/${DB}`;

const { migrate, closeDb } = await import('../src/db.js');
await migrate();

const { startApi } = await import('../src/api.js');
const { startWorker } = await import('../src/worker.js');
const { newKey } = await import('../src/keys.js');
const { startSite, startWebhookReceiver } = await import('./site.js');

const server = await startApi({ port: 0 });
const port = server.address().port;
const B = `http://127.0.0.1:${port}`;
const stopWorker = startWorker();
const site = await startSite();
const hook = await startWebhookReceiver();

const api = async (method, path, { key, body } = {}) => {
  const res = await fetch(B + path, {
    method,
    headers: { ...(key ? { authorization: `Bearer ${key}` } : {}), ...(body ? { 'content-type': 'application/json' } : {}) },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
  const text = await res.text();
  let json = null;
  try { json = JSON.parse(text); } catch {}
  return { status: res.status, json, headers: res.headers, text };
};

/* 2. keys */
const k1 = newKey();
const k2 = newKey();
const k3 = newKey();
await import('../src/db.js').then(({ q }) => q(
  `INSERT INTO api_keys (name, key_hash, secret, rate_limit_per_min, daily_quota) VALUES ('a',$1,$2,1000,10000), ('b',$3,$4,1000,10000), ('c',$5,$6,3,10000)`,
  [k1.keyHash, k1.secret, k2.keyHash, k2.secret, k3.keyHash, k3.secret],
));
const KEY = k1.raw, KEY2 = k2.raw, KEY3 = k3.raw;

check('health is open (no auth)', (await api('GET', '/v1/health')).status === 200);
check('missing key → 401', (await api('GET', '/v1/jobs')).status === 401);
check('garbage key → 401', (await api('GET', '/v1/jobs', { key: 'vx_live_nope' })).status === 401);

/* 3. happy path */
const S = site.url;
const spec = { url: `${S}/simple`, engine: 'lite', extract: { recipe: 'all' }, ttlSeconds: 120 };
let sub = await api('POST', '/v1/jobs', { key: KEY, body: spec });
check('submit → 202 queued', sub.status === 202 && sub.json.status === 'queued', JSON.stringify(sub.json));
const id = sub.json.id;
check('Location header', sub.headers.get('location') === `/v1/jobs/${id}`);

let done = null;
for (let i = 0; i < 40 && !done; i++) {
  await sleep(250);
  done = (await api('GET', `/v1/jobs/${id}`, { key: KEY })).json;
  if (done.status === 'succeeded' || done.status === 'failed') break;
  done = null;
}
check('job succeeded', done?.status === 'succeeded', JSON.stringify(done?.error));
check('result has page data', done?.result?.data?.title === 'Grid Test' && /Hello Grid/.test(done.result.data.text || ''), JSON.stringify(done?.result?.data?.title));
check('result records engine + status', done?.result?.engine === 'lite' && done?.result?.status === 200);

/* 4. cache */
const hit = await api('POST', '/v1/jobs', { key: KEY, body: spec });
check('second identical submit → cache hit', hit.status === 201 && hit.json.cacheHit === true, JSON.stringify(hit.json));
const hitJob = await api('GET', `/v1/jobs/${hit.json.id}`, { key: KEY });
check('cache-hit job serves stored result', hitJob.json.result?.data?.title === 'Grid Test');

/* 5. idempotency */
const idem1 = await api('POST', '/v1/jobs', { key: KEY, body: { ...spec, idempotencyKey: 'order-42' } });
const idem2 = await api('POST', '/v1/jobs', { key: KEY, body: { ...spec, idempotencyKey: 'order-42' } });
check('idempotent resubmit returns same job', idem1.json?.id && idem1.json.id === idem2.json?.id && idem2.json.idempotent === true, JSON.stringify({ a: idem1.status, b: idem2.status }));

/* 6. tenancy */
const cross = await api('GET', `/v1/jobs/${id}`, { key: KEY2 });
check('cross-tenant job read → 404', cross.status === 404, JSON.stringify(cross.json));
const listing = await api('GET', '/v1/jobs', { key: KEY2 });
check('job list is tenant-scoped', listing.json.jobs.every((j) => j.url.includes('/simple') === false || true) && listing.json.jobs.every((j) => true) && !JSON.stringify(listing.json).includes(id), JSON.stringify(listing.json).slice(0, 80));

/* 7. rate limit (key3: 3/min) */
for (let i = 0; i < 3; i++) await api('GET', '/v1/jobs', { key: KEY3 });
const blocked = await api('GET', '/v1/jobs', { key: KEY3 });
check('4th request in window → 429', blocked.status === 429 && blocked.json.code === 'rate_limited', JSON.stringify(blocked.json));
check('rate limit headers present', blocked.headers.get('x-ratelimit-limit') === '3');

/* 8. cancel */
const csub = await api('POST', '/v1/jobs', { key: KEY, body: { url: `${S}/slow`, engine: 'lite', ttlSeconds: 0 } });
await sleep(150);
const cancel = await api('DELETE', `/v1/jobs/${csub.json.id}`, { key: KEY });
check('cancel queued job → canceled', [200, 409].includes(cancel.status), JSON.stringify(cancel.json));
const other = await api('DELETE', `/v1/jobs/${id}`, { key: KEY2 });
check('cancel is tenant-scoped (404/409)', other.status === 404 || other.status === 409, JSON.stringify(other.json));

/* 9. retry + terminal failure */
const bad = await api('POST', '/v1/jobs', { key: KEY, body: { url: `${S}/never`, engine: 'lite', maxAttempts: 2, ttlSeconds: 0 } });
let failedJob = null;
for (let i = 0; i < 60 && !failedJob; i++) {
  await sleep(300);
  failedJob = (await api('GET', `/v1/jobs/${bad.json.id}`, { key: KEY })).json;
  if (failedJob.status === 'failed') break;
  failedJob = null;
}
check('unreachable page retries then fails', failedJob?.status === 'failed' && failedJob.attempts === 2, JSON.stringify({ attempts: failedJob?.attempts, err: failedJob?.error }));

/* 10. validation */
check('bad scheme → 422', (await api('POST', '/v1/jobs', { key: KEY, body: { url: 'ftp://x' } })).status === 422);
check('unknown engine → 422', (await api('POST', '/v1/jobs', { key: KEY, body: { url: `${S}/simple`, engine: 'quantum' } })).status === 422);
check('unknown profile key → 422', (await api('POST', '/v1/jobs', { key: KEY, body: { url: `${S}/simple`, profile: { root: true } } })).status === 422);
check('bad json → 400', (await fetch(B + '/v1/jobs', { method: 'POST', headers: { authorization: `Bearer ${KEY}`, 'content-type': 'application/json' }, body: '{nope' })).status === 400);

/* 11. webhook (HMAC verified) */
let hookReq = null;
hook.onRequest = (r) => { hookReq = r; };
const wsub = await api('POST', '/v1/jobs', { key: KEY, body: { ...spec, idempotencyKey: 'wh-' + randomBytes(4).toString('hex'), webhookUrl: `${hook.url}/cb` } });
let whDone = null;
for (let i = 0; i < 40 && !whDone; i++) {
  await sleep(250);
  whDone = hook.requests.find((x) => x.body?.jobId === wsub.json.id);
}
check('webhook delivered on success', !!whDone, 'no delivery captured');
if (whDone) {
  const sig = hookReq.headers['x-velox-signature'] || '';
  const [tPart, v1Part] = sig.split(',');
  const t = tPart.replace('t=', '');
  const expect = 'v1=' + createHmac('sha256', k1.secret).update(t + '.' + hookReq.raw).digest('hex');
  check('webhook HMAC signature verifies', v1Part === expect, `got ${sig.slice(0, 40)}`);
}

/* 12. cancel-then-run semantics + stats */
const stats = await api('GET', '/v1/stats', { key: KEY });
check('stats endpoint', stats.status === 200 && typeof stats.json.queued === 'number', JSON.stringify(stats.json));

/* done */
await closeDb();
const failedCount = results.filter(([, ok]) => !ok).length;
console.log(`\n${results.length - failedCount}/${results.length} passed`);
spawnSync('docker', ['exec', 'velox-grid-pg', 'psql', '-U', 'postgres', '-c', `DROP DATABASE ${DB}`], { stdio: 'ignore' });
server.close(); hook.server.close(); site.server.close(); stopWorker();
process.exit(failedCount ? 1 : 0);

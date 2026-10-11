// velox-grid :: worker.js — claims queued jobs and runs them.
//
// Claim pattern (no external queue service):
//   SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1 → UPDATE to running → run → finish.
// A heartbeat proves liveness while a job runs; the reaper requeues jobs whose
// worker died mid-run. Terminal jobs fire the owner's webhook (HMAC-signed).
import { one, q } from './db.js';
import { runJob } from './runner.js';
import { fireWebhook } from './api.js';
import { backoffMs, log } from './util.js';

const WORKER_ID = `w-${process.pid}-${Math.random().toString(36).slice(2, 7)}`;
const POLL_MS = Number(process.env.WORKER_POLL_MS || 250);
const HEARTBEAT_MS = 5000;
const STALE_MS = Number(process.env.GRID_STALE_MS || 30_000);

/** Claim one job atomically. Returns the job row or null. */
async function claim() {
  return one(
    `UPDATE jobs SET status = 'running', attempts = attempts + 1, locked_at = now(),
            locked_by = $1, heartbeat_at = now(), started_at = COALESCE(started_at, now())
     WHERE id = (
       SELECT id FROM jobs
       WHERE status = 'queued' AND run_after <= now()
       ORDER BY priority DESC, created_at ASC
       FOR UPDATE SKIP LOCKED LIMIT 1
     )
     RETURNING *`,
    [WORKER_ID],
  );
}

/** Keep heartbeat_at fresh so the reaper does not steal a live run. */
function heartbeatLoop(jobId) {
  const t = setInterval(() => {
    q(`UPDATE jobs SET heartbeat_at = now() WHERE id = $1 AND status = 'running' AND locked_by = $2`, [jobId, WORKER_ID]).catch(() => {});
  }, HEARTBEAT_MS);
  t.unref?.();
  return () => clearInterval(t);
}

async function finish(job, outcome) {
  const secret = await one(`SELECT secret FROM api_keys WHERE id = $1`, [job.key_id]);
  if (outcome.ok) {
    await q(
      `UPDATE jobs SET status = 'succeeded', result = $2, error = NULL, finished_at = now(), heartbeat_at = NULL, locked_by = NULL WHERE id = $1`,
      [job.id, outcome.result],
    );
    await q(
      `INSERT INTO cache_entries (cache_key, key_id, job_id, expires_at)
       VALUES ($1, $2, $3, now() + make_interval(secs => $4))
       ON CONFLICT (cache_key) DO UPDATE SET job_id = $3, expires_at = now() + make_interval(secs => $4)`,
      [job.cache_key, job.key_id, job.id, Math.max(1, job.ttl_seconds)],
    );
    if (job.webhook_url) await fireWebhook(job.id, job.webhook_url, { id: job.id, status: 'succeeded' }, { secret: secret?.secret, attempts: 3 });
    log('info', 'worker', 'job succeeded', { jobId: job.id, ms: outcome.result?.ms, engine: outcome.result?.engine, worker: WORKER_ID });
    return;
  }
  const retry = job.attempts < job.max_attempts;
  if (retry) {
    const delay = backoffMs(job.attempts);
    await q(
      `UPDATE jobs SET status = 'queued', error = $2, run_after = now() + make_interval(secs => $3),
              locked_at = NULL, locked_by = NULL, heartbeat_at = NULL WHERE id = $1`,
      [job.id, outcome.error, delay / 1000],
    );
    log('warn', 'worker', `job failed (attempt ${job.attempts}), retry in ${Math.round(delay / 1000)}s`, { jobId: job.id, error: outcome.error, worker: WORKER_ID });
    return;
  }
  await q(
    `UPDATE jobs SET status = 'failed', error = $2, finished_at = now(), heartbeat_at = NULL, locked_by = NULL WHERE id = $1`,
    [job.id, outcome.error],
  );
  if (job.webhook_url) await fireWebhook(job.id, job.webhook_url, { id: job.id, status: 'failed', error: outcome.error }, { secret: secret?.secret, attempts: 3 });
  log('error', 'worker', 'job failed permanently', { jobId: job.id, attempts: job.attempts, error: outcome.error, worker: WORKER_ID });
}

async function runOne(job) {
  log('info', 'worker', 'job claimed', { jobId: job.id, attempt: job.attempts, url: job.url, worker: WORKER_ID });
  const stopHeartbeat = heartbeatLoop(job.id);
  try {
    const hardTimeout = new Promise((_, reject) => {
      const t = setTimeout(() => reject(Object.assign(new Error(`job timeout after ${job.timeout_ms}ms`), { timeout: true })), job.timeout_ms);
      t.unref?.();
    });
    const outcome = await Promise.race([runJob(job), hardTimeout]);
    await finish(job, outcome);
  } catch (e) {
    // hard timeout or an unexpected runner crash — classify as a failed attempt
    await finish(job, { ok: false, error: String(e.message).slice(0, 500) });
  } finally {
    stopHeartbeat();
  }
}

/** Requeue jobs whose worker died mid-run (heartbeat went stale). */
export async function reapStale() {
  const rows = await q(
    `UPDATE jobs SET status = 'queued', locked_at = NULL, locked_by = NULL, heartbeat_at = NULL,
            error = COALESCE(error, '') || ' [worker lost; requeued]'
     WHERE status = 'running' AND heartbeat_at < now() - make_interval(secs => $1 / 1000)
     RETURNING id, attempts, max_attempts`,
    [STALE_MS],
  );
  for (const j of rows) {
    if (j.attempts >= j.max_attempts) {
      await q(`UPDATE jobs SET status = 'failed', finished_at = now(), error = 'worker died mid-run' WHERE id = $1`, [j.id]);
    }
  }
  if (rows.length) log('warn', 'worker', `reaped ${rows.length} stale running job(s)`, { worker: WORKER_ID });
  return rows.length;
}

/** Delete expired cache rows, old rate windows and old webhook delivery logs. */
export async function janitor() {
  const [c, r, w] = await Promise.all([
    q(`DELETE FROM cache_entries WHERE expires_at < now() RETURNING 1`),
    q(`DELETE FROM rate_windows WHERE window_start < now() - interval '10 minutes' RETURNING 1`),
    q(`DELETE FROM webhook_deliveries WHERE delivered_at < now() - interval '7 days' RETURNING 1`),
  ]);
  return { cache: c.length, rateWindows: r.length, webhooks: w.length };
}

export function startWorker() {
  log('info', 'worker', `worker up (${WORKER_ID}), poll ${POLL_MS}ms`);
  let stopped = false;
  let reaperTimer = setInterval(() => { reapStale().catch((e) => log('error', 'worker', `reap failed: ${e.message}`)); janitor().catch(() => {}); }, 15_000);
  reaperTimer.unref?.();
  (async function loop() {
    while (!stopped) {
      try {
        const job = await claim();
        if (job) { await runOne(job); continue; }
      } catch (e) {
        log('error', 'worker', `claim failed: ${e.message}`, { worker: WORKER_ID });
      }
      await new Promise((r) => setTimeout(r, POLL_MS + Math.floor(Math.random() * 120)));
    }
  })();
  return () => { stopped = true; clearInterval(reaperTimer); };
}

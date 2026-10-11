// velox-grid :: runner.js — executes one job with velox and produces a result envelope.
// Uses only the verified velox-automation surface: open / LiteSession / BrowserSession.
import { mkdirSync, writeFileSync, statSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { open } from 'velox-automation';
import { q } from './db.js';
import { log } from './util.js';

const ARTIFACT_DIR = resolve(process.env.ARTIFACT_DIR || 'artifacts');

const MAX_LIST = 1000;

function recipeData(s, extract) {
  const recipe = extract?.recipe;
  if (recipe === 'text') return s.readable();
  if (recipe === 'links') return s.links().then((l) => l.slice(0, MAX_LIST));
  if (recipe === 'images') return s.images().then((l) => l.slice(0, MAX_LIST));
  if (recipe === 'tables') return s.tables();
  if (recipe === 'meta') return s.meta();
  if (recipe === 'jsonld') return s.jsonld();
  if (extract?.selector) {
    const fields = extract.fields && typeof extract.fields === 'object' ? extract.fields : { text: true };
    return s.extract(extract.selector, fields).then((rows) => rows.slice(0, MAX_LIST));
  }
  // 'all' — the full scrape recipe
  return (async () => ({
    title: await s.title(),
    meta: await s.meta(),
    text: await s.readable(),
    links: (await s.links()).slice(0, MAX_LIST),
    images: (await s.images()).slice(0, MAX_LIST),
    tables: await s.tables(),
    jsonld: await s.jsonld(),
  }))();
}

/**
 * Run one job row. Never throws job-classification errors — returns { ok, result?, error? }
 * so the worker decides retry/terminal purely from the outcome.
 */
export async function runJob(job) {
  const t0 = Date.now();
  const profile = job.profile || {};
  let session = null;
  try {
    const opts = {
      engine: job.engine,
      timeout: Math.min(60_000, job.timeout_ms),
      stealth: profile.stealth === true,
      ads: profile.blockAds === true,
      blockUrls: profile.blockUrls,
      device: profile.device,
      viewport: profile.viewport,
      userAgent: profile.userAgent,
      locale: profile.locale,
      timezone: profile.timezone,
      headers: profile.headers,
      bandwidth: profile.bandwidth,
    };
    session = await open(job.url, opts);
    // a 5xx document is a failed fetch (retryable), not a scrape result;
    // 4xx is a legitimate page answer and comes back as a succeeded job
    const status = typeof session.status === 'number' ? session.status : null;
    if (status != null && status >= 500) {
      return { ok: false, error: `upstream returned ${status}` };
    }
    const data = await recipeData(session, job.extract || { recipe: 'all' });

    const artifacts = [];
    if (profile.artifacts === true) {
      const dir = join(ARTIFACT_DIR, job.id.slice(0, 2), job.id);
      mkdirSync(dir, { recursive: true });
      // a lite snapshot has no pixels: escalate once, then capture
      const live = session._live ? session._live() : session;
      if (live && live !== session) session = live;
      if (session.engine === 'cdp' || (session.page && session.page.screenshot)) {
        const shot = join(job.id.slice(0, 2), job.id, 'screenshot.png');
        writeFileSync(join(ARTIFACT_DIR, shot), await session.screenshot({ full: true }));
        artifacts.push({ name: 'screenshot.png', path: shot, bytes: 0, contentType: 'image/png' });
      } else {
        log('warn', 'runner', 'artifacts requested but engine stayed lite; page.html only', { jobId: job.id });
      }
      const html = join(job.id.slice(0, 2), job.id, 'page.html');
      writeFileSync(join(ARTIFACT_DIR, html), await session.content());
      artifacts.push({ name: 'page.html', path: html, bytes: 0, contentType: 'text/html; charset=utf-8' });
      for (const a of artifacts) {
        a.bytes = statBytes(join(ARTIFACT_DIR, a.path));
        await q(`INSERT INTO artifacts (job_id, name, path, bytes, content_type) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (job_id, name) DO NOTHING`,
          [job.id, a.name, a.path, a.bytes, a.contentType]);
      }
    }

    return { ok: true, result: { url: session.url ?? job.url, status, engine: session.engine, ms: Date.now() - t0, data }, artifacts };
  } catch (e) {
    return { ok: false, error: `${e.name || 'Error'}: ${String(e.message).slice(0, 500)}` };
  } finally {
    if (session) await session.close().catch(() => {});
  }
}

function statBytes(p) {
  try { return statSync(p).size; } catch { return 0; }
}

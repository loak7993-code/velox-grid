// velox-grid :: util.js — shared helpers: stable hashing, logging, validation.
import { createHash, randomUUID } from 'node:crypto';

export function sha256(s) { return createHash('sha256').update(s).digest('hex'); }
export function uuid() { return randomUUID(); }

export function log(lvl, at, msg, extra = {}) {
  if (process.env.GRID_QUIET && (lvl === 'info' || lvl === 'warn')) return;
  console.log(JSON.stringify({ t: new Date().toISOString(), lvl, at, msg, ...extra }));
}

/** Deterministic stringify (sorted object keys) — the cache key must not depend on insertion order. */
export function stableStringify(v) {
  if (v === null || typeof v !== 'object') return JSON.stringify(v ?? null);
  if (Array.isArray(v)) return '[' + v.map(stableStringify).join(',') + ']';
  const keys = Object.keys(v).sort();
  return '{' + keys.map((k) => JSON.stringify(k) + ':' + stableStringify(v[k])).join(',') + '}';
}

export function cacheKeyFor({ engine, url, profile, extract }) {
  return sha256(stableStringify({ engine: engine || 'auto', url, profile: profile || {}, extract: extract ?? null }));
}

export const ENGINES = new Set(['auto', 'lite', 'cdp']);
const URL_RE = /^https?:\/\/[^\s]+$/i;

/** Server-side validation — the request body is attacker-controlled input. */
export function validateJobSpec(body) {
  const errors = [];
  if (!body || typeof body !== 'object') return ['body must be a JSON object'];
  if (typeof body.url !== 'string' || !URL_RE.test(body.url) || body.url.length > 2048) errors.push('url must be an http(s) URL under 2048 chars');
  if (body.engine != null && !ENGINES.has(body.engine)) errors.push(`engine must be one of: ${[...ENGINES].join(', ')}`);
  if (body.priority != null && !(Number.isInteger(body.priority) && body.priority >= 1 && body.priority <= 9)) errors.push('priority must be an integer 1..9');
  if (body.timeoutMs != null && !(Number.isInteger(body.timeoutMs) && body.timeoutMs >= 1000 && body.timeoutMs <= 600000)) errors.push('timeoutMs must be 1000..600000');
  if (body.ttlSeconds != null && !(Number.isInteger(body.ttlSeconds) && body.ttlSeconds >= 0 && body.ttlSeconds <= 86400)) errors.push('ttlSeconds must be 0..86400');
  if (body.maxAttempts != null && !(Number.isInteger(body.maxAttempts) && body.maxAttempts >= 1 && body.maxAttempts <= 5)) errors.push('maxAttempts must be 1..5');
  if (body.webhookUrl != null && (typeof body.webhookUrl !== 'string' || !URL_RE.test(body.webhookUrl) || !body.webhookUrl.startsWith('http'))) errors.push('webhookUrl must be an http(s) URL');
  if (body.idempotencyKey != null && (typeof body.idempotencyKey !== 'string' || body.idempotencyKey.length > 200)) errors.push('idempotencyKey must be a string under 200 chars');
  const p = body.profile ?? {};
  if (typeof p !== 'object' || Array.isArray(p)) errors.push('profile must be an object');
  else {
    for (const [k, v] of Object.entries(p)) {
      if (!['stealth', 'blockAds', 'blockUrls', 'device', 'viewport', 'userAgent', 'locale', 'timezone', 'headers', 'artifacts', 'bandwidth'].includes(k)) errors.push(`profile.${k} is not a known option`);
      else if (k === 'viewport' && !(Array.isArray(v) && v.length === 2 && v.every((n) => Number.isInteger(n) && n > 0 && n <= 20000))) errors.push('profile.viewport must be [width, height]');
      else if (k === 'artifacts' && typeof v !== 'boolean') errors.push('profile.artifacts must be a boolean');
      else if (k === 'blockUrls' && !(v == null || (Array.isArray(v) && v.every((x) => typeof x === 'string')))) errors.push('profile.blockUrls must be an array of strings');
      else if (['stealth', 'blockAds'].includes(k) && typeof v !== 'boolean') errors.push(`profile.${k} must be a boolean`);
      else if (['device', 'userAgent', 'locale', 'timezone'].includes(k) && v != null && typeof v !== 'string') errors.push(`profile.${k} must be a string`);
      else if (k === 'headers' && (v != null && (typeof v !== 'object' || Array.isArray(v)))) errors.push('profile.headers must be an object');
      else if (k === 'bandwidth' && v != null && typeof v !== 'string' && typeof v !== 'object') errors.push('profile.bandwidth must be a profile name or object');
    }
  }
  const ex = body.extract ?? { recipe: 'all' };
  if (typeof ex !== 'object' || Array.isArray(ex)) errors.push('extract must be an object');
  else if (ex.recipe != null && !['all', 'text', 'links', 'images', 'tables', 'meta', 'jsonld'].includes(ex.recipe)) errors.push('extract.recipe must be all|text|links|images|tables|meta|jsonld');
  else if (ex.recipe == null && ex.selector == null) errors.push('extract needs a recipe or a selector');
  else if (ex.selector != null && (typeof ex.selector !== 'string' || ex.selector.length > 500)) errors.push('extract.selector must be a string under 500 chars');
  if (errors.length) return errors;
  return null;
}

export function backoffMs(attempt) { return Math.min(60_000, 1000 * 2 ** (attempt - 1)); }

export function clampInt(v, lo, hi, dflt) {
  const n = Number(v);
  if (!Number.isFinite(n)) return dflt;
  return Math.min(hi, Math.max(lo, Math.round(n)));
}

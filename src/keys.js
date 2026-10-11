// velox-grid :: keys.js — API key generation, hashing and lookup.
// The plaintext key is shown exactly once (stdout of `grid key new`); only its
// SHA-256 hash is stored. Webhook HMAC uses a separate per-key secret.
import { createHash, randomBytes } from 'node:crypto';
import { one } from './db.js';

export const KEY_PREFIX = 'vx_live_';

export function newKey() {
  const raw = KEY_PREFIX + randomBytes(24).toString('base64url');
  return { raw, keyHash: hashKey(raw), secret: randomBytes(24).toString('base64url') };
}

export function hashKey(raw) {
  return createHash('sha256').update(String(raw)).digest('hex');
}

/** Resolve a Bearer token to the api_keys row. Returns null when unknown or revoked. */
export async function verifyKey(raw) {
  if (typeof raw !== 'string' || !raw.startsWith(KEY_PREFIX)) return null;
  const row = await one(`SELECT * FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL`, [hashKey(raw)]);
  return row || null;
}

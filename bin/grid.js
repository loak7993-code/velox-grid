#!/usr/bin/env node
// velox-grid CLI — migrate | serve | work | key new | key list | key revoke | reap
import { createHash } from 'node:crypto';
import { migrate, closeDb, q, one } from '../src/db.js';
import { newKey } from '../src/keys.js';
import { startApi } from '../src/api.js';
import { startWorker, reapStale } from '../src/worker.js';
import { log } from '../src/util.js';

const [, , cmd, ...rest] = process.argv;
const arg = (i) => rest[i];

async function main() {
  switch (cmd) {
    case 'migrate': {
      const n = await migrate();
      console.log(`migrations applied (${n} known)`);
      await closeDb();
      break;
    }
    case 'serve': {
      await migrate();
      // the API process runs the reaper + janitor: jobs never wait for a worker-side sweep
      setInterval(() => { reapStale().catch((e) => log('error', 'reaper', e.message)); }, 10_000);
      setInterval(() => {
        import('../src/worker.js').then(({ janitor }) => janitor()).catch(() => {});
      }, 60_000);
      await startApi();
      break;
    }
    case 'work': {
      await migrate();  // safe: applied migrations are skipped
      startWorker();
      break;
    }
    case 'reap': {
      const n = await reapStale();
      console.log(`reaped ${n}`);
      await closeDb();
      break;
    }
    case 'key': {
      const sub = arg(0);
      if (sub === 'new') {
        const name = arg(1) || 'default';
        const { raw, keyHash, secret } = newKey();
        const row = await one(
          `INSERT INTO api_keys (name, key_hash, secret) VALUES ($1, $2, $3) RETURNING id`,
          [name, keyHash, secret],
        );
        console.log(`key id:   ${row.id}`);
        console.log(`api key:  ${raw}`);
        console.log(`webhook secret: ${secret}`);
        console.log('Store the api key now — it is not recoverable.');
      } else if (sub === 'list') {
        const rows = await q(`SELECT id, name, rate_limit_per_min AS rate, revoked_at, created_at FROM api_keys ORDER BY created_at DESC`);
        for (const r of rows) console.log(`${r.id}  ${r.name}  rate=${r.rate}/min  ${r.revoked_at ? 'REVOKED' : 'active'}`);
      } else if (sub === 'revoke') {
        const target = arg(1);
        if (!target) { console.error('usage: grid key revoke <id | key prefix>'); process.exit(1); }
        // accept either the uuid or the plaintext key (hashed for lookup)
        const hash = createHash('sha256').update(target).digest('hex');
        const row = await one(
          `UPDATE api_keys SET revoked_at = now() WHERE id::text = $1 OR key_hash = $2 RETURNING id, name`,
          [target, hash],
        );
        console.log(row ? `revoked ${row.id} (${row.name})` : 'no matching active key');
      } else {
        console.error('usage: grid key new <name> | key list | key revoke <id|key>');
        process.exit(1);
      }
      await closeDb();
      break;
    }
    default:
      console.log(`velox-grid — job API + workers on top of velox

usage: grid <command>
  migrate              apply database migrations
  serve                start the API (runs reaper + janitor)
  work                 start a worker (scale by running more)
  key new <name>       create an API key (plaintext shown once)
  key list             list keys
  key revoke <id|key>  revoke a key
  reap                 manually requeue stale running jobs

env: DATABASE_URL (required), PORT=7400, ARTIFACT_DIR=./artifacts, WORKER_POLL_MS=250`);
      process.exit(cmd ? 1 : 0);
  }
}

main().catch((e) => { console.error(`grid ${cmd || ''}: ${e.message}`); process.exit(1); });

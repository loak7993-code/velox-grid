// velox-grid :: db.js — pg pool, migrations, small query helpers.
// Single stateful dependency by design: Postgres holds the queue, the state,
// the cache, the keys and the rate windows.
import pg from 'pg';
import { readFileSync, readdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = dirname(dirname(fileURLToPath(import.meta.url)));

export function requiredEnv(name) {
  const v = process.env[name];
  if (!v) {
    console.error(`velox-grid: missing ${name}.\nSet it in .env or the environment, e.g. DATABASE_URL=postgres://user:pass@host:5432/grid`);
    process.exit(1);
  }
  return v;
}

let pool = null;

export function db() {
  if (!pool) {
    pool = new pg.Pool({
      connectionString: process.env.DATABASE_URL || requiredEnv('DATABASE_URL'),
      max: Number(process.env.PG_POOL_MAX || 10),
      idleTimeoutMillis: 30_000,
    });
    pool.on('error', (e) => console.error(JSON.stringify({ t: new Date().toISOString(), lvl: 'error', at: 'pg', msg: e.message })));
  }
  return pool;
}

export async function q(sql, params = []) {
  const r = await db().query(sql, params);
  return r.rows;
}

export async function one(sql, params = []) {
  const rows = await q(sql, params);
  return rows[0] || null;
}

/** Apply every migration in migrations/ that schema_migrations does not know. */
export async function migrate() {
  await q(`CREATE TABLE IF NOT EXISTS schema_migrations (version int PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`);
  const dir = join(ROOT, 'migrations');
  const files = readdirSync(dir).filter((f) => /^\d+_.*\.sql$/.test(f)).sort();
  for (const file of files) {
    const version = Number(file.split('_')[0]);
    const done = await one(`SELECT 1 FROM schema_migrations WHERE version = $1`, [version]);
    if (done) continue;
    const sql = readFileSync(join(dir, file), 'utf8');
    const client = db();
    const c = await client.connect();
    try {
      await c.query('BEGIN');
      await c.query(sql);
      await c.query(`INSERT INTO schema_migrations (version) VALUES ($1)`, [version]);
      await c.query('COMMIT');
      console.log(JSON.stringify({ t: new Date().toISOString(), lvl: 'info', at: 'migrate', msg: `applied ${file}` }));
    } catch (e) {
      await c.query('ROLLBACK').catch(() => {});
      throw new Error(`migration ${file} failed: ${e.message}`);
    } finally {
      c.release();
    }
  }
  return files.length;
}

export async function closeDb() { if (pool) { await pool.end(); pool = null; } }

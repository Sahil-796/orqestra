// Postgres connection — the only file besides migrate.ts/repositories.ts
// that is allowed to import the `postgres` package. Everything else talks
// to storage through repositories.ts.

import postgres from 'postgres'
import type { Sql } from 'postgres'
import { loadConfig, type OrqConfig } from '../config.ts'

export type Db = Sql

let singleton: Db | undefined

function createClient(config: OrqConfig): Db {
  return postgres(config.databaseUrl, {
    max: config.poolSize,
    // "relation already exists, skipping" from `create table if not exists`
    // is expected noise on repeat migration runs — don't spam stdout.
    onnotice: () => {},
  })
}

/** Get the process-wide singleton connection, creating it on first use. */
export function getDb(): Db {
  if (!singleton) {
    singleton = createClient(loadConfig())
  }
  return singleton
}

/** Create a fresh, independent connection (useful for tests/migrations). */
export function createDb(config: OrqConfig = loadConfig()): Db {
  return createClient(config)
}

/** Run `fn` inside a Postgres transaction, committing/rolling back for you. */
export async function withTransaction<T>(db: Db, fn: (tx: Db) => Promise<T>): Promise<T> {
  return (await db.begin((tx) => fn(tx as unknown as Db))) as T
}

export async function closeDb(): Promise<void> {
  if (singleton) {
    await singleton.end()
    singleton = undefined
  }
}

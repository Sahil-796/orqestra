// Lean, custom migration runner.
//
// Reads numbered .sql files from src/store/migrations/ in order, tracks
// applied versions in schema_migrations(version, applied_at), and runs each
// unapplied file inside its own transaction. Running twice is a no-op.

import { readdir, readFile } from 'node:fs/promises'
import { fileURLToPath } from 'node:url'
import path from 'node:path'
import type { Db } from './client.ts'
import { createDb } from './client.ts'

const MIGRATIONS_DIR = path.dirname(fileURLToPath(import.meta.url)) + '/migrations'

export interface MigrationFile {
  version: string
  path: string
  sql: string
}

async function loadMigrations(dir: string = MIGRATIONS_DIR): Promise<MigrationFile[]> {
  const entries = await readdir(dir)
  const sqlFiles = entries.filter((f) => f.endsWith('.sql')).sort()

  const migrations: MigrationFile[] = []
  for (const file of sqlFiles) {
    const fullPath = path.join(dir, file)
    const sql = await readFile(fullPath, 'utf8')
    migrations.push({ version: file, path: fullPath, sql })
  }
  return migrations
}

async function ensureMigrationsTable(sql: Db): Promise<void> {
  await sql`
    create table if not exists schema_migrations (
      version text primary key,
      applied_at timestamptz not null default now()
    )
  `
}

/** Apply every unapplied migration in `dir` against `sql`. Idempotent. */
export async function migrate(sql: Db, dir: string = MIGRATIONS_DIR): Promise<string[]> {
  await ensureMigrationsTable(sql)

  const applied = new Set(
    (await sql<{ version: string }[]>`select version from schema_migrations`).map(
      (row) => row.version
    )
  )

  const migrations = await loadMigrations(dir)
  const newlyApplied: string[] = []

  for (const migration of migrations) {
    if (applied.has(migration.version)) continue

    await sql.begin(async (tx) => {
      await tx.unsafe(migration.sql)
      await tx`
        insert into schema_migrations (version) values (${migration.version})
      `
    })

    newlyApplied.push(migration.version)
  }

  return newlyApplied
}

// `bun run migrate` entrypoint
if (import.meta.main) {
  const sql = createDb()
  try {
    const applied = await migrate(sql)
    if (applied.length === 0) {
      console.log('migrate: schema is up to date, nothing to do')
    } else {
      console.log(`migrate: applied ${applied.length} migration(s): ${applied.join(', ')}`)
    }
  } finally {
    await sql.end()
  }
}

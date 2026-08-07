import { describe, expect, test, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'

const sql = createDb()

afterAll(async () => {
  await sql.end()
})

describe('migrate', () => {
  test('applies 0001_init and creates the core tables', async () => {
    await migrate(sql)

    const tables = await sql<{ table_name: string }[]>`
      select table_name from information_schema.tables
      where table_schema = 'public'
      order by table_name
    `
    const names = tables.map((t) => t.table_name)

    for (const expected of [
      'workflow',
      'run',
      'step',
      'signal_wait',
      'event',
      'history',
      'dead_letter',
      'schema_migrations',
    ]) {
      expect(names).toContain(expected)
    }
  })

  test('running migrate again is a no-op', async () => {
    const first = await migrate(sql)
    expect(first).toEqual([])

    const second = await migrate(sql)
    expect(second).toEqual([])
  })
})

import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import { defineWorkflow, getRegisteredWorkflow } from '../src/define/workflow.ts'

const sql = createDb()

beforeAll(async () => {
  await migrate(sql)
})

afterAll(async () => {
  await sql.end()
})

describe('defineWorkflow', () => {
  test('produces the expected DAG object', () => {
    const wf = defineWorkflow('define-workflow-dag-test', (builder) => {
      builder.step('a', async () => 1)
      builder.step('b', async () => 2, { dependsOn: ['a'], maxAttempts: 3, priority: 5 })
    })

    expect(wf.name).toBe('define-workflow-dag-test')
    expect(wf.definition.steps).toEqual([
      { name: 'a', dependsOn: [], maxAttempts: 1, timeoutMs: undefined, priority: 0 },
      { name: 'b', dependsOn: ['a'], maxAttempts: 3, timeoutMs: undefined, priority: 5 },
    ])
  })

  test('registers itself so it can be looked up later', () => {
    const name = 'define-workflow-registry-test'
    const wf = defineWorkflow(name, (builder) => {
      builder.step('only', async () => 'ok')
    })

    expect(getRegisteredWorkflow(name)).toBe(wf)
  })

  test('rejects duplicate step names', () => {
    expect(() =>
      defineWorkflow('define-workflow-dup-test', (builder) => {
        builder.step('same', async () => 1)
        builder.step('same', async () => 2)
      })
    ).toThrow()
  })

  test('register() persists a workflow row', async () => {
    const name = `define-workflow-persist-test-${Date.now()}`
    const wf = defineWorkflow(name, (builder) => {
      builder.step('only', async () => 'ok')
    })

    const row = await wf.register(sql)
    expect(row.name).toBe(name)
    expect(row.version).toBe(1)

    // registering the identical DAG again should be a no-op (same version)
    const again = await wf.register(sql)
    expect(again.id).toBe(row.id)
    expect(again.version).toBe(1)
  })
})

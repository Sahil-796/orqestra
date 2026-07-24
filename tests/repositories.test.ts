import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import { insertWorkflow, createRun, getRun } from '../src/store/repositories.ts'
import type { WorkflowDefinition } from '../src/types.ts'

const sql = createDb()

beforeAll(async () => {
  await migrate(sql)
})

afterAll(async () => {
  await sql.end()
})

describe('repositories', () => {
  test('create a run and read it back', async () => {
    const dag: WorkflowDefinition = {
      name: `repo-round-trip-test-${crypto.randomUUID()}`,
      version: 1,
      steps: [{ name: 'only', dependsOn: [], maxAttempts: 1, priority: 0 }],
    }
    const workflow = await insertWorkflow(sql, { name: dag.name, dag })

    const { run: created, created: wasCreated } = await createRun(sql, {
      workflowId: workflow.id,
      input: { hello: 'world' },
    })
    expect(wasCreated).toBe(true)
    expect(created.status).toBe('queued')
    expect(created.namespace).toBe('default')

    const fetched = await getRun(sql, created.id)
    expect(fetched).toBeDefined()
    expect(fetched?.id).toBe(created.id)
    expect(fetched?.input).toEqual({ hello: 'world' })
    expect(fetched?.workflow_id).toBe(workflow.id)
  })
})

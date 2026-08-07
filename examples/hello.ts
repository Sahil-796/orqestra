// A tiny defineWorkflow example. Phase 0 only covers *declaring* a
// workflow's DAG and registering it — actually running it lands in
// Phase 1's executor.

import { defineWorkflow, orquestra } from '../src/index.ts'

export const helloWorkflow = defineWorkflow('hello', (wf) => {
  wf.step('greet', async (ctx) => {
    return `hello, ${JSON.stringify(ctx.input)}`
  })

  wf.step(
    'shout',
    async () => {
      return 'HELLO!'
    },
    { dependsOn: ['greet'] }
  )
})

if (import.meta.main) {
  const orq = orquestra()
  await helloWorkflow.register(orq.db)
  console.log(`registered workflow "${helloWorkflow.name}" with steps:`, helloWorkflow.definition.steps.map((s) => s.name))
  await orq.close()
}

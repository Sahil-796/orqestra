// A small runnable durable workflow — Phase 1's executor actually runs the
// DAG (register + start + execute to completion), unlike examples/hello.ts
// which only declares one.

import { defineWorkflow, orquestra, startRun } from '../src/index.ts'

export const orderWorkflow = defineWorkflow('order-fulfillment', (wf) => {
  wf.step('validateOrder', async (ctx) => {
    const { itemCount } = ctx.input as { itemCount: number }
    if (itemCount <= 0) throw new Error('order must have at least one item')
    return { itemCount }
  })

  wf.step(
    'chargePayment',
    async (ctx) => {
      const { itemCount } = ctx.input as { itemCount: number }
      return { amount: itemCount * 25 }
    },
    { dependsOn: ['validateOrder'] }
  )

  wf.step(
    'reserveInventory',
    async (ctx) => {
      const { itemCount } = ctx.input as { itemCount: number }
      return { reserved: itemCount }
    },
    { dependsOn: ['validateOrder'] }
  )

  // fans in on both chargePayment and reserveInventory
  wf.step(
    'shipOrder',
    async () => {
      return { shippedAt: new Date().toISOString() }
    },
    { dependsOn: ['chargePayment', 'reserveInventory'] }
  )
})

if (import.meta.main) {
  const orq = orquestra()

  const result = await startRun(orq.db, orderWorkflow, {
    input: { itemCount: 3 },
    idempotencyKey: `order-demo-${new Date().toISOString().slice(0, 10)}`,
  })

  console.log(`run ${result.runId} finished with status "${result.status}"`)
  console.log('output:', JSON.stringify(result.output, null, 2))

  await orq.close()
}

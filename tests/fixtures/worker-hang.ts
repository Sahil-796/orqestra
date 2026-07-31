// Standalone fixture, run as its own `bun` subprocess by
// tests/workers-distributed.test.ts's "kill one mid-step" case. Mirrors the
// style of tests/fixtures/crash-workflow.ts, but for a *worker* rather than
// the inline executor: this process starts a real `createWorker` against a
// short lease TTL, claims the single `work` step off the queue, signals
// that it has started (a durable, count-once side effect — appendFileSync
// to ORQ_SIGNAL_FILE), and then hangs forever.
//
// The test spawns this as its own process, waits for the "started" signal,
// then sends a real `kill -9` — proving the lease is left behind on a step
// that is genuinely `running` under a genuinely dead worker, not a
// simulated failure. A separate in-process "surviving" worker (started by
// the test, registered with the SAME workflow name but a DIFFERENT `work`
// implementation that actually completes) is what proves the reclaim: it
// can only finish the run by claiming the step back after this process's
// lease expires.
//
// Required env vars: ORQ_WORKFLOW_NAME, ORQ_NAMESPACE, ORQ_SIGNAL_FILE.
// Optional: ORQ_WORKER_ID (default 'crash-worker'), ORQ_LEASE_TTL_MS
// (default 500).

import { appendFileSync } from 'node:fs'
import { defineWorkflow } from '../../src/define/workflow.ts'
import { createWorker } from '../../src/worker/worker.ts'
import { createDb } from '../../src/store/client.ts'

const workflowName = process.env.ORQ_WORKFLOW_NAME
const namespace = process.env.ORQ_NAMESPACE
const signalFile = process.env.ORQ_SIGNAL_FILE
const workerId = process.env.ORQ_WORKER_ID ?? 'crash-worker'
const leaseTtlMs = Number(process.env.ORQ_LEASE_TTL_MS ?? '500')

if (!workflowName) throw new Error('worker-hang fixture: ORQ_WORKFLOW_NAME is required')
if (!namespace) throw new Error('worker-hang fixture: ORQ_NAMESPACE is required')
if (!signalFile) throw new Error('worker-hang fixture: ORQ_SIGNAL_FILE is required')

const wf = defineWorkflow(workflowName, (builder) => {
  builder.step(
    'work',
    async () => {
      // Proof this worker actually claimed and started the step, written
      // BEFORE hanging so the test can bound its wait on this file instead
      // of an arbitrary sleep.
      appendFileSync(signalFile, 'started\n')
      // Hang forever. The test's `kill -9` is what ends this process — no
      // code below this line ever runs on this pass.
      await new Promise(() => {})
      return 'unreachable'
    },
    { maxAttempts: 5 }
  )
})

const sql = createDb()
const worker = createWorker({
  db: sql,
  handles: [wf],
  workerId,
  namespace,
  leaseTtlMs,
  pollIntervalMs: 20,
  reclaimIntervalMs: 60_000, // this process's job is to get stuck, not to reclaim anything
  concurrency: 1,
})
worker.start()

// Keep the process alive; the test kills us with SIGKILL once it observes
// the "started" signal.
await new Promise(() => {})

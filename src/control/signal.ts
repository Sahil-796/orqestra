// The public face of signals (#18): publish an event into the system and
// wake whatever is waiting for it. Deliberately thin — the storage layer
// already owns the atomicity (publishEvent records the event and wakes
// matching blocked steps in one transaction, exactly-once by the
// `status = 'blocked'` guard), so this file's only real job is the ergonomics
// and the observability the raw repository call leaves out: a `run`-scoped
// history entry for each step a publish woke.
//
// This is the entry point everything programmatic funnels through — users and
// the CLI here, Agent 2's webhook ingress, Agent 3's event triggers — so a
// signal published from any of them takes the same durable path and shows up
// the same way on a run's timeline.

import type { Db } from '../store/client.ts'
import {
  insertHistory,
  publishEvent,
  type EventRow,
  type PublishEventInput,
  type StepRow,
} from '../store/repositories.ts'

export interface PublishSignalResult {
  /** The recorded event row. */
  event: EventRow
  /** False when an idempotency key matched an existing event — nothing new happened. */
  created: boolean
  /** Every blocked step this publish moved back to `ready`. */
  woken: StepRow[]
}

/**
 * Publish an event / signal. Records it durably and wakes every `blocked`
 * step whose `waitForEvent` matches (by name, optionally narrowed by
 * `correlationKey`), delivering the payload to each. Idempotent when
 * `idempotencyKey` is supplied: a redelivery neither re-records nor re-wakes.
 *
 * Returns which steps were woken so a caller can observe the effect; a
 * `step.event_delivered` history row is written for each, so the wake is
 * legible on the run's timeline too.
 */
export async function publishSignal(db: Db, input: PublishSignalInput): Promise<PublishSignalResult> {
  const result = await publishEvent(db, input)

  for (const step of result.woken) {
    await insertHistory(db, {
      runId: step.run_id,
      stepId: step.id,
      type: 'step.event_delivered',
      data: { event: result.event.name, eventId: result.event.id },
    })
  }

  return result
}

/** Input to `publishSignal` — the same shape as the repository's `publishEvent`. */
export type PublishSignalInput = PublishEventInput

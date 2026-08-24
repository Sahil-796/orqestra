// Event-trigger routing (#22): given claimed undispatched events, find every
// workflow declared with a matching `{type:'event'}` trigger (see
// WorkflowHandle.triggers, define/workflow.ts) and start a run of it.
//
// `claimUndispatchedEvents` (repositories.ts) does the at-most-once claim —
// it atomically stamps `dispatched_at` under FOR UPDATE SKIP LOCKED, so two
// concurrent poll ticks never both route the same event row. What it does
// NOT protect against is routing the same already-claimed event to the same
// workflow twice if this module's own run-starting step is retried (e.g. a
// crash between claiming the event and starting every matching run) — that's
// what the per-(event, workflow) `idempotencyKey` below covers, via
// `createRun`'s existing idempotency-key dedup.

import type { Db } from '../store/client.ts'
import type { WorkflowHandle } from '../define/workflow.ts'
import { claimUndispatchedEvents, resetEventDispatch, startRunForWorkflowName, type EventRow } from '../store/repositories.ts'
import type { WorkflowTrigger } from '../types.ts'

type EventTrigger = Extract<WorkflowTrigger, { type: 'event' }>

// Same matching rule as `ctx.waitForEvent` (types.ts's WaitForEventOptions
// doc): the trigger's event name must match, and if the trigger specifies a
// `correlationKey` the event's `correlation_key` must equal it exactly — a
// trigger with no `correlationKey` is broad and matches any event of that
// name regardless of the event's own correlation.
function matchesEventTrigger(trigger: WorkflowTrigger, event: EventRow): trigger is EventTrigger {
  if (trigger.type !== 'event') return false
  if (trigger.event !== event.name) return false
  if (trigger.correlationKey !== undefined && trigger.correlationKey !== event.correlation_key) {
    return false
  }
  return true
}

/** Deterministic per-(event, workflow) key so a re-dispatch never double-starts. */
export function eventTriggerIdempotencyKey(eventId: string, workflowName: string): string {
  return `event-trigger:${eventId}:${workflowName}`
}

export interface EventTriggerError {
  event: EventRow
  workflowName: string
  error: unknown
}

export interface PollEventsResult {
  /** Undispatched events claimed this tick. */
  claimed: number
  /** How many (event, workflow) matches successfully started a run. */
  started: number
  errors: EventTriggerError[]
}

export interface PollEventsOptions {
  limit?: number
}

/**
 * One poll tick: claim undispatched events and start a run for every
 * workflow whose `{type:'event'}` trigger matches. Safe to call repeatedly
 * on an interval.
 */
export async function pollUndispatchedEvents(
  sql: Db,
  workflows: readonly WorkflowHandle[],
  opts: PollEventsOptions = {}
): Promise<PollEventsResult> {
  const events = await claimUndispatchedEvents(sql, opts.limit)
  const result: PollEventsResult = { claimed: events.length, started: 0, errors: [] }

  for (const event of events) {
    let eventFailed = false
    for (const workflow of workflows) {
      const matched = workflow.triggers.some((trigger) => matchesEventTrigger(trigger, event))
      if (!matched) continue

      try {
        await startRunForWorkflowName(sql, {
          workflowName: workflow.name,
          input: event.payload,
          idempotencyKey: eventTriggerIdempotencyKey(event.id, workflow.name),
        })
        result.started++
      } catch (error) {
        eventFailed = true
        result.errors.push({ event, workflowName: workflow.name, error })
      }
    }
    // A claimed event whose routing partially failed must be retried, not lost:
    // un-stamp its dispatch so the next tick re-claims it (idempotency keys stop
    // already-started workflows from double-starting).
    if (eventFailed) await resetEventDispatch(sql, event.id)
  }

  return result
}

// The HTTP "ways in" for Phase 5: API-triggered starts (#21), the request
// side of delayed starts (#24), webhook ingestion (#25), and a direct signal
// publish. src/server.ts is deliberately thin — it just wires host/port from
// config and hands every request to `handleRequest` here, so the routing
// logic is testable by calling it directly with a `Request`, no socket bound.
//
// No auth in this phase (TODO — noted, not solved: any caller can start a
// run, publish a signal, or feed a webhook). The webhook body is treated as
// untrusted data throughout: it's read as JSON defensively and never trusted
// beyond the fields webhook.ts's mapping explicitly reads.

import type { Db } from '../store/client.ts'
import { publishSignal, type PublishSignalResult } from '../control/signal.ts'
import { scheduleRun, startRun } from '../control/start.ts'
import { mapWebhookToEvent } from './webhook.ts'
import type { ScheduleRow, StartRunByNameResult } from '../store/repositories.ts'

export interface TriggerServerDeps {
  db: Db
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  })
}

function badRequest(message: string): Response {
  return json({ error: message }, 400)
}

function notFound(): Response {
  return json({ error: 'not found' }, 404)
}

function methodNotAllowed(): Response {
  return json({ error: 'method not allowed' }, 405)
}

// Bodies are optional on these routes (an empty POST is valid — e.g. "start
// this workflow with no input"), but a body that IS present and isn't valid
// JSON is a 400, not a silent `undefined`.
async function parseJsonBody(req: Request): Promise<{ ok: true; body: unknown } | { ok: false; error: string }> {
  const raw = await req.text()
  if (raw.trim().length === 0) return { ok: true, body: undefined }
  try {
    return { ok: true, body: JSON.parse(raw) }
  } catch {
    return { ok: false, error: 'request body is not valid JSON' }
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function stringOrUndefined(value: unknown): string | undefined {
  return typeof value === 'string' && value.length > 0 ? value : undefined
}

function numberOrUndefined(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isFinite(value) ? value : undefined
}

// A future start time, from either an absolute `runAt` (ISO 8601 / anything
// `Date` parses) or a relative `delayMs`. `delayMs` wins if both are present
// — it's unambiguous relative to "now", whereas a malformed `runAt` string
// silently becomes `Invalid Date` and must be rejected rather than guessed
// at. Returns undefined for "not a delayed start" (start now), and throws a
// string message for "asked for a delay but it's malformed" so the caller can
// turn that into a 400.
function resolveDelayedStart(body: Record<string, unknown>): Date | undefined {
  const delayMs = numberOrUndefined(body.delayMs)
  if (delayMs !== undefined) {
    if (delayMs <= 0) throw new Error('delayMs must be a positive number')
    return new Date(Date.now() + delayMs)
  }

  const runAtRaw = body.runAt
  if (runAtRaw === undefined || runAtRaw === null) return undefined
  if (typeof runAtRaw !== 'string' && typeof runAtRaw !== 'number') {
    throw new Error('runAt must be a string or number')
  }
  const runAt = new Date(runAtRaw)
  if (Number.isNaN(runAt.getTime())) throw new Error('runAt is not a valid date')
  return runAt
}

function idempotencyKeyFrom(req: Request, body: Record<string, unknown>): string | undefined {
  return stringOrUndefined(req.headers.get('idempotency-key') ?? undefined) ?? stringOrUndefined(body.idempotencyKey)
}

// decodeURIComponent throws URIError on a malformed %-escape; a bad path
// segment is a client error (400), not a server crash (500).
function safeDecodeSegment(raw: string): string | undefined {
  try {
    return decodeURIComponent(raw)
  } catch {
    return undefined
  }
}

// ---- POST /workflows/:name/runs (and its alias POST /runs) ----------------
//
// #21 API trigger + #24 delayed-start request. A future `runAt`/`delayMs`
// diverts to a `once` schedule row instead of starting a run immediately —
// Agent 3's poller is what actually promotes it later.

// Shared core of both `POST /workflows/:name/runs` and `POST /runs` — the
// only difference between those two routes is where `workflowName` comes
// from (the path vs. a body field), so both parse their own request and then
// hand off here rather than duplicating the start-vs-schedule decision.
async function startRunCore(req: Request, db: Db, workflowName: string, body: Record<string, unknown>): Promise<Response> {
  if (workflowName.length === 0) {
    return badRequest('workflow name is required')
  }

  const version = body.version === undefined ? undefined : numberOrUndefined(body.version)
  if (body.version !== undefined && version === undefined) {
    return badRequest('version must be a number')
  }

  const namespace = stringOrUndefined(body.namespace)
  const priority = numberOrUndefined(body.priority)
  const idempotencyKey = idempotencyKeyFrom(req, body)

  let runAt: Date | undefined
  try {
    runAt = resolveDelayedStart(body)
  } catch (e) {
    return badRequest(e instanceof Error ? e.message : 'invalid delayed-start fields')
  }

  try {
    if (runAt) {
      const schedule: ScheduleRow = await scheduleRun(db, {
        workflowName,
        runAt,
        input: body.input,
        namespace,
        priority,
      })
      return json(
        {
          scheduleId: schedule.id,
          workflowName: schedule.workflow_name,
          nextRunAt: schedule.next_run_at,
          kind: schedule.kind,
        },
        202
      )
    }

    const result: StartRunByNameResult = await startRun(db, {
      workflowName,
      version,
      input: body.input,
      namespace,
      priority,
      idempotencyKey,
    })
    return json(
      { runId: result.runId, workflowId: result.workflowId, created: result.created },
      result.created ? 201 : 200
    )
  } catch (e) {
    // The only expected failure here is "workflow not registered" — anything
    // storage-side (a real DB outage, a constraint violation) is a 500.
    const message = e instanceof Error ? e.message : String(e)
    if (message.includes('no workflow registered under name')) return badRequest(message)
    return json({ error: message }, 500)
  }
}

// ---- POST /webhooks/:name --------------------------------------------------
//
// #25 webhook ingestion. Durably records an event (publishSignal) so any step
// blocked on `ctx.waitForEvent(name)` wakes. Returns 2xx only once the event
// is actually persisted — never before.

async function handleWebhook(req: Request, db: Db, routeName: string): Promise<Response> {
  const parsed = await parseJsonBody(req)
  if (!parsed.ok) return badRequest(parsed.error)

  if (typeof routeName !== 'string' || routeName.length === 0) {
    return badRequest('webhook name is required')
  }

  const headers: Record<string, string | undefined> = {}
  req.headers.forEach((value, key) => {
    headers[key] = value
  })

  let mapped
  try {
    mapped = mapWebhookToEvent({ routeName, body: parsed.body, headers })
  } catch (e) {
    return badRequest(e instanceof Error ? e.message : 'could not map webhook to an event')
  }

  const result: PublishSignalResult = await publishSignal(db, {
    name: mapped.name,
    correlationKey: mapped.correlationKey,
    payload: mapped.payload,
    source: mapped.source,
    idempotencyKey: mapped.idempotencyKey,
  })

  return json(
    {
      eventId: result.event.id,
      name: result.event.name,
      created: result.created,
      woken: result.woken.length,
    },
    result.created ? 201 : 200
  )
}

// ---- POST /signals ----------------------------------------------------------
//
// A direct signal publish, for callers that aren't modeling their integration
// as "a webhook" — a thin wrapper over the same `publishSignal` path.

async function handleSignal(req: Request, db: Db): Promise<Response> {
  const parsed = await parseJsonBody(req)
  if (!parsed.ok) return badRequest(parsed.error)

  const body = parsed.body
  if (!isRecord(body)) return badRequest('request body must be a JSON object')

  const name = stringOrUndefined(body.name)
  if (!name) return badRequest('"name" is required')

  const correlationKey = stringOrUndefined(body.correlationKey)
  const source = stringOrUndefined(body.source)
  const idempotencyKey = idempotencyKeyFrom(req, body)

  const result: PublishSignalResult = await publishSignal(db, {
    name,
    correlationKey,
    payload: body.payload,
    source,
    idempotencyKey,
  })

  return json(
    {
      eventId: result.event.id,
      name: result.event.name,
      created: result.created,
      woken: result.woken.length,
    },
    result.created ? 201 : 200
  )
}

// ---- routing ----------------------------------------------------------------

// Matches `/workflows/:name/runs` — used by the primary API-trigger route.
const WORKFLOW_RUNS_RE = /^\/workflows\/([^/]+)\/runs\/?$/
// Matches `/webhooks/:name`.
const WEBHOOK_RE = /^\/webhooks\/([^/]+)\/?$/

export function createTriggerHandler(deps: TriggerServerDeps): (req: Request) => Promise<Response> {
  const { db } = deps

  return async function handleRequest(req: Request): Promise<Response> {
    const url = new URL(req.url)
    const path = url.pathname

    if (path === '/healthz') {
      if (req.method !== 'GET') return methodNotAllowed()
      return json({ ok: true })
    }

    if (path === '/runs') {
      if (req.method !== 'POST') return methodNotAllowed()
      const parsed = await parseJsonBody(req)
      if (!parsed.ok) return badRequest(parsed.error)
      const body = parsed.body
      if (!isRecord(body)) return badRequest('request body must be a JSON object')
      const workflowName = stringOrUndefined(body.workflowName) ?? stringOrUndefined(body.workflow)
      if (!workflowName) return badRequest('"workflowName" is required')
      return startRunCore(req, db, workflowName, body)
    }

    const runsMatch = WORKFLOW_RUNS_RE.exec(path)
    if (runsMatch) {
      if (req.method !== 'POST') return methodNotAllowed()
      const workflowName = safeDecodeSegment(runsMatch[1] ?? '')
      if (workflowName === undefined || workflowName.length === 0) return badRequest('workflow name is required')
      const parsed = await parseJsonBody(req)
      if (!parsed.ok) return badRequest(parsed.error)
      const body = parsed.body === undefined ? {} : parsed.body
      if (!isRecord(body)) return badRequest('request body must be a JSON object')
      return startRunCore(req, db, workflowName, body)
    }

    const webhookMatch = WEBHOOK_RE.exec(path)
    if (webhookMatch) {
      if (req.method !== 'POST') return methodNotAllowed()
      const routeName = safeDecodeSegment(webhookMatch[1] ?? '')
      if (routeName === undefined || routeName.length === 0) return badRequest('webhook name is required')
      return handleWebhook(req, db, routeName)
    }

    if (path === '/signals') {
      if (req.method !== 'POST') return methodNotAllowed()
      return handleSignal(req, db)
    }

    return notFound()
  }
}

// Phase 8 dashboard read API (#30-#35) plus the operator retry (#27) and
// cancel (#11) controls, all under the `/dashboard/api` prefix so nothing
// collides with the trigger routes (src/triggers/http.ts) or the existing
// `/dead-letter` operator routes in src/server.ts.
//
// This file only ever talks to storage through src/store/repositories.ts and
// to control policy through src/control/{retry,cancel}.ts — it never imports
// `postgres` or writes SQL itself (the storage boundary CLAUDE.md describes).
//
// `createDashboardApiHandler` returns `undefined` for any path outside
// `/dashboard/api`, so server.ts can try it first and fall through to the
// trigger handler (and the static UI handler) for everything else — same
// composition style as the existing operator routes.

import type { Db } from '../store/client.ts'
import { cancelRun } from '../control/cancel.ts'
import { retryDeadLetterRun } from '../control/retry.ts'
import {
  getQueueDepth,
  getRun,
  getRunErrors,
  getRunLogs,
  getRunMetrics,
  getRunTimeline,
  getStepsByRun,
  getThroughput,
  listRuns,
  listWorkerHealth,
  type RunListFilter,
  type RunMetricsFilter,
} from '../store/repositories.ts'
import { deserializeError, RUN_STATUSES, type RunStatus, type SerializedError } from '../types.ts'

export interface DashboardApiDeps {
  db: Db
}

// ---- response helpers -------------------------------------------------
//
// Kept local (not imported from server.ts) so this module has no dependency
// on server.ts and matches its JSON/error shape exactly, mirroring the
// duplication server.ts already has against triggers/http.ts.

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  })
}

function badRequest(message: string): Response {
  return json({ error: message }, 400)
}

function notFound(message: string): Response {
  return json({ error: message }, 404)
}

function methodNotAllowed(): Response {
  return json({ error: 'method not allowed' }, 405)
}

function safeDecodeSegment(raw: string): string | undefined {
  try {
    return decodeURIComponent(raw)
  } catch {
    return undefined
  }
}

// ---- query-param parsing ------------------------------------------------

// Positive-integer query param, mirroring server.ts's existing `?limit=`
// handling: absent is fine (caller uses the repository default), present but
// not a positive integer is a 400.
function parsePositiveInt(
  url: URL,
  name: string
): { ok: true; value: number | undefined } | { ok: false; error: string } {
  const raw = url.searchParams.get(name)
  if (raw === null) return { ok: true, value: undefined }
  const parsed = Number(raw)
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return { ok: false, error: `${name} must be a positive integer` }
  }
  return { ok: true, value: parsed }
}

// Same shape, but 0 is allowed (offset, bucketMs-style windows).
function parseNonNegativeInt(
  url: URL,
  name: string
): { ok: true; value: number | undefined } | { ok: false; error: string } {
  const raw = url.searchParams.get(name)
  if (raw === null) return { ok: true, value: undefined }
  const parsed = Number(raw)
  if (!Number.isInteger(parsed) || parsed < 0) {
    return { ok: false, error: `${name} must be a non-negative integer` }
  }
  return { ok: true, value: parsed }
}

function isRunStatus(value: string): value is RunStatus {
  return (RUN_STATUSES as readonly string[]).includes(value)
}

// ---- route table ----------------------------------------------------------

const RUN_DETAIL_RE = /^\/dashboard\/api\/runs\/([^/]+)\/?$/
const RUN_LOGS_RE = /^\/dashboard\/api\/runs\/([^/]+)\/logs\/?$/
const RUN_RETRY_RE = /^\/dashboard\/api\/runs\/([^/]+)\/retry\/?$/
const RUN_CANCEL_RE = /^\/dashboard\/api\/runs\/([^/]+)\/cancel\/?$/

// ---- GET /dashboard/api/runs -----------------------------------------------
//
// #30 run listing. `?status=&workflow=&namespace=&limit=&offset=` all
// optional; a bad `status` (not one of RunStatus's values) or a non-positive
// `limit`/negative `offset` is a 400.
async function handleListRuns(db: Db, url: URL): Promise<Response> {
  const statusRaw = url.searchParams.get('status')
  if (statusRaw !== null && !isRunStatus(statusRaw)) {
    return badRequest(`status must be one of: ${RUN_STATUSES.join(', ')}`)
  }

  const limit = parsePositiveInt(url, 'limit')
  if (!limit.ok) return badRequest(limit.error)
  const offset = parseNonNegativeInt(url, 'offset')
  if (!offset.ok) return badRequest(offset.error)

  const filter: RunListFilter = {}
  if (statusRaw !== null) filter.status = statusRaw
  const workflowName = url.searchParams.get('workflow')
  if (workflowName !== null) filter.workflowName = workflowName
  const namespace = url.searchParams.get('namespace')
  if (namespace !== null) filter.namespace = namespace
  if (limit.value !== undefined) filter.limit = limit.value
  if (offset.value !== undefined) filter.offset = offset.value

  const runs = await listRuns(db, filter)
  return json({ runs, count: runs.length })
}

// ---- GET /dashboard/api/runs/:id -------------------------------------------
//
// #30 (timeline) + #33 (errors), composed with the run + its steps into one
// detail payload. 404 when the run doesn't exist.
async function handleGetRunDetail(db: Db, runId: string): Promise<Response> {
  const run = await getRun(db, runId)
  if (!run) return notFound('no run found for that id')

  const [steps, timeline, errorSteps] = await Promise.all([
    getStepsByRun(db, runId),
    getRunTimeline(db, runId),
    getRunErrors(db, runId),
  ])

  const errors = errorSteps.map((step) => {
    // deserializeError yields a real `Error` instance so callers get
    // `instanceof Error` semantics, but Error's message/stack aren't
    // enumerable own properties — JSON.stringify(new Error(...)) drops them
    // (see types.ts's codec comment). Project the fields the UI needs into a
    // plain object explicitly rather than serializing the Error itself.
    const error = step.error ? deserializeError(step.error as SerializedError) : null
    return {
      stepId: step.id,
      stepName: step.name,
      attempt: step.attempt,
      updatedAt: step.updated_at,
      error: error ? { name: error.name, message: error.message, stack: error.stack } : null,
    }
  })

  return json({ run, steps, timeline, errors })
}

// ---- GET /dashboard/api/runs/:id/logs --------------------------------------
//
// #31 structured logs — `history` rows of type `log` for the run.
async function handleGetRunLogs(db: Db, runId: string): Promise<Response> {
  const run = await getRun(db, runId)
  if (!run) return notFound('no run found for that id')

  const logs = await getRunLogs(db, runId)
  return json({ runId, logs })
}

// ---- GET /dashboard/api/metrics ---------------------------------------------
//
// #32 duration metrics. `?workflow=&namespace=&sinceMs=&groupByWorkflow=`.
async function handleGetMetrics(db: Db, url: URL): Promise<Response> {
  const sinceMs = parsePositiveInt(url, 'sinceMs')
  if (!sinceMs.ok) return badRequest(sinceMs.error)

  const groupByRaw = url.searchParams.get('groupByWorkflow')
  if (groupByRaw !== null && groupByRaw !== 'true' && groupByRaw !== 'false') {
    return badRequest('groupByWorkflow must be "true" or "false"')
  }

  const filter: RunMetricsFilter = {}
  const workflowName = url.searchParams.get('workflow')
  if (workflowName !== null) filter.workflowName = workflowName
  const namespace = url.searchParams.get('namespace')
  if (namespace !== null) filter.namespace = namespace
  if (sinceMs.value !== undefined) filter.sinceMs = sinceMs.value
  if (groupByRaw === 'true') filter.groupByWorkflow = true

  const metrics = await getRunMetrics(db, filter)
  return json({ metrics })
}

// ---- GET /dashboard/api/queue -----------------------------------------------
//
// #35 queue depth + throughput. Default window is the last 60 minutes;
// `?sinceMs=&bucketMs=` override it.
const DEFAULT_QUEUE_SINCE_MS = 60 * 60 * 1000

async function handleGetQueue(db: Db, url: URL): Promise<Response> {
  const sinceMs = parsePositiveInt(url, 'sinceMs')
  if (!sinceMs.ok) return badRequest(sinceMs.error)
  const bucketMs = parsePositiveInt(url, 'bucketMs')
  if (!bucketMs.ok) return badRequest(bucketMs.error)

  const [depth, throughput] = await Promise.all([
    getQueueDepth(db),
    getThroughput(db, sinceMs.value ?? DEFAULT_QUEUE_SINCE_MS, bucketMs.value),
  ])

  return json({ depth, throughput })
}

// ---- GET /dashboard/api/workers ---------------------------------------------
//
// #34 worker health. `?staleAfterMs=` overrides the repository's default.
async function handleGetWorkers(db: Db, url: URL): Promise<Response> {
  const staleAfterMs = parsePositiveInt(url, 'staleAfterMs')
  if (!staleAfterMs.ok) return badRequest(staleAfterMs.error)

  const workers = await listWorkerHealth(db, staleAfterMs.value)
  return json({ workers })
}

// ---- POST /dashboard/api/runs/:id/retry ------------------------------------
//
// #27 manual retry — shares retryDeadLetterRun with the existing
// `/dead-letter/:id/retry` operator route; 404 when the run isn't parked in
// dead_letter (mirrors handleRetryDeadLetter in server.ts).
async function handleRetryRun(db: Db, runId: string): Promise<Response> {
  const result = await retryDeadLetterRun(db, runId)
  if (!result.retried || !result.run) {
    return notFound('no dead-lettered run to retry for that id')
  }
  return json({ runId: result.run.id, status: result.run.status, retried: true })
}

// ---- POST /dashboard/api/runs/:id/cancel -----------------------------------
//
// #11 cancel. 404 when there's no such run; otherwise reports whatever
// cancelRun's policy decided (request recorded now, finalized synchronously,
// or pending a running step's worker to finalize it).
async function handleCancelRun(db: Db, runId: string): Promise<Response> {
  const existing = await getRun(db, runId)
  if (!existing) return notFound('no run found for that id')

  const result = await cancelRun(db, runId)
  return json({
    runId,
    cancelled: result.finalized,
    requested: result.requested,
    pending: result.pending,
    status: result.run?.status ?? existing.status,
  })
}

/**
 * Handle a `/dashboard/api/*` request, or return `undefined` for any other
 * path so the caller (src/server.ts) can fall through to its other handlers.
 */
export function createDashboardApiHandler(
  deps: DashboardApiDeps
): (req: Request) => Promise<Response | undefined> {
  const { db } = deps

  return async function handleDashboardApiRequest(req: Request): Promise<Response | undefined> {
    const url = new URL(req.url)
    const path = url.pathname

    if (path === '/dashboard/api/runs') {
      if (req.method !== 'GET') return methodNotAllowed()
      return handleListRuns(db, url)
    }

    if (path === '/dashboard/api/metrics') {
      if (req.method !== 'GET') return methodNotAllowed()
      return handleGetMetrics(db, url)
    }

    if (path === '/dashboard/api/queue') {
      if (req.method !== 'GET') return methodNotAllowed()
      return handleGetQueue(db, url)
    }

    if (path === '/dashboard/api/workers') {
      if (req.method !== 'GET') return methodNotAllowed()
      return handleGetWorkers(db, url)
    }

    const retryMatch = RUN_RETRY_RE.exec(path)
    if (retryMatch) {
      if (req.method !== 'POST') return methodNotAllowed()
      const runId = safeDecodeSegment(retryMatch[1] ?? '')
      if (runId === undefined || runId.length === 0) return badRequest('run id is required')
      return handleRetryRun(db, runId)
    }

    const cancelMatch = RUN_CANCEL_RE.exec(path)
    if (cancelMatch) {
      if (req.method !== 'POST') return methodNotAllowed()
      const runId = safeDecodeSegment(cancelMatch[1] ?? '')
      if (runId === undefined || runId.length === 0) return badRequest('run id is required')
      return handleCancelRun(db, runId)
    }

    const logsMatch = RUN_LOGS_RE.exec(path)
    if (logsMatch) {
      if (req.method !== 'GET') return methodNotAllowed()
      const runId = safeDecodeSegment(logsMatch[1] ?? '')
      if (runId === undefined || runId.length === 0) return badRequest('run id is required')
      return handleGetRunLogs(db, runId)
    }

    const detailMatch = RUN_DETAIL_RE.exec(path)
    if (detailMatch) {
      if (req.method !== 'GET') return methodNotAllowed()
      const runId = safeDecodeSegment(detailMatch[1] ?? '')
      if (runId === undefined || runId.length === 0) return badRequest('run id is required')
      return handleGetRunDetail(db, runId)
    }

    return undefined
  }
}

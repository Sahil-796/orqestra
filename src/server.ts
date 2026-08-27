// Thin Bun.serve wiring for Phase 5's HTTP ingress plus Phase 7's operator
// surface. The trigger routes (starts, webhooks, signals) live in
// src/triggers/http.ts (createTriggerHandler); the operator routes for manual
// retry (#27) are added here and everything else falls through to the trigger
// handler. Both layers stay testable by calling their handler directly with a
// Request, without binding a socket — this file's only extra job is reading
// host/port from config/env and starting the server.

import { loadConfig } from './config.ts'
import { getDb } from './store/client.ts'
import type { Db } from './store/client.ts'
import { createTriggerHandler } from './triggers/http.ts'
import { listDeadLetteredRuns, retryDeadLetterRun } from './control/retry.ts'
import type { RunRow } from './store/repositories.ts'

export interface StartServerOptions {
  port?: number
  hostname?: string
}

// Same response helpers as src/triggers/http.ts — kept local so the operator
// routes match the trigger routes' JSON/error shape exactly.
function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  })
}

function badRequest(message: string): Response {
  return json({ error: message }, 400)
}

function methodNotAllowed(): Response {
  return json({ error: 'method not allowed' }, 405)
}

// decodeURIComponent throws URIError on a malformed %-escape; a bad path
// segment is a client error (400), not a server crash (500) — mirrors
// http.ts's safeDecodeSegment.
function safeDecodeSegment(raw: string): string | undefined {
  try {
    return decodeURIComponent(raw)
  } catch {
    return undefined
  }
}

// The operator-facing projection of a run — enough to triage a dead-letter
// queue without shipping every column across the wire.
function deadLetterView(run: RunRow) {
  return {
    runId: run.id,
    workflowId: run.workflow_id,
    namespace: run.namespace,
    status: run.status,
    deadLetteredAt: run.dead_lettered_at,
    deadLetterReason: run.dead_letter_reason,
    createdAt: run.created_at,
  }
}

// Matches `/dead-letter/:id/retry`.
const DEAD_LETTER_RETRY_RE = /^\/dead-letter\/([^/]+)\/retry\/?$/

// ---- GET /dead-letter ------------------------------------------------------
//
// #27 operator triage: list every run currently parked in dead_letter. An
// optional `?limit=` caps the page (bad/absent value falls back to the
// repository default).
async function handleListDeadLetter(req: Request, db: Db, url: URL): Promise<Response> {
  const limitRaw = url.searchParams.get('limit')
  let limit: number | undefined
  if (limitRaw !== null) {
    const parsed = Number(limitRaw)
    if (!Number.isInteger(parsed) || parsed <= 0) return badRequest('limit must be a positive integer')
    limit = parsed
  }

  const runs = await listDeadLetteredRuns(db, limit)
  return json({ runs: runs.map(deadLetterView), count: runs.length })
}

// ---- POST /dead-letter/:id/retry -------------------------------------------
//
// #27 operator manual retry: reset a dead-lettered run back to `queued` so a
// worker re-runs it. A run that isn't actually parked in dead_letter (no such
// run, or any other status) is a 404 — there is nothing to retry.
async function handleRetryDeadLetter(db: Db, runId: string): Promise<Response> {
  const result = await retryDeadLetterRun(db, runId)
  if (!result.retried || !result.run) {
    return json({ error: 'no dead-lettered run to retry for that id' }, 404)
  }
  return json({ runId: result.run.id, status: result.run.status, retried: true })
}

// The composed handler: operator routes first, then fall through to the trigger
// handler for starts/webhooks/signals/healthz.
export function createServerHandler(deps: { db: Db }): (req: Request) => Promise<Response> {
  const { db } = deps
  const triggerHandler = createTriggerHandler({ db })

  return async function handleRequest(req: Request): Promise<Response> {
    const url = new URL(req.url)
    const path = url.pathname

    if (path === '/dead-letter') {
      if (req.method !== 'GET') return methodNotAllowed()
      return handleListDeadLetter(req, db, url)
    }

    const retryMatch = DEAD_LETTER_RETRY_RE.exec(path)
    if (retryMatch) {
      if (req.method !== 'POST') return methodNotAllowed()
      const runId = safeDecodeSegment(retryMatch[1] ?? '')
      if (runId === undefined || runId.length === 0) return badRequest('run id is required')
      return handleRetryDeadLetter(db, runId)
    }

    return triggerHandler(req)
  }
}

/**
 * Start the HTTP server. Host/port come from config
 * (`ORQ_HTTP_HOST` / `ORQ_HTTP_PORT`, defaulting to 0.0.0.0:3000) and can be
 * overridden per-call via options.
 */
export function startServer(options: StartServerOptions = {}) {
  // Load config first so bad env fails fast before we accept any traffic, and
  // so host/port follow the same fail-fast parsing as the DB/worker knobs.
  const config = loadConfig()
  const db = getDb()
  const handler = createServerHandler({ db })

  const port = options.port ?? config.httpPort
  const hostname = options.hostname ?? config.httpHost

  return Bun.serve({
    port,
    hostname,
    fetch: handler,
  })
}

if (import.meta.main) {
  const server = startServer()
  // eslint-disable-next-line no-console
  console.log(`orqestra server listening on http://${server.hostname}:${server.port}`)
}

// Thin Bun.serve wiring for Phase 5's HTTP ingress. All routing/handling logic
// lives in src/triggers/http.ts (createTriggerHandler) so it stays testable
// by calling the handler directly with a Request, without binding a socket.
// This file's only job: read host/port from config/env and start the server.

import { loadConfig } from './config.ts'
import { getDb } from './store/client.ts'
import { createTriggerHandler } from './triggers/http.ts'

export interface StartServerOptions {
  port?: number
  hostname?: string
}

/**
 * Start the HTTP trigger server. Defaults the port to `PORT` (falling back to
 * 3000) — there's no dedicated ORQ_* server env var yet, since config.ts owns
 * the DB/worker knobs and this is a new, separate concern.
 */
export function startServer(options: StartServerOptions = {}) {
  // Loading config here (even though only databaseUrl is used indirectly via
  // getDb()) keeps this file consistent with the rest of src/: fail fast on
  // bad env before accepting any traffic.
  loadConfig()
  const db = getDb()
  const handler = createTriggerHandler({ db })

  const port = options.port ?? (process.env.PORT ? Number(process.env.PORT) : 3000)
  const hostname = options.hostname ?? process.env.HOST ?? '0.0.0.0'

  return Bun.serve({
    port,
    hostname,
    fetch: handler,
  })
}

if (import.meta.main) {
  const server = startServer()
  // eslint-disable-next-line no-console
  console.log(`orqestra trigger server listening on http://${server.hostname}:${server.port}`)
}

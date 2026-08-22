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
 * Start the HTTP trigger server. Host/port come from config
 * (`ORQ_HTTP_HOST` / `ORQ_HTTP_PORT`, defaulting to 0.0.0.0:3000) and can be
 * overridden per-call via options.
 */
export function startServer(options: StartServerOptions = {}) {
  // Load config first so bad env fails fast before we accept any traffic, and
  // so host/port follow the same fail-fast parsing as the DB/worker knobs.
  const config = loadConfig()
  const db = getDb()
  const handler = createTriggerHandler({ db })

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
  console.log(`orqestra trigger server listening on http://${server.hostname}:${server.port}`)
}

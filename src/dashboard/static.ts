// Static file serving for the dashboard UI. `GET /dashboard` serves
// `src/dashboard/ui/index.html`; `GET /dashboard/ui/*` serves any other file
// under `src/dashboard/ui/`. That directory belongs to a separate unit (the
// UI build) and may not exist yet, or may be empty — both are fine here,
// they just mean every request 404s until the UI lands.
//
// This module never imports `postgres` or touches storage; it's pure file
// serving, guarded against path traversal.

import { dirname, join, normalize, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const HERE = dirname(fileURLToPath(import.meta.url))
const UI_ROOT = resolve(HERE, 'ui')
const INDEX_FILE = join(UI_ROOT, 'index.html')

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  })
}

function notFound(): Response {
  return json({ error: 'not found' }, 404)
}

function methodNotAllowed(): Response {
  return json({ error: 'method not allowed' }, 405)
}

const CONTENT_TYPES: Record<string, string> = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
  '.map': 'application/json; charset=utf-8',
}

function contentTypeFor(path: string): string {
  const dot = path.lastIndexOf('.')
  if (dot === -1) return 'application/octet-stream'
  return CONTENT_TYPES[path.slice(dot)] ?? 'application/octet-stream'
}

async function serveFile(absolutePath: string): Promise<Response> {
  const file = Bun.file(absolutePath)
  if (!(await file.exists())) return notFound()
  return new Response(file, {
    status: 200,
    headers: { 'content-type': contentTypeFor(absolutePath) },
  })
}

/**
 * Handle a `GET /dashboard` or `GET /dashboard/ui/*` request, or return
 * `undefined` for any other path so the caller (src/server.ts) can fall
 * through to its other handlers.
 */
export function createDashboardStaticHandler(): (req: Request) => Promise<Response | undefined> {
  return async function handleDashboardStaticRequest(req: Request): Promise<Response | undefined> {
    const url = new URL(req.url)
    const path = url.pathname

    if (path === '/dashboard' || path === '/dashboard/') {
      if (req.method !== 'GET') return methodNotAllowed()
      return serveFile(INDEX_FILE)
    }

    if (path === '/dashboard/ui' || path === '/dashboard/ui/') {
      if (req.method !== 'GET') return methodNotAllowed()
      return serveFile(INDEX_FILE)
    }

    if (path.startsWith('/dashboard/ui/')) {
      if (req.method !== 'GET') return methodNotAllowed()
      const relative = path.slice('/dashboard/ui/'.length)
      // normalize() collapses `..` segments; if the result still tries to
      // climb out of UI_ROOT (or is absolute), it's a traversal attempt — 404
      // it the same as a genuinely missing file rather than leaking a stat.
      const normalized = normalize(relative)
      if (normalized.startsWith('..') || normalized.startsWith('/')) return notFound()
      const absolutePath = join(UI_ROOT, normalized)
      if (!absolutePath.startsWith(UI_ROOT)) return notFound()
      return serveFile(absolutePath)
    }

    return undefined
  }
}

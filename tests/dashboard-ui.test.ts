// Phase 8 dashboard UI (#30-#35 surface) — a light serving test only. The UI
// logic itself is browser JS and isn't exercised here; this just proves
// src/dashboard/static.ts actually serves the files this unit built, with
// the right content types, and that index.html references the right assets.
// Same handler-calling pattern as tests/dashboard-api.test.ts /
// tests/manual-retry.test.ts.

import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import { createServerHandler } from '../src/server.ts'

const sql = createDb()
const handler = createServerHandler({ db: sql })

beforeAll(async () => {
  await migrate(sql)
})

afterAll(async () => {
  await sql.end()
})

function req(path: string): Request {
  return new Request(`http://localhost${path}`)
}

describe('dashboard UI static serving', () => {
  test('GET /dashboard serves index.html referencing the UI assets', async () => {
    const res = await handler(req('/dashboard'))
    expect(res.status).toBe(200)
    expect(res.headers.get('content-type')).toContain('text/html')
    const body = await res.text()
    expect(body).toContain('ORQESTRA_DASHBOARD_ROOT')
    expect(body).toContain('/dashboard/ui/app.js')
    expect(body).toContain('/dashboard/ui/styles.css')
  })

  test('GET /dashboard/ui/app.js serves the app script', async () => {
    const res = await handler(req('/dashboard/ui/app.js'))
    expect(res.status).toBe(200)
    expect(res.headers.get('content-type')).toContain('text/javascript')
    const body = await res.text()
    expect(body).toContain("import { api } from './api.js'")
  })

  test('GET /dashboard/ui/styles.css serves the stylesheet', async () => {
    const res = await handler(req('/dashboard/ui/styles.css'))
    expect(res.status).toBe(200)
    expect(res.headers.get('content-type')).toContain('text/css')
    const body = await res.text()
    expect(body).toContain('.topbar')
  })

  test('GET /dashboard/ui/api.js and format.js serve as JS modules', async () => {
    const apiRes = await handler(req('/dashboard/ui/api.js'))
    expect(apiRes.status).toBe(200)
    expect(apiRes.headers.get('content-type')).toContain('text/javascript')

    const formatRes = await handler(req('/dashboard/ui/format.js'))
    expect(formatRes.status).toBe(200)
    expect(formatRes.headers.get('content-type')).toContain('text/javascript')
  })

  test('GET /dashboard/ui/does-not-exist.js 404s', async () => {
    const res = await handler(req('/dashboard/ui/does-not-exist.js'))
    expect(res.status).toBe(404)
  })
})

// Thin fetch wrapper for the /dashboard/api read API. Same-origin, JSON in
// and out; every function throws a plain Error with a readable message on a
// non-2xx response so callers can render it instead of blowing up.

const BASE = '/dashboard/api'

async function request(path, init) {
  let res
  try {
    res = await fetch(BASE + path, init)
  } catch (err) {
    throw new Error(`network error calling ${path}: ${err.message ?? err}`)
  }
  let body = null
  try {
    body = await res.json()
  } catch {
    // no/invalid JSON body — fall through, body stays null
  }
  if (!res.ok) {
    const message = (body && body.error) || `${res.status} ${res.statusText}`
    throw new Error(message)
  }
  return body
}

function qs(params) {
  const usp = new URLSearchParams()
  for (const [key, value] of Object.entries(params || {})) {
    if (value === undefined || value === null || value === '') continue
    usp.set(key, String(value))
  }
  const s = usp.toString()
  return s ? `?${s}` : ''
}

export const api = {
  listRuns(filter) {
    return request(`/runs${qs(filter)}`)
  },
  getRun(id) {
    return request(`/runs/${encodeURIComponent(id)}`)
  },
  getRunLogs(id) {
    return request(`/runs/${encodeURIComponent(id)}/logs`)
  },
  retryRun(id) {
    return request(`/runs/${encodeURIComponent(id)}/retry`, { method: 'POST' })
  },
  cancelRun(id) {
    return request(`/runs/${encodeURIComponent(id)}/cancel`, { method: 'POST' })
  },
  getMetrics(filter) {
    return request(`/metrics${qs(filter)}`)
  },
  getQueue(filter) {
    return request(`/queue${qs(filter)}`)
  },
  getWorkers(filter) {
    return request(`/workers${qs(filter)}`)
  },
}

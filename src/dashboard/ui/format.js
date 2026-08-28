// Small formatting helpers shared across views. No dependencies, pure
// functions, so they're easy to eyeball for correctness.

export function esc(value) {
  return String(value ?? '').replace(/[&<>"']/g, (c) => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;',
  })[c])
}

export function fmtDate(value) {
  if (!value) return '—'
  const d = value instanceof Date ? value : new Date(value)
  if (Number.isNaN(d.getTime())) return '—'
  return d.toLocaleString()
}

export function fmtRelative(value) {
  if (!value) return '—'
  const d = value instanceof Date ? value : new Date(value)
  if (Number.isNaN(d.getTime())) return '—'
  const diffMs = Date.now() - d.getTime()
  const future = diffMs < 0
  const abs = Math.abs(diffMs)
  const sec = Math.round(abs / 1000)
  let text
  if (sec < 5) text = 'just now'
  else if (sec < 60) text = `${sec}s`
  else if (sec < 3600) text = `${Math.round(sec / 60)}m`
  else if (sec < 86400) text = `${Math.round(sec / 3600)}h`
  else text = `${Math.round(sec / 86400)}d`
  if (text === 'just now') return text
  return future ? `in ${text}` : `${text} ago`
}

export function fmtDateFull(value) {
  if (!value) return '—'
  return `${fmtDate(value)} (${fmtRelative(value)})`
}

export function fmtDurationMs(ms) {
  if (ms === null || ms === undefined || Number.isNaN(ms)) return '—'
  if (ms < 1000) return `${Math.round(ms)}ms`
  const sec = ms / 1000
  if (sec < 60) return `${sec.toFixed(1)}s`
  const min = sec / 60
  if (min < 60) return `${min.toFixed(1)}m`
  const hr = min / 60
  return `${hr.toFixed(1)}h`
}

export function statusBadge(status) {
  return `<span class="badge status-${esc(status)}">${esc(status)}</span>`
}

export function aliveBadge(alive) {
  return `<span class="badge alive-${alive}">${alive ? 'alive' : 'stale'}</span>`
}

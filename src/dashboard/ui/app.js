// orqestra dashboard — vanilla JS, no framework, no build step. Client-side
// routing is a simple hash scheme:
//   #runs                 -> runs list (default)
//   #run/<id>              -> run detail
//   #metrics / #queue / #workers -> the other tabs
// State lives in module-level variables; each view renders into its own
// <section> by rebuilding innerHTML from a template string. Small enough
// dashboard that a virtual-DOM layer would be pure overhead.

import { api } from './api.js'
import { esc, fmtDate, fmtRelative, fmtDurationMs, statusBadge, aliveBadge } from './format.js'

const $ = (sel) => document.querySelector(sel)
const $$ = (sel) => Array.from(document.querySelectorAll(sel))

const views = {
  runs: $('#view-runs'),
  'run-detail': $('#view-run-detail'),
  metrics: $('#view-metrics'),
  queue: $('#view-queue'),
  workers: $('#view-workers'),
}

let runsState = { status: '', workflow: '', namespace: '', limit: 25, offset: 0, count: 0 }
let workersTimer = null

// ---- connection status --------------------------------------------------

function setConnStatus(ok, detail) {
  const el = $('#conn-status')
  el.className = `status ${ok ? 'ok' : 'err'}`
  el.textContent = ok ? 'connected' : `error: ${detail}`
}

// ---- routing --------------------------------------------------------------

function currentRoute() {
  const hash = window.location.hash.replace(/^#/, '') || 'runs'
  const runMatch = /^run\/(.+)$/.exec(hash)
  if (runMatch) return { name: 'run-detail', runId: decodeURIComponent(runMatch[1]) }
  if (views[hash]) return { name: hash }
  return { name: 'runs' }
}

function showView(name) {
  for (const [key, el] of Object.entries(views)) {
    el.classList.toggle('active', key === name)
  }
  for (const btn of $$('.tab-btn')) {
    const view = btn.dataset.view
    const active = view === name || (name === 'run-detail' && view === 'runs')
    btn.classList.toggle('active', active)
  }
}

async function route() {
  const r = currentRoute()
  showView(r.name)
  stopWorkersAutoRefresh()
  if (r.name === 'runs') return renderRunsList()
  if (r.name === 'run-detail') return renderRunDetail(r.runId)
  if (r.name === 'metrics') return renderMetrics()
  if (r.name === 'queue') return renderQueue()
  if (r.name === 'workers') return renderWorkers()
}

window.addEventListener('hashchange', route)

for (const btn of $$('.tab-btn')) {
  btn.addEventListener('click', () => {
    window.location.hash = btn.dataset.view
  })
}

// ---- runs list (#30) --------------------------------------------------

function renderErrorBox(container, err) {
  container.innerHTML = `<div class="error-box">${esc(err.message ?? String(err))}</div>`
}

async function renderRunsList() {
  const wrap = $('#runs-table-wrap')
  wrap.innerHTML = '<p class="empty">Loading...</p>'
  try {
    const { runs, count } = await api.listRuns({
      status: runsState.status || undefined,
      workflow: runsState.workflow || undefined,
      namespace: runsState.namespace || undefined,
      limit: runsState.limit,
      offset: runsState.offset,
    })
    runsState.count = count
    setConnStatus(true)
    if (runs.length === 0) {
      wrap.innerHTML = '<p class="empty">No runs yet.</p>'
    } else {
      wrap.innerHTML = `
        <div class="overflow-x">
        <table>
          <thead>
            <tr>
              <th>Status</th><th>Workflow</th><th>Namespace</th>
              <th>Created</th><th>Duration</th><th>Run ID</th>
            </tr>
          </thead>
          <tbody>
            ${runs
              .map(
                (run) => `
              <tr data-run-id="${esc(run.id)}">
                <td>${statusBadge(run.status)}</td>
                <td>${esc(run.workflowName)}</td>
                <td>${esc(run.namespace)}</td>
                <td title="${esc(fmtDate(run.createdAt))}">${esc(fmtRelative(run.createdAt))}</td>
                <td>${esc(fmtDurationMs(run.durationMs))}</td>
                <td class="dim">${esc(run.id)}</td>
              </tr>`
              )
              .join('')}
          </tbody>
        </table>
        </div>
      `
      for (const row of $$('#runs-table-wrap tr[data-run-id]')) {
        row.addEventListener('click', () => {
          window.location.hash = `run/${encodeURIComponent(row.dataset.runId)}`
        })
      }
    }
  } catch (err) {
    setConnStatus(false, err.message)
    renderErrorBox(wrap, err)
  }
  $('#page-info').textContent = `showing ${runsState.offset + 1}-${runsState.offset + runsState.count} `
  $('#page-prev').disabled = runsState.offset === 0
  $('#page-next').disabled = runsState.count < runsState.limit
}

$('#filter-apply').addEventListener('click', () => {
  runsState.status = $('#filter-status').value
  runsState.workflow = $('#filter-workflow').value.trim()
  runsState.namespace = $('#filter-namespace').value.trim()
  runsState.offset = 0
  renderRunsList()
})

$('#filter-clear').addEventListener('click', () => {
  $('#filter-status').value = ''
  $('#filter-workflow').value = ''
  $('#filter-namespace').value = ''
  runsState = { ...runsState, status: '', workflow: '', namespace: '', offset: 0 }
  renderRunsList()
})

$('#page-prev').addEventListener('click', () => {
  runsState.offset = Math.max(0, runsState.offset - runsState.limit)
  renderRunsList()
})

$('#page-next').addEventListener('click', () => {
  runsState.offset += runsState.limit
  renderRunsList()
})

$('#back-to-runs').addEventListener('click', () => {
  window.location.hash = 'runs'
})

// ---- run detail (#30 timeline, #33 errors, #31 logs, #27/#11 controls) ----

function renderTimeline(timeline) {
  if (timeline.length === 0) return '<p class="empty">No history events yet.</p>'
  return timeline
    .map(
      (h) => `
    <div class="timeline-item">
      <span class="t">${esc(fmtDate(h.at))}</span>
      <span class="type">${esc(h.type)}</span>
      <span class="data">${esc(h.step_id ? `step:${h.step_id} ` : '')}${esc(safeJson(h.data))}</span>
    </div>`
    )
    .join('')
}

function safeJson(value) {
  try {
    return JSON.stringify(value)
  } catch {
    return String(value)
  }
}

function renderSteps(steps) {
  if (steps.length === 0) return '<p class="empty">No steps.</p>'
  return `
    <div class="overflow-x">
    <table>
      <thead><tr><th>Step</th><th>Status</th><th>Attempt</th><th>Updated</th></tr></thead>
      <tbody>
        ${steps
          .map(
            (s) => `
          <tr>
            <td>${esc(s.name)}</td>
            <td>${statusBadge(s.status)}</td>
            <td>${esc(s.attempt)}/${esc(s.max_attempts)}</td>
            <td title="${esc(fmtDate(s.updated_at))}">${esc(fmtRelative(s.updated_at))}</td>
          </tr>`
          )
          .join('')}
      </tbody>
    </table>
    </div>
  `
}

function renderErrors(errors) {
  const withError = errors.filter((e) => e.error)
  if (withError.length === 0) return '<p class="empty">No step errors.</p>'
  return withError
    .map(
      (e) => `
    <div class="card">
      <div><strong>${esc(e.stepName)}</strong> <span class="dim">attempt ${esc(e.attempt)} · ${esc(
        fmtRelative(e.updatedAt)
      )}</span></div>
      <div class="dim" style="margin-top:4px">${esc(e.error.name)}: ${esc(e.error.message)}</div>
      ${e.error.stack ? `<pre class="stack">${esc(e.error.stack)}</pre>` : ''}
    </div>`
    )
    .join('')
}

function renderLogs(logs) {
  if (logs.length === 0) return '<p class="empty">No log entries.</p>'
  return `<div class="scrollbox">${logs
    .map(
      (l) => `
    <div class="timeline-item">
      <span class="t">${esc(fmtDate(l.at))}</span>
      <span class="data">${esc(safeJson(l.data))}</span>
    </div>`
    )
    .join('')}</div>`
}

async function renderRunDetail(runId) {
  const content = $('#run-detail-content')
  content.innerHTML = '<p class="empty">Loading...</p>'
  try {
    const [detail, logsResp] = await Promise.all([api.getRun(runId), api.getRunLogs(runId).catch(() => ({ logs: [] }))])
    setConnStatus(true)
    const { run, steps, timeline, errors } = detail
    const canRetry = run.status === 'dead_letter'
    const canCancel = !['completed', 'failed', 'cancelled', 'dead_letter', 'completed_with_errors'].includes(
      run.status
    )
    content.innerHTML = `
      <div class="card run-header">
        <div class="field"><span class="label">Run</span><span class="value">${esc(run.id)}</span></div>
        <div class="field"><span class="label">Status</span><span class="value">${statusBadge(run.status)}</span></div>
        <div class="field"><span class="label">Namespace</span><span class="value">${esc(run.namespace)}</span></div>
        <div class="field"><span class="label">Created</span><span class="value">${esc(fmtDate(run.created_at))}</span></div>
        <div class="field"><span class="label">Started</span><span class="value">${esc(fmtDate(run.started_at))}</span></div>
        <div class="field"><span class="label">Finished</span><span class="value">${esc(fmtDate(run.finished_at))}</span></div>
        <div class="run-actions">
          <button id="btn-retry" ${canRetry ? '' : 'disabled'} title="${canRetry ? 'Retry this dead-lettered run' : 'Only dead_letter runs can be retried'}">Retry</button>
          <button id="btn-cancel" class="danger" ${canCancel ? '' : 'disabled'} title="${canCancel ? 'Cancel this run' : 'Run is already terminal'}">Cancel</button>
        </div>
      </div>
      <div id="run-action-feedback"></div>

      <h3>Steps</h3>
      ${renderSteps(steps)}

      <h3>Timeline</h3>
      <div class="scrollbox">${renderTimeline(timeline)}</div>

      <h3>Errors</h3>
      ${renderErrors(errors)}

      <h3>Logs</h3>
      ${renderLogs(logsResp.logs ?? [])}
    `

    const feedback = $('#run-action-feedback')
    const retryBtn = $('#btn-retry')
    const cancelBtn = $('#btn-cancel')
    if (retryBtn) {
      retryBtn.addEventListener('click', async () => {
        if (!window.confirm(`Retry run ${runId}?`)) return
        try {
          const result = await api.retryRun(runId)
          feedback.innerHTML = `<div class="card">Retried — new status: ${statusBadge(result.status)}</div>`
          renderRunDetail(runId)
        } catch (err) {
          renderErrorBox(feedback, err)
        }
      })
    }
    if (cancelBtn) {
      cancelBtn.addEventListener('click', async () => {
        if (!window.confirm(`Cancel run ${runId}?`)) return
        try {
          const result = await api.cancelRun(runId)
          feedback.innerHTML = `<div class="card">Cancel ${
            result.cancelled ? 'completed' : result.pending ? 'requested (pending running step)' : 'requested'
          } — status: ${statusBadge(result.status)}</div>`
          renderRunDetail(runId)
        } catch (err) {
          renderErrorBox(feedback, err)
        }
      })
    }
  } catch (err) {
    setConnStatus(false, err.message)
    renderErrorBox(content, err)
  }
}

// ---- metrics (#32) -------------------------------------------------------

function renderMetricsRow(m) {
  const statusEntries = Object.entries(m.statusCounts || {})
  const maxCount = Math.max(1, ...statusEntries.map(([, c]) => c))
  return `
    <div class="card">
      <h3 style="margin-top:0">${esc(m.workflowName ?? 'all workflows')}</h3>
      <div class="tiles">
        <div class="tile"><div class="tile-label">Run count</div><div class="tile-value">${esc(m.runCount)}</div></div>
        <div class="tile"><div class="tile-label">Avg step duration</div><div class="tile-value">${esc(fmtDurationMs(m.avgStepDurationMs))}</div></div>
        <div class="tile"><div class="tile-label">P50 step duration</div><div class="tile-value">${esc(fmtDurationMs(m.p50StepDurationMs))}</div></div>
        <div class="tile"><div class="tile-label">P95 step duration</div><div class="tile-value">${esc(fmtDurationMs(m.p95StepDurationMs))}</div></div>
        <div class="tile"><div class="tile-label">Avg queue wait</div><div class="tile-value">${esc(fmtDurationMs(m.avgRunQueueWaitMs))}</div></div>
        <div class="tile"><div class="tile-label">Total attempts</div><div class="tile-value">${esc(m.totalAttempts)}</div></div>
        <div class="tile"><div class="tile-label">Total reclaims</div><div class="tile-value">${esc(m.totalReclaims)}</div></div>
      </div>
      <div class="status-bars">
        ${statusEntries
          .map(
            ([status, cnt]) => `
          <div class="status-bar-row">
            <span class="name">${esc(status)}</span>
            <span class="bar-track"><span class="bar-fill" style="width:${(cnt / maxCount) * 100}%"></span></span>
            <span class="count">${esc(cnt)}</span>
          </div>`
          )
          .join('')}
      </div>
    </div>
  `
}

async function renderMetrics() {
  const content = $('#metrics-content')
  content.innerHTML = '<p class="empty">Loading...</p>'
  try {
    const groupByWorkflow = $('#metrics-group').checked
    const { metrics } = await api.getMetrics({ groupByWorkflow: groupByWorkflow || undefined })
    setConnStatus(true)
    if (metrics.length === 0) {
      content.innerHTML = '<p class="empty">No metrics yet — no runs recorded.</p>'
      return
    }
    content.innerHTML = metrics.map(renderMetricsRow).join('')
  } catch (err) {
    setConnStatus(false, err.message)
    renderErrorBox(content, err)
  }
}

$('#metrics-refresh').addEventListener('click', renderMetrics)
$('#metrics-group').addEventListener('change', renderMetrics)

// ---- queue & throughput (#35) --------------------------------------------

function renderSparkline(throughput) {
  if (throughput.length === 0) return '<p class="empty">No throughput in this window.</p>'
  const width = 640
  const height = 120
  const barGap = 2
  const barWidth = Math.max(2, width / throughput.length - barGap)
  const maxVal = Math.max(1, ...throughput.map((b) => b.completed + b.failed))
  const bars = throughput
    .map((b, i) => {
      const x = i * (barWidth + barGap)
      const completedH = (b.completed / maxVal) * (height - 20)
      const failedH = (b.failed / maxVal) * (height - 20)
      const completedY = height - completedH
      const failedY = completedY - failedH
      return `
        <rect class="bar-completed" x="${x}" y="${completedY}" width="${barWidth}" height="${completedH}">
          <title>${esc(fmtDate(b.bucketStart))}: ${b.completed} completed</title>
        </rect>
        <rect class="bar-failed" x="${x}" y="${failedY}" width="${barWidth}" height="${failedH}">
          <title>${esc(fmtDate(b.bucketStart))}: ${b.failed} failed</title>
        </rect>
      `
    })
    .join('')
  return `<svg class="sparkline" viewBox="0 0 ${width} ${height}" width="100%" height="${height}">${bars}</svg>`
}

function renderQueueDepth(depth) {
  const entries = Object.entries(depth)
  if (entries.length === 0) return '<p class="empty">No steps recorded.</p>'
  const maxCount = Math.max(1, ...entries.map(([, c]) => c))
  return `
    <div class="status-bars">
      ${entries
        .map(
          ([status, cnt]) => `
        <div class="status-bar-row">
          <span class="name">${esc(status)}</span>
          <span class="bar-track"><span class="bar-fill" style="width:${(cnt / maxCount) * 100}%"></span></span>
          <span class="count">${esc(cnt)}</span>
        </div>`
        )
        .join('')}
    </div>
  `
}

async function renderQueue() {
  const content = $('#queue-content')
  content.innerHTML = '<p class="empty">Loading...</p>'
  try {
    const { depth, throughput } = await api.getQueue({})
    setConnStatus(true)
    content.innerHTML = `
      <div class="card">
        <h3 style="margin-top:0">Queue depth by step status</h3>
        ${renderQueueDepth(depth)}
      </div>
      <div class="card">
        <h3 style="margin-top:0">Throughput (last hour, green = completed, red = failed)</h3>
        ${renderSparkline(throughput)}
      </div>
    `
  } catch (err) {
    setConnStatus(false, err.message)
    renderErrorBox(content, err)
  }
}

$('#queue-refresh').addEventListener('click', renderQueue)

// ---- workers (#34), auto-refreshing ---------------------------------------

function renderWorkersTable(workers) {
  if (workers.length === 0) return '<p class="empty">No workers have reported in yet.</p>'
  return `
    <div class="overflow-x">
    <table>
      <thead>
        <tr>
          <th>Worker</th><th>Hostname</th><th>Health</th><th>Status</th>
          <th>Leased steps</th><th>Concurrency</th><th>Last heartbeat</th><th>Started</th>
        </tr>
      </thead>
      <tbody>
        ${workers
          .map(
            (w) => `
          <tr>
            <td class="dim">${esc(w.workerId)}</td>
            <td>${esc(w.hostname ?? '—')}</td>
            <td>${aliveBadge(w.alive)}</td>
            <td>${esc(w.status)}</td>
            <td>${esc(w.leasedSteps)}</td>
            <td>${esc(w.concurrency ?? '—')}</td>
            <td title="${esc(fmtDate(w.lastHeartbeatAt))}">${esc(fmtRelative(w.lastHeartbeatAt))}</td>
            <td title="${esc(fmtDate(w.startedAt))}">${esc(fmtRelative(w.startedAt))}</td>
          </tr>`
          )
          .join('')}
      </tbody>
    </table>
    </div>
  `
}

async function renderWorkers() {
  const content = $('#workers-content')
  const isFirstLoad = content.innerHTML.includes('Loading') || content.innerHTML.trim() === ''
  if (isFirstLoad) content.innerHTML = '<p class="empty">Loading...</p>'
  try {
    const { workers } = await api.getWorkers({})
    setConnStatus(true)
    content.innerHTML = renderWorkersTable(workers)
  } catch (err) {
    setConnStatus(false, err.message)
    renderErrorBox(content, err)
  }
  startWorkersAutoRefresh()
}

function startWorkersAutoRefresh() {
  if (workersTimer) return
  workersTimer = setInterval(() => {
    if (currentRoute().name === 'workers') renderWorkers()
  }, 5000)
}

function stopWorkersAutoRefresh() {
  if (workersTimer) {
    clearInterval(workersTimer)
    workersTimer = null
  }
}

// ---- boot -----------------------------------------------------------------

route()

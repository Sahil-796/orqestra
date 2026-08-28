// small, dependency-free structured logger — JSON lines to stdout/stderr

import type { LogLevel } from '../config.ts'
import type { Db } from '../store/client.ts'
import { insertHistory } from '../store/repositories.ts'

export type LogFields = Record<string, unknown>

export interface Logger {
  debug(msg: string, fields?: LogFields): void
  info(msg: string, fields?: LogFields): void
  warn(msg: string, fields?: LogFields): void
  error(msg: string, fields?: LogFields): void
  child(fields: LogFields): Logger
}

const LEVEL_ORDER: Record<LogLevel, number> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
}

function write(level: LogLevel, msg: string, fields: LogFields): void {
  const line = JSON.stringify({
    level,
    msg,
    time: new Date().toISOString(),
    ...fields,
  })
  if (level === 'error' || level === 'warn') {
    console.error(line)
  } else {
    console.log(line)
  }
}

class BaseLogger implements Logger {
  constructor(
    private readonly minLevel: LogLevel,
    private readonly baseFields: LogFields = {}
  ) {}

  private log(level: LogLevel, msg: string, fields?: LogFields): void {
    if (LEVEL_ORDER[level] < LEVEL_ORDER[this.minLevel]) return
    write(level, msg, { ...this.baseFields, ...fields })
  }

  debug(msg: string, fields?: LogFields): void {
    this.log('debug', msg, fields)
  }

  info(msg: string, fields?: LogFields): void {
    this.log('info', msg, fields)
  }

  warn(msg: string, fields?: LogFields): void {
    this.log('warn', msg, fields)
  }

  error(msg: string, fields?: LogFields): void {
    this.log('error', msg, fields)
  }

  child(fields: LogFields): Logger {
    return new BaseLogger(this.minLevel, { ...this.baseFields, ...fields })
  }
}

export function createLogger(minLevel: LogLevel = 'info', baseFields: LogFields = {}): Logger {
  return new BaseLogger(minLevel, baseFields)
}

// ---- #31: run-scoped persisting logger --------------------------------------
//
// Wraps a plain Logger so every call ALSO lands as a `history` row
// (`type: 'log'`) scoped to one run — that's what `getRunLogs` (repositories.ts)
// reads back for the dashboard's log view. The stdout side is unchanged (same
// JSON lines, same level filtering); persistence is purely additive.
//
// Best-effort by design: a storage hiccup while writing a log line must never
// take down the workflow it's describing, so persistence failures are caught
// and reported through the stdout logger instead of thrown.
class PersistingLogger implements Logger {
  constructor(
    private readonly db: Db,
    private readonly runId: string,
    private readonly inner: Logger,
    private readonly baseFields: LogFields = {}
  ) {}

  private persist(level: LogLevel, msg: string, fields?: LogFields): void {
    try {
      const merged: LogFields = { ...this.baseFields, ...fields }
      const { stepId, ...rest } = merged
      const stepIdStr = typeof stepId === 'string' ? stepId : undefined
      // Fire-and-forget: logging must never make the caller await a DB
      // round-trip, and a rejected insert is handled below rather than
      // propagated.
      void insertHistory(this.db, {
        runId: this.runId,
        stepId: stepIdStr,
        type: 'log',
        data: { level, msg, ...rest },
      }).catch((e: unknown) => {
        this.inner.error('failed to persist run log', {
          runId: this.runId,
          originalMsg: msg,
          error: e instanceof Error ? e.message : String(e),
        })
      })
    } catch (e) {
      // Building the insert itself threw (shouldn't happen, but this is a
      // logger — it must not be the thing that crashes the workflow).
      this.inner.error('failed to build persisted run log', {
        runId: this.runId,
        originalMsg: msg,
        error: e instanceof Error ? e.message : String(e),
      })
    }
  }

  debug(msg: string, fields?: LogFields): void {
    this.inner.debug(msg, fields)
    this.persist('debug', msg, fields)
  }

  info(msg: string, fields?: LogFields): void {
    this.inner.info(msg, fields)
    this.persist('info', msg, fields)
  }

  warn(msg: string, fields?: LogFields): void {
    this.inner.warn(msg, fields)
    this.persist('warn', msg, fields)
  }

  error(msg: string, fields?: LogFields): void {
    this.inner.error(msg, fields)
    this.persist('error', msg, fields)
  }

  child(fields: LogFields): Logger {
    return new PersistingLogger(this.db, this.runId, this.inner.child(fields), {
      ...this.baseFields,
      ...fields,
    })
  }
}

/**
 * A Logger scoped to one run: writes the same JSON lines as `createLogger`
 * AND persists every call as a `history` row (`type: 'log'`) via
 * `insertHistory`, so `getRunLogs`/the dashboard can show it later. Pass an
 * existing Logger as `inner` to reuse its min-level/base fields for the
 * stdout side; otherwise a fresh one is created from `base`.
 */
export function createRunLogger(db: Db, runId: string, base: LogFields = {}, inner?: Logger): Logger {
  return new PersistingLogger(db, runId, inner ?? createLogger('info', base), base)
}

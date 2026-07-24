// small, dependency-free structured logger — JSON lines to stdout/stderr

import type { LogLevel } from '../config.ts'

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

// typed env/config loading — fails fast with a clear error on malformed input

export type LogLevel = 'debug' | 'info' | 'warn' | 'error'

export interface OrqConfig {
  databaseUrl: string
  poolSize: number
  logLevel: LogLevel
}

const DEFAULT_DATABASE_URL = 'postgres://orqestra:orqestra@localhost:5433/orqestra'
const DEFAULT_POOL_SIZE = 10
const DEFAULT_LOG_LEVEL: LogLevel = 'info'

const LOG_LEVELS: readonly LogLevel[] = ['debug', 'info', 'warn', 'error']

function isLogLevel(value: string): value is LogLevel {
  return (LOG_LEVELS as readonly string[]).includes(value)
}

function parsePoolSize(raw: string | undefined): number {
  if (raw === undefined || raw === '') return DEFAULT_POOL_SIZE
  const n = Number(raw)
  if (!Number.isInteger(n) || n <= 0) {
    throw new Error(`ORQ_POOL_SIZE must be a positive integer, got: ${raw}`)
  }
  return n
}

function parseLogLevel(raw: string | undefined): LogLevel {
  if (raw === undefined || raw === '') return DEFAULT_LOG_LEVEL
  if (!isLogLevel(raw)) {
    throw new Error(`ORQ_LOG_LEVEL must be one of ${LOG_LEVELS.join(', ')}, got: ${raw}`)
  }
  return raw
}

function parseDatabaseUrl(raw: string | undefined): string {
  const url = raw && raw !== '' ? raw : DEFAULT_DATABASE_URL
  try {
    // eslint-disable-next-line no-new
    new URL(url)
  } catch {
    throw new Error(`DATABASE_URL is not a valid URL: ${url}`)
  }
  return url
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): OrqConfig {
  return {
    databaseUrl: parseDatabaseUrl(env.DATABASE_URL),
    poolSize: parsePoolSize(env.ORQ_POOL_SIZE),
    logLevel: parseLogLevel(env.ORQ_LOG_LEVEL),
  }
}

// typed env/config loading — fails fast with a clear error on malformed input

export type LogLevel = 'debug' | 'info' | 'warn' | 'error'

export interface OrqConfig {
  databaseUrl: string
  poolSize: number
  logLevel: LogLevel
  /** Default worker lease TTL — how long a claimed step stays claimed before it's presumed crashed. */
  leaseTtlMs: number
  /** Default worker poll interval — how long a worker sleeps after finding the queue empty. */
  pollIntervalMs: number
  /** Default max steps a single worker runs at once. */
  workerConcurrency: number
  /** HTTP trigger server bind host. */
  httpHost: string
  /** HTTP trigger server bind port. */
  httpPort: number
}

const DEFAULT_DATABASE_URL = 'postgres://orqestra:orqestra@localhost:5433/orqestra'
const DEFAULT_POOL_SIZE = 10
const DEFAULT_LOG_LEVEL: LogLevel = 'info'
const DEFAULT_LEASE_TTL_MS = 30_000
const DEFAULT_POLL_INTERVAL_MS = 200
const DEFAULT_WORKER_CONCURRENCY = 1
const DEFAULT_HTTP_HOST = '0.0.0.0'
const DEFAULT_HTTP_PORT = 3000

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

function parseLeaseTtlMs(raw: string | undefined): number {
  if (raw === undefined || raw === '') return DEFAULT_LEASE_TTL_MS
  const n = Number(raw)
  if (!Number.isInteger(n) || n <= 0) {
    throw new Error(`ORQ_LEASE_TTL_MS must be a positive integer, got: ${raw}`)
  }
  return n
}

function parsePollIntervalMs(raw: string | undefined): number {
  if (raw === undefined || raw === '') return DEFAULT_POLL_INTERVAL_MS
  const n = Number(raw)
  if (!Number.isInteger(n) || n <= 0) {
    throw new Error(`ORQ_POLL_INTERVAL_MS must be a positive integer, got: ${raw}`)
  }
  return n
}

function parseWorkerConcurrency(raw: string | undefined): number {
  if (raw === undefined || raw === '') return DEFAULT_WORKER_CONCURRENCY
  const n = Number(raw)
  if (!Number.isInteger(n) || n <= 0) {
    throw new Error(`ORQ_WORKER_CONCURRENCY must be a positive integer, got: ${raw}`)
  }
  return n
}

function parseHttpHost(raw: string | undefined): string {
  if (raw === undefined || raw === '') return DEFAULT_HTTP_HOST
  return raw
}

function parseHttpPort(raw: string | undefined): number {
  if (raw === undefined || raw === '') return DEFAULT_HTTP_PORT
  const n = Number(raw)
  if (!Number.isInteger(n) || n < 0 || n > 65535) {
    throw new Error(`ORQ_HTTP_PORT must be an integer in 0..65535, got: ${raw}`)
  }
  return n
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
    leaseTtlMs: parseLeaseTtlMs(env.ORQ_LEASE_TTL_MS),
    pollIntervalMs: parsePollIntervalMs(env.ORQ_POLL_INTERVAL_MS),
    workerConcurrency: parseWorkerConcurrency(env.ORQ_WORKER_CONCURRENCY),
    httpHost: parseHttpHost(env.ORQ_HTTP_HOST),
    httpPort: parseHttpPort(env.ORQ_HTTP_PORT),
  }
}

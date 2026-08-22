// Webhook -> event mapping (#25). Pure and DB-free on purpose: given the
// pieces of an inbound HTTP request, decide what event name, correlation key,
// idempotency key and payload it durably becomes. Kept separate from
// src/triggers/http.ts so this mapping policy is unit-testable without
// binding a socket or touching Postgres.

export interface WebhookRequestInput {
  /** The `:name` path segment from `POST /webhooks/:name`. */
  routeName: string
  /** Parsed JSON body. Untrusted — validated defensively, never trusted blindly. */
  body: unknown
  /** Request headers, lower-cased keys (Bun's `Headers` already lower-cases on get, but callers may hand in a plain record). */
  headers: Record<string, string | undefined>
  /** Provenance label recorded on the event row. Defaults to 'webhook'. */
  source?: string
}

export interface MappedWebhookEvent {
  name: string
  correlationKey?: string
  payload: unknown
  source: string
  idempotencyKey?: string
}

function header(headers: Record<string, string | undefined>, key: string): string | undefined {
  const value = headers[key] ?? headers[key.toLowerCase()]
  return typeof value === 'string' && value.length > 0 ? value : undefined
}

function stringField(body: unknown, field: string): string | undefined {
  if (typeof body !== 'object' || body === null) return undefined
  const value = (body as Record<string, unknown>)[field]
  return typeof value === 'string' && value.length > 0 ? value : undefined
}

/**
 * Map an inbound webhook request to a durable event. The event name comes
 * from the route's `:name` segment (a body-level `event`/`name` field, if
 * present, further qualifies it as `"<route>.<field>"` — useful for a single
 * webhook endpoint that carries several event types, e.g. a payment
 * provider's `POST /webhooks/stripe` with `{ type: "charge.succeeded" }`).
 *
 * The idempotency key is picked in priority order — an explicit
 * `Idempotency-Key` header wins (the same header the API-trigger route
 * honors), then a common provider convention (`X-Webhook-Id` /
 * `X-Delivery-Id`), then a body-level `id`/`eventId` field. Redelivery
 * without any of these is accepted but not deduplicated — the caller can't
 * promise exactly-once without SOME stable id to key on, and refusing the
 * whole delivery for that would be worse than an occasional duplicate wake.
 */
export function mapWebhookToEvent(input: WebhookRequestInput): MappedWebhookEvent {
  const routeName = input.routeName.trim()
  if (routeName.length === 0) {
    throw new Error('mapWebhookToEvent: routeName must be non-empty')
  }

  const bodyEventType = stringField(input.body, 'event') ?? stringField(input.body, 'type')
  const name = bodyEventType ? `${routeName}.${bodyEventType}` : routeName

  const correlationKey =
    stringField(input.body, 'correlationKey') ?? stringField(input.body, 'correlationId')

  const idempotencyKey =
    header(input.headers, 'idempotency-key') ??
    header(input.headers, 'x-webhook-id') ??
    header(input.headers, 'x-delivery-id') ??
    stringField(input.body, 'idempotencyKey') ??
    stringField(input.body, 'id') ??
    stringField(input.body, 'eventId')

  return {
    name,
    correlationKey,
    payload: input.body,
    source: input.source ?? 'webhook',
    idempotencyKey,
  }
}

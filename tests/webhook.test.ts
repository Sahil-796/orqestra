// Pure unit coverage for src/triggers/webhook.ts's request -> event mapping —
// no DB, no socket. The end-to-end "does this actually wake a blocked
// workflow" proof lives in tests/http-triggers.test.ts, reusing
// tests/signals.test.ts's pattern.

import { describe, expect, test } from 'bun:test'
import { mapWebhookToEvent } from '../src/triggers/webhook.ts'

describe('mapWebhookToEvent', () => {
  test('uses the route name as the event name by default', () => {
    const mapped = mapWebhookToEvent({ routeName: 'stripe', body: { foo: 'bar' }, headers: {} })
    expect(mapped.name).toBe('stripe')
    expect(mapped.payload).toEqual({ foo: 'bar' })
    expect(mapped.source).toBe('webhook')
  })

  test('qualifies the event name with a body-level `type` field', () => {
    const mapped = mapWebhookToEvent({
      routeName: 'stripe',
      body: { type: 'charge.succeeded' },
      headers: {},
    })
    expect(mapped.name).toBe('stripe.charge.succeeded')
  })

  test('qualifies the event name with a body-level `event` field', () => {
    const mapped = mapWebhookToEvent({
      routeName: 'github',
      body: { event: 'push' },
      headers: {},
    })
    expect(mapped.name).toBe('github.push')
  })

  test('reads correlationKey from the body', () => {
    const mapped = mapWebhookToEvent({
      routeName: 'orders',
      body: { correlationId: 'order-42' },
      headers: {},
    })
    expect(mapped.correlationKey).toBe('order-42')
  })

  test('idempotency key priority: Idempotency-Key header wins over everything', () => {
    const mapped = mapWebhookToEvent({
      routeName: 'orders',
      body: { id: 'body-id' },
      headers: { 'idempotency-key': 'header-key', 'x-webhook-id': 'wh-id' },
    })
    expect(mapped.idempotencyKey).toBe('header-key')
  })

  test('idempotency key falls back to provider delivery-id headers', () => {
    const mapped = mapWebhookToEvent({
      routeName: 'orders',
      body: {},
      headers: { 'x-webhook-id': 'wh-id' },
    })
    expect(mapped.idempotencyKey).toBe('wh-id')
  })

  test('idempotency key falls back to an explicit body idempotencyKey', () => {
    const mapped = mapWebhookToEvent({
      routeName: 'orders',
      body: { idempotencyKey: 'body-key' },
      headers: {},
    })
    expect(mapped.idempotencyKey).toBe('body-key')
  })

  test('a body id/eventId is NOT used as an idempotency key (it is usually a resource id, not a delivery id, so deduping on it would silently drop distinct deliveries about the same resource)', () => {
    const mapped = mapWebhookToEvent({
      routeName: 'orders',
      body: { id: 'body-id', eventId: 'evt-id' },
      headers: {},
    })
    expect(mapped.idempotencyKey).toBeUndefined()
  })

  test('no idempotency key when nothing usable is present', () => {
    const mapped = mapWebhookToEvent({ routeName: 'orders', body: {}, headers: {} })
    expect(mapped.idempotencyKey).toBeUndefined()
  })

  test('throws on an empty route name', () => {
    expect(() => mapWebhookToEvent({ routeName: '', body: {}, headers: {} })).toThrow()
  })
})

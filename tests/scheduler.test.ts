import { describe, expect, test } from 'bun:test'
import {
  dependenciesSatisfied,
  newlyReadySteps,
  isRunComplete,
  isRunBlocked,
  type StepLike,
} from '../src/engine/scheduler.ts'

function step(name: string, status: string, depends_on: string[] = []): StepLike {
  return { name, status, depends_on }
}

describe('dependenciesSatisfied', () => {
  test('true when there are no dependencies', () => {
    const s = step('a', 'pending')
    expect(dependenciesSatisfied(s, [s])).toBe(true)
  })

  test('true only once every named dep is completed', () => {
    const all = [step('a', 'completed'), step('b', 'running'), step('c', 'pending', ['a', 'b'])]
    expect(dependenciesSatisfied(all[2]!, all)).toBe(false)
    all[1] = step('b', 'completed')
    expect(dependenciesSatisfied(all[2]!, all)).toBe(true)
  })

  test('a missing dependency name is never satisfied', () => {
    const all = [step('c', 'pending', ['ghost'])]
    expect(dependenciesSatisfied(all[0]!, all)).toBe(false)
  })
})

describe('newlyReadySteps', () => {
  test('returns pending steps whose deps just all completed', () => {
    const all = [
      step('a', 'completed'),
      step('b', 'completed'),
      step('c', 'pending', ['a', 'b']),
      step('d', 'pending', ['c']),
      step('e', 'running'),
    ]
    expect(newlyReadySteps(all).map((s) => s.name)).toEqual(['c'])
  })

  test('empty when nothing pending has satisfied deps', () => {
    const all = [step('a', 'running'), step('b', 'pending', ['a'])]
    expect(newlyReadySteps(all)).toEqual([])
  })
})

describe('isRunComplete', () => {
  test('true only when every step is completed', () => {
    expect(isRunComplete([step('a', 'completed'), step('b', 'completed')])).toBe(true)
    expect(isRunComplete([step('a', 'completed'), step('b', 'running')])).toBe(false)
  })
})

describe('isRunBlocked', () => {
  test('not blocked while something is ready or running', () => {
    const all = [step('a', 'ready'), step('b', 'pending', ['nonexistent'])]
    expect(isRunBlocked(all)).toBe(false)
  })

  test('not blocked when a pending step only depends on still-in-flight steps', () => {
    const all = [step('a', 'pending', []), step('b', 'pending', ['a'])]
    // nothing ready/running here on purpose (e.g. the read happened between
    // transitions) — but 'a' has no deps so it's salvageable, not dead.
    expect(isRunBlocked(all)).toBe(false)
  })

  test('blocked when every pending step depends on a failed step', () => {
    const all = [step('a', 'failed'), step('b', 'pending', ['a'])]
    expect(isRunBlocked(all)).toBe(true)
  })

  test('blocked when every pending step depends on a cancelled step', () => {
    const all = [step('a', 'cancelled'), step('b', 'pending', ['a'])]
    expect(isRunBlocked(all)).toBe(true)
  })

  test('blocked when a pending step depends on a name that does not exist', () => {
    const all = [step('b', 'pending', ['ghost'])]
    expect(isRunBlocked(all)).toBe(true)
  })

  test('not blocked when complete', () => {
    expect(isRunBlocked([step('a', 'completed')])).toBe(false)
  })

  test('not blocked when nothing pending and nothing ready/running (already resolved)', () => {
    expect(isRunBlocked([step('a', 'completed'), step('b', 'failed')])).toBe(false)
  })
})

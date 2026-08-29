#!/usr/bin/env bun
// CLI entry point: `bun run bench <scenario> [flags]` (wired as `bun run
// bench` in package.json). Parses argv with no external dependency, merges
// knobs (scenario defaults < CLI flags < required fallbacks), migrates the
// DB idempotently, runs the scenario (once, or swept across a knob), prints
// a report via bench/metrics.ts's formatReport, and optionally appends the
// raw BenchResult(s) to bench/results/.

import { mkdir, writeFile } from 'node:fs/promises'
import { join, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import { formatReport } from './metrics.ts'
import { scenarios, type Scenario, type ScenarioKnobs } from './scenarios.ts'
import { runScenario, type BenchResult } from './harness.ts'

const NUMERIC_FLAGS: Record<string, keyof ScenarioKnobs> = {
  '--runs': 'runs',
  '--workers': 'workers',
  '--concurrency': 'concurrency',
  '--steps': 'steps',
  '--width': 'width',
  '--fail': 'fail',
  '--step-work-ms': 'stepWorkMs',
  '--lease-ttl': 'leaseTtlMs',
  '--poll-interval': 'pollIntervalMs',
}

const DEFAULT_RUNS = 50
const DEFAULT_WORKERS = 4
const DEFAULT_CONCURRENCY = 4

interface ParsedArgs {
  scenarioName: string | undefined
  flagKnobs: Partial<ScenarioKnobs>
  sweep: { knob: keyof ScenarioKnobs; values: number[] } | undefined
  json: boolean
}

function usageAndExit(): never {
  const names = Object.keys(scenarios).sort()
  console.error('Usage: bun run bench <scenario> [flags]')
  console.error(`Available scenarios: ${names.join(', ')}`)
  console.error(
    'Flags: --runs --workers --concurrency --steps --width --fail --step-work-ms --lease-ttl --poll-interval (numbers), --sweep=<knob>=v1,v2,..., --json'
  )
  process.exit(1)
}

function parseArgs(argv: string[]): ParsedArgs {
  const [first, ...rest] = argv
  const scenarioName = first && !first.startsWith('--') ? first : undefined
  const flagKnobs: Partial<ScenarioKnobs> = {}
  let sweep: ParsedArgs['sweep']
  let json = false

  for (const arg of rest) {
    if (arg === '--json') {
      json = true
      continue
    }
    if (arg.startsWith('--sweep=')) {
      const spec = arg.slice('--sweep='.length)
      const eq = spec.indexOf('=')
      if (eq === -1) {
        console.error(`bad --sweep spec "${spec}", expected knob=v1,v2,...`)
        process.exit(1)
      }
      const knobName = spec.slice(0, eq)
      const valuesRaw = spec.slice(eq + 1)
      const values = valuesRaw.split(',').map((v) => Number(v.trim()))
      if (values.some((v) => !Number.isFinite(v))) {
        console.error(`bad --sweep values "${valuesRaw}", expected a comma-separated list of numbers`)
        process.exit(1)
      }
      const knobKeys: (keyof ScenarioKnobs)[] = [
        'runs',
        'workers',
        'concurrency',
        'steps',
        'width',
        'fail',
        'stepWorkMs',
        'leaseTtlMs',
        'pollIntervalMs',
      ]
      const matched = knobKeys.find((k) => k === knobName)
      if (!matched) {
        console.error(`bad --sweep knob "${knobName}", expected one of: ${knobKeys.join(', ')}`)
        process.exit(1)
      }
      sweep = { knob: matched, values }
      continue
    }
    const eqIdx = arg.indexOf('=')
    const flagName = eqIdx === -1 ? arg : arg.slice(0, eqIdx)
    const knobKey = NUMERIC_FLAGS[flagName]
    if (!knobKey) {
      console.error(`unknown flag "${arg}"`)
      usageAndExit()
    }
    let valueRaw: string | undefined
    if (eqIdx !== -1) {
      valueRaw = arg.slice(eqIdx + 1)
    } else {
      const idx = rest.indexOf(arg)
      valueRaw = rest[idx + 1]
    }
    const value = Number(valueRaw)
    if (valueRaw === undefined || !Number.isFinite(value)) {
      console.error(`flag "${flagName}" needs a numeric value`)
      process.exit(1)
    }
    flagKnobs[knobKey] = value
  }

  return { scenarioName, flagKnobs, sweep, json }
}

function mergeKnobs(scenario: Scenario, flagKnobs: Partial<ScenarioKnobs>): ScenarioKnobs {
  const merged: Partial<ScenarioKnobs> = { ...scenario.defaultKnobs, ...flagKnobs }
  return {
    runs: merged.runs ?? DEFAULT_RUNS,
    workers: merged.workers ?? DEFAULT_WORKERS,
    concurrency: merged.concurrency ?? DEFAULT_CONCURRENCY,
    steps: merged.steps,
    width: merged.width,
    fail: merged.fail,
    stepWorkMs: merged.stepWorkMs,
    leaseTtlMs: merged.leaseTtlMs,
    pollIntervalMs: merged.pollIntervalMs,
  }
}

function safeTimestamp(): string {
  return new Date().toISOString().replace(/[:.]/g, '-')
}

function printResult(scenarioName: string, knobs: ScenarioKnobs, result: BenchResult): void {
  console.log(
    formatReport({
      scenario: scenarioName,
      runs: knobs.runs,
      steps: result.steps,
      workers: knobs.workers,
      wallMs: result.wallMs,
      runsPerSec: result.runsPerSec,
      stepsPerSec: result.stepsPerSec,
      latency: result.latency,
      extra: {
        emptyPollRatio: result.emptyPollRatio.toFixed(3),
        claimAttempts: result.claimAttempts,
        claimsFound: result.claimsFound,
        allTerminal: String(result.allTerminal),
        unexpectedFailures: result.unexpectedFailures,
      },
    })
  )
}

async function writeJsonResults(scenarioName: string, results: BenchResult[]): Promise<void> {
  const here = dirname(fileURLToPath(import.meta.url))
  const dir = join(here, 'results')
  await mkdir(dir, { recursive: true })
  const path = join(dir, `${scenarioName}-${safeTimestamp()}.json`)
  await writeFile(path, JSON.stringify(results, null, 2))
  console.log(`wrote ${path}`)
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2))
  if (!args.scenarioName) usageAndExit()

  const scenario = scenarios[args.scenarioName]
  if (!scenario) {
    console.error(`unknown scenario "${args.scenarioName}"`)
    usageAndExit()
  }

  const migrateDb = createDb()
  try {
    await migrate(migrateDb)
  } finally {
    await migrateDb.end()
  }

  let anyNotTerminal = false
  let anyUnexpectedFailures = 0
  const allResults: BenchResult[] = []

  if (args.sweep) {
    const { knob, values } = args.sweep
    const baseline: { workers: number; result: BenchResult }[] = []
    for (const value of values) {
      const knobs = mergeKnobs(scenario, { ...args.flagKnobs, [knob]: value })
      const result = await runScenario(scenario, knobs)
      allResults.push(result)
      printResult(scenario.name, knobs, result)
      if (!result.allTerminal) anyNotTerminal = true
      anyUnexpectedFailures += result.unexpectedFailures
      baseline.push({ workers: value, result })
    }
    const first = baseline[0]
    if (first && first.result.runsPerSec > 0) {
      console.log('')
      console.log(`scaling summary (vs ${knob}=${first.workers}):`)
      for (const { workers, result } of baseline) {
        const ratio = result.runsPerSec / first.result.runsPerSec
        console.log(
          `  ${knob}=${workers}: ${result.runsPerSec.toFixed(2)} runs/s (${ratio.toFixed(2)}x), ${result.stepsPerSec.toFixed(2)} steps/s`
        )
      }
    }
  } else {
    const knobs = mergeKnobs(scenario, args.flagKnobs)
    const result = await runScenario(scenario, knobs)
    allResults.push(result)
    printResult(scenario.name, knobs, result)
    if (!result.allTerminal) anyNotTerminal = true
    anyUnexpectedFailures += result.unexpectedFailures
  }

  if (args.json) {
    await writeJsonResults(scenario.name, allResults)
  }

  if (anyNotTerminal || anyUnexpectedFailures > 0) {
    console.error(
      `bench failed: allTerminal violated=${anyNotTerminal}, unexpectedFailures=${anyUnexpectedFailures}`
    )
    process.exit(1)
  }
  process.exit(0)
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})

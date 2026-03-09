export type OrqestraOptions = {
  redisUrl: string
  prefix?: string
}

export type StepStatus = 'pending' | 'running' | 'completed' | 'failed' | 'dlq'

export type WorkflowStatus = 'pending' | 'running' | 'completed' | 'failed' | 'dlq'
export type WorkflowState = {
  id: string
  name: string
  status: WorkflowStatus
  payload: unknown
  steps: StepState[]
  createdAt: number
  updatedAt: number
}

export type StepResult<T = unknown> = {
  data?: T
  error?: string
}

export type StepState = {
  name: string
  status: StepStatus
  attempts: number
  startedAt?: number
  finishedAt?: number
  result?: StepResult
}


export type StepContext = {
  workflowId: string
  payload: unknown //  ctx passed to the whole workflow
  step: <T>(name: string, fn: () => Promise<T>) => Promise<T> // step is a func which receves name, fn and returns T
}

export type WorkflowHandler = (ctx: StepContext) => Promise<void>

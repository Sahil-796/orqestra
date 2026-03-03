export interface OrqestraOptions {
  redisUrl: string
  prefix?: string
}

export type StepStatus = 'pending' | 'running' | 'completed' | 'failed' | 'dlq'

export type WorkflowStatus = 'pending' | 'running' | 'completed' | 'failed' | 'dlq'
export interface WorkflowState {
  id: string
  name: string
  status: WorkflowStatus
  payload: unknown
  steps: StepState[]
  createdAt: number
  updatedAt: number
}

export interface StepResult<T = unknown> {
  data?: T
  error?: string
}

export interface StepState {
  name: string
  status: StepStatus
  attempts: number
  startedAt?: number
  finishedAt?: number
  result?: StepResult
}


// export interface StepContext {
//   workflowId: string
//   payload: unknown
//   step: <T>(name: string, fn: () => Promise<T>) => Promise<T>
// }

// export type WorkflowHandler = (ctx: StepContext) => Promise<void>

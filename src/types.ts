import Redis from 'ioredis'

export interface OrqestraOptions {
  redis: Redis | string
  prefix?: string
  maxAttempts?: number
  visibilityTimeout?: number
}

// export type StepStatus = 'pending' | 'running' | 'completed' | 'failed' | 'dlq'
// export type WorkflowStatus = 'pending' | 'running' | 'completed' | 'failed' | 'dlq'

// export interface StepResult {
//   data?: any
//   error?: string
// }

// export interface StepState {
//   name: string
//   status: StepStatus
//   attempts: number
//   startedAt?: number
//   finishedAt?: number
//   result?: StepResult
// }

// export interface WorkflowState {
//   id: string
//   type: string
//   status: WorkflowStatus
//   currentStep: string
//   payload: any
//   steps: StepState[]
//   createdAt: number
//   updatedAt: number
// }


// export type StepHandler = (ctx: StepContext) => Promise<any>

// export interface StepContext {
//   workflowId: string
//   stepName: string
//   idempotencyKey: string
//   attempt: number
//   payload: any
// }

// Type definitions for Backoff

/// <reference path="../node/node.d.ts" />

declare module "backoff" {
  import events = require("events");

  export interface BackoffOptions {
    randomisationFactor?: number;
    initialDelay?: number;
    maxDelay?: number;
  }

  export interface BackoffStrategy extends events.EventEmitter {
    failAfter(numberOfBackoffs: number): void;
    backoff(err?: Error): void;
    reset(): void;
  }

  export function fibonacci(options?: BackoffOptions): BackoffStrategy;
  export function exponential(options?: BackoffOptions): BackoffStrategy;
}

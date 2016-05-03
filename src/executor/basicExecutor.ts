module Plywood {
  export interface Executor {
    (ex: Expression, env?: Environment): Q.Promise<PlywoodValue>;
  }

  export interface BasicExecutorParameters {
    datasets: Datum;
  }

  export function basicExecutorFactory(parameters: BasicExecutorParameters): Executor {
    var datasets = parameters.datasets;
    return (ex: Expression, env: Environment = {}) => {
      return ex.compute(datasets, env);
    }
  }
}

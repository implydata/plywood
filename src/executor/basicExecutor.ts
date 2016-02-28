module Plywood {
  export interface Executor {
    (ex: Expression): Q.Promise<PlywoodValue>;
  }

  export interface BasicExecutorParameters {
    datasets: Datum;
  }

  export function basicExecutorFactory(parameters: BasicExecutorParameters): Executor {
    var datasets = parameters.datasets;
    return (ex: Expression) => {
      return ex.compute(datasets);
    }
  }
}

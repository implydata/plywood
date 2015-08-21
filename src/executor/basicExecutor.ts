module Plywood {
  export interface Executor {
    (ex: Expression): Q.Promise<Dataset>;
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

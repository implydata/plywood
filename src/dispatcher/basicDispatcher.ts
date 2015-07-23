module Plywood {
  export interface Dispatcher {
    (ex: Expression): Q.Promise<NativeDataset>;
  }

  export interface BasicDispatcherParameters {
    datasets: Datum;
  }

  export function basicDispatcherFactory(parameters: BasicDispatcherParameters): Dispatcher {
    var datasets = parameters.datasets;
    return (ex: Expression) => {
      return ex.compute(datasets);
    }
  }
}

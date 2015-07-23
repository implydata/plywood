module Plywood {
  export module helper {
    export interface VerboseRequesterParameters<T> {
      requester: Requester.PlywoodRequester<T>;
      printLine?: (line: string) => void;
      preQuery?: (query: any) => void;
      onSuccess?: (data: any, time: number, query: any) => void;
      onError?: (error: Error, time: number, query: any) => void;
    }

    export function verboseRequesterFactory<T>(parameters: VerboseRequesterParameters<T>): Requester.PlywoodRequester<any> {
      var requester = parameters.requester;

      var printLine = parameters.printLine || ((line: string): void => {
          console.log(line);
        });

      var preQuery = parameters.preQuery || ((query: any): void => {
          printLine("vvvvvvvvvvvvvvvvvvvvvvvvvv");
          printLine("Sending query:");
          printLine(JSON.stringify(query, null, 2));
          printLine("^^^^^^^^^^^^^^^^^^^^^^^^^^");
        });

      var onSuccess = parameters.onSuccess || ((data: any, time: number, query: any): void => {
          printLine("vvvvvvvvvvvvvvvvvvvvvvvvvv");
          printLine(`Got result: (in ${time}ms)`);
          printLine(JSON.stringify(data, null, 2));
          printLine("^^^^^^^^^^^^^^^^^^^^^^^^^^");
        });

      var onError = parameters.onError || ((error: Error, time: number, query: any): void => {
          printLine("vvvvvvvvvvvvvvvvvvvvvvvvvv");
          printLine(`Got error: ${error.message} (in ${time}ms)`);
          printLine("^^^^^^^^^^^^^^^^^^^^^^^^^^");
        });

      return (request: Requester.DatabaseRequest<any>): Q.Promise<any> => {
        preQuery(request.query);
        var startTime = Date.now();
        return requester(request)
          .then(data => {
            onSuccess(data, Date.now() - startTime, request.query);
            return data;
          }, (error) => {
            onError(error, Date.now() - startTime, request.query);
            throw error;
          });
      }
    }
  }
}

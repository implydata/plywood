module Plywood {
  export module helper {

    export function promiseWhile(condition: () => boolean, action: () => Q.Promise<any>): Q.Promise<any> {
      var resolver = <Q.Deferred<any>>Q.defer();

      var loop = (): Q.Promise<any> => {
        if (!condition()) {
          resolver.resolve();
          return;
        }
        return Q(action())
          .then(loop)
          .catch(function (e) {
            resolver.reject(e);
          });
      };

      Q(null).then(loop);

      return resolver.promise;
    }

  }
}

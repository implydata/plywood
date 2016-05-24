module Plywood {
  export module helper {

    export function promiseWhile(condition: () => boolean, action: () => Q.Promise<any>): Q.Promise<any> {
      var loop = (): Q.Promise<any> => {
        if (!condition()) return Q(null);
        return Q(action()).then(loop)
      };

      return Q(null).then(loop);
    }

  }
}

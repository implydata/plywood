declare module Requester {
  interface DatabaseRequest<T> {
    query: T;
    context?: { [key: string]: any };
  }

  interface PlywoodRequester<T> {
    (request: DatabaseRequest<T>): Q.Promise<any>;
  }
}

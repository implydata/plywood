declare module Locator {
  interface Location {
    hostname: string;
    port?: number;
  }

  interface PlywoodLocator {
    (): Q.Promise<Location>;

    // Event emitter extension
    addListener?(event: string, listener: Function): any;
    on?(event: string, listener: Function): any;
    once?(event: string, listener: Function): any;
    removeListener?(event: string, listener: Function): any;
    removeAllListeners?(event?: string): any;
    setMaxListeners?(n: number): void;
    listeners?(event: string): Function[];
    emit?(event: string, ...args: any[]): boolean;
  }
}

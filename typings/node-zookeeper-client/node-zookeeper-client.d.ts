// Type definitions for Async 0.1.23
// Project: https://github.com/alexguan/node-zookeeper-client
// Definitions by: Vadim Ogievetsky <https://github.com/borisyankov/>
// Definitions: https://github.com/borisyankov/DefinitelyTyped

/// <reference path="../node/node.d.ts" />

declare module "node-zookeeper-client" {
  import events = require("events");

  export interface ClientOptions {
    sessionTimeout: number;
    spinDelay: number;
    retries: number;
  }

  export class ACL {
    public permission: number
  }

  export class ACLS {
    static OPEN_ACL_UNSAFE: ACL[];
    static CREATOR_ALL_ACL: ACL[];
    static READ_ACL_UNSAFE: ACL[];
  }

  export interface Stat {

  }

  export class CreateMode {
    static PERSISTENT: number;
    static PERSISTENT_SEQUENTIAL: number;
    static EPHEMERAL: number;
    static EPHEMERAL_SEQUENTIA: number;
  }

  export interface Watcher {
    (event: any): void;
  }

  export interface Exception extends Error {
    getCode(): number;
    getPath(): string;
    getName(): string;
    toString(): string;
  }

  export interface ExceptionStatic {
    OK: number;
    SYSTEM_ERROR: number;
    RUNTIME_INCONSISTENCY: number;
    DATA_INCONSISTENCY: number;
    CONNECTION_LOSS: number;
    MARSHALLING_ERROR: number;
    UNIMPLEMENTED: number;
    OPERATION_TIMEOUT: number;
    BAD_ARGUMENTS: number;
    API_ERROR: number;
    NO_NODE: number;
    NO_AUTH: number;
    BAD_VERSION: number;
    NO_CHILDREN_FOR_EPHEMERALS: number;
    NODE_EXISTS: number;
    NOT_EMPTY: number;
    SESSION_EXPIRED: number;
    INVALID_CALLBACK: number;
    INVALID_ACL: number;
    AUTH_FAILED: number;
  }

  export var Exception: ExceptionStatic;

  export interface BasicCallback {
    (error: Exception): void;
  }

  export interface PathCallback {
    (error: Exception, path: string): void;
  }

  export interface StatCallback {
    (error: Exception, stat: Stat): void;
  }

  export interface ChildrenCallback {
    (error: Exception, children: string[], stat: Stat): void;
  }

  export interface DataCallback {
    (error: Exception, data: Buffer, stat: Stat): void;
  }

  export interface ACLCallback {
    (error: Exception, acls: ACL[], stat: Stat): void;
  }

  export interface ResultsCallback {
    (error: Exception, results: any): void;
  }

  export class State {
    static CONNECTED: number;
    static CONNECTED_READ_ONLY: number;
    static DISCONNECTED: number;
    static EXPIRED: number;
    static AUTH_FAILED: number;
  }

  export interface Client extends events.EventEmitter {
    connect(): void;
    close(): void;

    create(path: string, callback: PathCallback): void;
    create(path: string, data: Buffer, callback: PathCallback): void;
    create(path: string, acls: ACL[], callback: PathCallback): void;
    create(path: string, mode: CreateMode, callback: PathCallback): void;
    create(path: string, acls: ACL[], mode: CreateMode, callback: PathCallback): void;
    create(path: string, data: Buffer, mode: CreateMode, callback: PathCallback): void;
    create(path: string, data: Buffer, acls: ACL[], callback: PathCallback): void;
    create(path: string, data: Buffer, acls: ACL[], mode: CreateMode, callback: PathCallback): void;

    remove(path: string, callback: BasicCallback): void;
    remove(path: string, version: number, callback: BasicCallback): void;

    exists(path: string, callback: StatCallback): void;
    exists(path: string, watcher: Watcher, callback: StatCallback): void;

    getChildren(path: string, callback: ChildrenCallback): void;
    getChildren(path: string, watcher: Watcher, callback: ChildrenCallback): void;

    getData(path: string, callback: DataCallback): void;
    getData(path: string, watcher: Watcher, callback: DataCallback): void;

    setData(path: string, data: Buffer, callback: StatCallback): void;
    setData(path: string, data: Buffer, version: number, callback: StatCallback): void;

    getACL(path: string, callback: ACLCallback): void;

    setACL(path: string, acls: ACL[], callback: StatCallback): void;
    setACL(path: string, acls: ACL[], version: number, callback: StatCallback): void;

    transaction(): Transaction;

    mkdirp(path: string, callback: PathCallback): void;
    mkdirp(path: string, data: Buffer, callback: PathCallback): void;
    mkdirp(path: string, acls: ACL[], callback: PathCallback): void;
    mkdirp(path: string, mode: CreateMode, callback: PathCallback): void;
    mkdirp(path: string, acls: ACL[], mode: CreateMode, callback: PathCallback): void;
    mkdirp(path: string, data: Buffer, mode: CreateMode, callback: PathCallback): void;
    mkdirp(path: string, data: Buffer, acls: ACL[], callback: PathCallback): void;
    mkdirp(path: string, data: Buffer, acls: ACL[], mode: CreateMode, callback: PathCallback): void;

    addAuthInfo(scheme: string, auth: Buffer): void;
    getState(): State;
    getSessionId(): Buffer;
    getSessionPassword(): Buffer;
    getSessionTimeout(): number;
    getType(): number;
    getName(): string;
    getPath(): number;
    toString(): string;
  }

  export interface Transaction {
    create(path: string, data?: Buffer, acls?: ACL[], mode?: CreateMode): Transaction;
    setData(path: string, data: Buffer, version?: number): Transaction;
    check(path: string, version?: number): Transaction;
    remove(path: string, data: Buffer, version: number): Transaction;
    commit(callback: ResultsCallback): void;
  }

  export function createClient(connectionString: string, options?: ClientOptions): Client;
}

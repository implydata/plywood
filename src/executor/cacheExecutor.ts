module Plywood {
  interface LRUCacheParameters {
    name?: string;
    keepFor?: number;
    currentTime?: () => number;
  }

  class LRUCache<T> {
    public name: string;
    public keepFor: number;
    public currentTime: () => number;
    public store: Lookup<{time: number; value: T}>;
    public size: number;

    constructor(parameters: LRUCacheParameters) {
      this.name = parameters.name || "nameless";
      this.keepFor = parameters.keepFor || (30 * 60 * 1000);
      this.currentTime = parameters.currentTime || (() => Date.now());
      if (typeof this.keepFor !== "number") {
        throw new TypeError("keepFor must be a number");
      }
      if (this.keepFor < 5 * 60 * 1000) {
        throw new Error("must keep for at least 5 minutes");
      }
      this.clear();
    }

    public clear() {
      this.store = {};
      this.size = 0;
    }

    public tidy() {
      var trimBefore = this.currentTime() - this.keepFor;
      var store = this.store;
      for (var hash in store) {
        var slot = store[hash];
        if (trimBefore <= slot.time) continue;
        delete store[hash];
        this.size--;
      }
    }

    public get(hash: string): T {
      if (hasOwnProperty(this.store, hash)) {
        var storeSlot = this.store[hash];
        storeSlot.time = this.currentTime();
        return storeSlot.value;
      } else {
        return null;
      }
    }

    public set(hash: string, value: T): void {
      if (!hasOwnProperty(this.store, hash)) {
        this.size++;
      }
      this.store[hash] = {
        value: value,
        time: this.currentTime()
      };
    }

    public getOrCreate(hash: string, createFn: () => T) {
      var ret = this.get(hash);
      if (!ret) {
        ret = createFn();
        this.set(hash, ret);
      }
      return ret;
    }

    public toString() {
      return "[" + this.name + " cache, size: " + this.size + "]";
    }

    public debug() {
      console.log(this.name + " cache");
      console.log("Size: " + this.size);
      var store = this.store;
      for (var hash in store) {
        var slot = store[hash];
        console.log(hash, JSON.stringify(slot));
      }
    }
  }


  interface CacheExecutorParameters {
    executor: Executor;
    datasets: Datum;
  }

  function cacheExecutorFactory(parameters: CacheExecutorParameters) {
    var datasets = parameters.datasets;
    for (var k in datasets) {
      if (!hasOwnProperty(datasets, k)) continue;

    }

    // Break down query and see how much of it can be solved
    //

    return (ex: Expression) => {
      throw 'to do'
    }
  }
}

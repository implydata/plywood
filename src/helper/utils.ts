module Plywood {
  export module helper {

    export function parseJSON(text: string): any[] {
      text = text.trim();
      var firstChar = text[0];

      if (firstChar[0] === '[') {
        try {
          return JSON.parse(text);
        } catch (e) {
          throw new Error(`could not parse`);
        }

      } else if (firstChar[0] === '{') { // Also support line json
        return text.split(/\r?\n/).map((line, i) => {
          try {
            return JSON.parse(line);
          } catch (e) {
            throw new Error(`problem in line: ${i}: '${line}'`);
          }
        });

      } else {
        throw new Error(`Unsupported start, starts with '${firstChar[0]}'`);

      }
    }

    export function find<T>(array: T[], fn: (value: T, index: int, array: T[]) => boolean): T {
      for (let i = 0, n = array.length; i < n; i++) {
        let a = array[i];
        if (fn.call(array, a, i)) return a;
      }
      return null;
    }

    export function findIndex<T>(array: T[], fn: (value: T, index: int, array: T[]) => boolean): int {
      for (let i = 0, n = array.length; i < n; i++) {
        let a = array[i];
        if (fn.call(array, a, i)) return i;
      }
      return -1;
    }

    export interface Nameable {
      name: string;
    }

    export function findByName<T extends Nameable>(array: T[], name: string): T {
      return find(array, (x) => x.name === name);
    }

    export function findIndexByName<T extends Nameable>(array: T[], name: string): int {
      return findIndex(array, (x) => x.name === name);
    }

    export function overrideByName<T extends Nameable>(things: T[], thingOverrides: T[]): T[] {
      things = things.slice();
      for (var thingOverride of thingOverrides) {
        var idx = findIndexByName(things, thingOverride.name);
        if (idx === -1) {
          things.push(thingOverride);
        } else {
          things[idx] = thingOverride;
        }
      }
      return things;
    }

  }
}

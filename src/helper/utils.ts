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

  }
}

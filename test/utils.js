/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2020 Imply Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const { expect } = require('chai');

const { Expression, toJS } = require('../build/plywood');

const hasOwnProperty = Object.prototype.hasOwnProperty;

const uniformizeDoubles = v => {
  const t = typeof v;
  if (t === 'number') {
    if (v !== Math.floor(v)) {
      return Number(v.toPrecision(4));
    } else {
      return v;
    }
  } else if (t === 'object') {
    if (!v) {
      // null
      return v;
    } else if (Array.isArray(v)) {
      return v.map(uniformizeDoubles);
    } else if (v.toISOString) {
      return v;
    } else {
      let needNew = false;
      const newV = {};
      for (const k in v) {
        if (!hasOwnProperty.call(v, k)) continue;
        const oldValue = v[k];
        const newValue = uniformizeDoubles(oldValue);
        newV[k] = newValue;
        if (newValue !== oldValue) needNew = true;
      }
      return needNew ? newV : v;
    }
  } else {
    return v;
  }
};

exports.wrapVerbose = (requester, name) => {
  return request => {
    console.log(`Requesting ${name}:`);
    console.log('', JSON.stringify(request.query, null, 2));
    const startTime = Date.now();
    return requester(request).then(
      result => {
        console.log(`GOT RESULT FROM ${name} (took ${Date.now() - startTime}ms)`);
        return result;
      },
      err => {
        console.log(`GOT ${name} ERROR`, err);
        throw err;
      },
    );
  };
};

exports.makeEqualityTest = executorMap => {
  return ({ executorNames, expression, verbose, before, after }) => {
    if (executorNames.length < 2) {
      throw new Error('must have at least two executorNames');
    }

    if (typeof expression === 'string') {
      expression = Expression.parse(expression);
    }

    const executors = executorNames.map(executorName => {
      const executor = executorMap[executorName];
      if (!executor) throw new Error(`no such executor ${executorName}`);
      return executor;
    });

    return () => {
      if (typeof before === 'function') before();

      return Promise.all(executors.map(executor => executor(expression))).then(
        results => {
          if (typeof after === 'function') after(null, results[0], results);

          results = results.map(result => {
            return uniformizeDoubles(toJS(result));
          });

          if (verbose) {
            console.log('vvvvvvvvvvvvvvvvvvvvvvv');
            console.log(`From ${executorNames[0]} I got:`);
            console.log(JSON.stringify(results[0], null, 2));
            console.log('^^^^^^^^^^^^^^^^^^^^^^^');
          }

          for (let i = 1; i < executorNames.length; i++) {
            expect(results[i]).to.deep.equal(
              results[0],
              `results of '${executorNames[0]}' (expected) and '${executorNames[i]}' (actual) must match`,
            );
          }

          return results[0];
        },
        err => {
          if (typeof after === 'function') {
            after(err);
          }
          console.log('got error from executor');
          console.log(err);
          throw err;
        },
      );
    };
  };
};

// To be used as a tag
exports.sane = function () {
  const str = String.raw.apply(String, arguments);

  const match = str.match(/^\n( *)/m);
  if (!match) throw new Error('sane string must start with a \\n is:' + str);
  const spaces = match[1].length;

  let lines = str.split('\n');
  lines.shift(); // Remove the first empty lines
  lines = lines.map(line => line.substr(spaces)); // Remove indentation
  if (lines[lines.length - 1] === '') lines.pop(); // Remove last line if empty

  return lines
    .join('\n')
    .replace(/\\`/g, '`') // Fix \` that should be `
    .replace(/\\\{/g, '{') // Fix \{ that should be {
    .replace(/\\\\/g, '\\'); // Fix \\ that should be \
};

exports.grabConsoleWarn = function (fn) {
  const originalConsoleWarn = console.warn;
  let text = null;
  console.warn = function (str) {
    text = (text || '') + str + '\n';
  };
  fn();
  console.warn = originalConsoleWarn;
  return text;
};

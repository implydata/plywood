var { expect } = require("chai");
var Q = require('q');

var { Expression, toJS } = require('../build/plywood');

var hasOwnProperty = Object.prototype.hasOwnProperty;

var uniformizeDoubles = (v) => {
  var t = typeof v;
  if (t === 'number') {
    if (v !== Math.floor(v)) {
      return Number(v.toPrecision(4));
    } else {
      return v;
    }
  } else if (t === 'object') {
    if (!v) { // null
      return v;
    } else if (Array.isArray(v)) {
      return v.map(uniformizeDoubles);
    } else if (v.toISOString) {
      return v;
    } else {
      var needNew = false;
      var newV = {};
      for (var k in v) {
        if (!hasOwnProperty.call(v, k)) continue;
        var oldValue = v[k];
        var newValue = uniformizeDoubles(oldValue);
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
  return (request) => {
    console.log(`Requesting ${name}:`);
    console.log('', JSON.stringify(request.query, null, 2));
    var startTime = Date.now();
    return requester(request).then(
      (result) => {
        console.log(`GOT RESULT FROM ${name} (took ${Date.now() - startTime}ms)`);
        return result;
      },
      (err) => {
        console.log(`GOT ${name} ERROR`, err);
        throw err;
      }
    );
  };
};

exports.makeEqualityTest = (executorMap) => {
  return ({executorNames, expression, sql, verbose, before, after}) => {
    if (executorNames.length < 2) {
      throw new Error("must have at least two executorNames");
    }

    if (expression && sql) throw new Error("can not set 'expression' and 'sql' at the same time");

    if (typeof expression === 'string') {
      expression = Expression.parse(expression);
    }

    if (typeof sql === 'string') {
      expression = Expression.parseSQL(sql).expression;
    }

    var executors = executorNames.map((executorName) => {
      var executor = executorMap[executorName];
      if (!executor) throw new Error(`no such executor ${executorName}`);
      return executor;
    });

    return (testComplete) => {
      if (typeof before === "function") before();

      return Q.all(executors.map((executor) => executor(expression)))
        .then((results) => {
          if (typeof after === "function") after(null, results[0], results);

          results = results.map((result) => {
            return uniformizeDoubles(toJS(result));
          });

          if (verbose) {
            console.log('vvvvvvvvvvvvvvvvvvvvvvv');
            console.log(`From ${executorNames[0]} I got:`);
            console.log(JSON.stringify(results[0], null, 2));
            console.log('^^^^^^^^^^^^^^^^^^^^^^^');
          }

          for (var i = 1; i < executorNames.length; i++) {
            expect(results[i]).to.deep.equal(results[0], `results of '${executorNames[0]}' (expected) and '${executorNames[i]}' (actual) must match`);
          }

          testComplete(null, results[0]);
        },
        (err) => {
          if (typeof after === "function") {
            after(err);
          }
          console.log("got error from executor");
          console.log(err);
          throw err;
        })
        .done();
    };
  };
};

// To be used as a tag
exports.sane = function() {
  var str = String.raw.apply(String, arguments);

  var match = str.match(/^\n( *)/m);
  if (!match) throw new Error('sane string must start with a \\n is:' + str);
  var spaces = match[1].length;

  var lines = str.split('\n');
  lines.shift(); // Remove the first empty lines
  lines = lines.map((line) => line.substr(spaces)); // Remove indentation
  if (lines[lines.length - 1] === '') lines.pop(); // Remove last line if empty

  return lines.join('\n')
    .replace(/\\`/g, '`')    // Fix \` that should be `
    .replace(/\\\{/g, '{')   // Fix \{ that should be {
    .replace(/\\\\/g, '\\'); // Fix \\ that should be \
};


exports.grabConsoleWarn = function(fn) {
  var originalConsoleWarn = console.warn;
  var text = null;
  console.warn = function(str) {
    text = (text || '') + str + '\n';
  };
  fn();
  console.warn = originalConsoleWarn;
  return text;
};

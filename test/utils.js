var { expect } = require("chai");
var Q = require('q');

var plywood = require('../build/plywood');

var uniformizeResults = (result) => {
  if (!((typeof result !== "undefined" && result !== null) ? result.prop : undefined)) {
    return result;
  }

  var ret = {};
  for (var k in result) {
    var p = result[k];
    if (!result.hasOwnProperty(k)) {
      continue;
    }
    if (k === 'split') {
      continue;
    }
    if (k === 'prop') {
      var propNames = [];
      for (var name in p) {
        var value = p[name];
        propNames.push(name);
      }
      propNames.sort();

      var prop = {};
      for (var i = 0, name; i < propNames.length; i++) {
        name = propNames[i];
        value = p[name];
        if (!p.hasOwnProperty(name)) {
          continue;
        }
        if (typeof value === 'number' && value !== Math.floor(value)) {
          prop[name] = Number(value.toPrecision(5));
        } else if (Array.isArray(value) && typeof value[0] === 'number' && typeof value[1] === 'number' && (value[0] !== Math.floor(value[0]) || value[1] !== Math.floor(value[1]))) {
          prop[name] = [value[0].toFixed(3), value[1].toFixed(3)];
        } else {
          prop[name] = value;
        }
      }
      p = prop;
    }

    ret[k] = p;
  }

  if (result.splits) {
    ret.splits = result.splits.map(uniformizeResults);
  }

  if (result.loading) {
    ret.loading = true;
  }

  return ret;
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
  return ({executorNames, expression, verbose, before, after}) => {
    if (executorNames.length < 2) {
      throw new Error("must have at least two executorNames");
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
            //return uniformizeResults(result.toJS());
            return result.toJS();
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

  return lines.join('\n').replace(/\\`/g, '`'); // Fix \` that should be `
};

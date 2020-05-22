/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2016 Imply Data, Inc.
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

var fs = require('fs');
var peg = require("pegjs");

function mkParser(pegjsFilename, outputFilename) {
  var grammar = fs.readFileSync(pegjsFilename, 'utf8');

  try {
    var parserSrc = peg.generate(grammar, {
      format: 'bare',
      output: 'source',
      optimize: "speed" // or "size"
    });
  } catch (e) {
    console.error(e);
    process.exit(1);
  }

  parserSrc = 'module.exports =\n' + parserSrc.replace("\n(function() {\n", "\nfunction(plywood, chronoshift) {\n").replace("\n})()", "\n}");

  fs.writeFileSync(outputFilename, parserSrc, 'utf8');
}

// Expressions

mkParser('./src/expressions/expression.pegjs', './build/expressionParser.js');

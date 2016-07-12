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
var PEG = require("pegjs");

var prefixToRemove = '(function() {\n  "use strict";\n';
var postfixToRemove = '})()';

// Expressions

var expressionGrammarFilename = './src/expressions/expression.pegjs';
var expressionGrammar = fs.readFileSync(expressionGrammarFilename, 'utf8');

try {
  var expressionParser = PEG.buildParser(expressionGrammar, {
    output: 'source',
    optimize: "speed" // or "size"
  });
} catch (e) {
  console.error(e);
  process.exit(1);
}

expressionParser = expressionParser.substring(prefixToRemove.length, expressionParser.length - postfixToRemove.length);

expressionParser = '"use strict";\nmodule.exports = function(plywood, chronoshift) {' + expressionParser + '};\n';

fs.writeFileSync('./build/expressionParser.js', expressionParser, 'utf8');

// SQL

var plyqlGrammarFilename = './src/expressions/plyql.pegjs';
var plyqlGrammar = fs.readFileSync(plyqlGrammarFilename, 'utf8');

try {
  var plyqlParser = PEG.buildParser(plyqlGrammar, {
    output: 'source',
    optimize: "speed" // or "size"
  });
} catch (e) {
  console.error(e);
  process.exit(1);
}

plyqlParser = plyqlParser.substring(prefixToRemove.length, plyqlParser.length - postfixToRemove.length);

plyqlParser = '"use strict";\nmodule.exports = function(plywood, chronoshift) {' + plyqlParser + '};\n';

fs.writeFileSync('./build/plyqlParser.js', plyqlParser, 'utf8');

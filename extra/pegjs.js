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

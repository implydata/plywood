var fs = require('fs');
var PEG = require("pegjs");

var prefixToRemove = '(function() {';
var postfixToRemove = '})()';

// Expressions

var expressionGrammarFilename = './src/expressions/expression.pegjs';
var expressionGrammar = fs.readFileSync(expressionGrammarFilename, 'utf8');

var expressionParser = PEG.buildParser(expressionGrammar, {
  output: 'source',
  optimize: "speed" // or "size"
});

expressionParser = expressionParser.substring(prefixToRemove.length, expressionParser.length - postfixToRemove.length);

expressionParser = 'module.exports = function(plywood) {' + expressionParser + '};\n';

fs.writeFileSync('./parser/expression.js', expressionParser, 'utf8');

// SQL

var plyqlGrammarFilename = './src/expressions/plyql.pegjs';
var plyqlGrammar = fs.readFileSync(plyqlGrammarFilename, 'utf8');

var plyqlParser = PEG.buildParser(plyqlGrammar, {
  output: 'source',
  optimize: "speed" // or "size"
});

plyqlParser = plyqlParser.substring(prefixToRemove.length, plyqlParser.length - postfixToRemove.length);

plyqlParser = 'module.exports = function(plywood) {' + plyqlParser + '};\n';

fs.writeFileSync('./parser/plyql.js', plyqlParser, 'utf8');

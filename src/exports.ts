expressionParser = (<PEGParserFactory>require("../parser/expression"))(Plywood);
plyqlParser = (<PEGParserFactory>require("../parser/plyql"))(Plywood);

if (typeof module !== 'undefined' && typeof module.exports !== 'undefined') {
  module.exports = Plywood;

  // Make Chronoshift available outside of Plywood as well
  module.exports.Chronoshift = Chronoshift;
}

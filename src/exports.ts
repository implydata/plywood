expressionParser = (<PEGParserFactory>require("./expressionParser"))(Plywood);
plyqlParser = (<PEGParserFactory>require("./plyqlParser"))(Plywood);

if (typeof module !== 'undefined' && typeof module.exports !== 'undefined') {
  module.exports = Plywood;

  // Make Chronoshift available outside of Plywood as well
  module.exports.Chronoshift = Chronoshift;
}

expressionParser = (<PEGParserFactory>require("./expressionParser"))(Plywood, Chronoshift);
plyqlParser = (<PEGParserFactory>require("./plyqlParser"))(Plywood, Chronoshift);

if (typeof module !== 'undefined' && typeof module.exports !== 'undefined') {
  module.exports = Plywood;

  // Make Chronoshift available outside of Plywood as well
  module.exports.Chronoshift = Chronoshift;
}

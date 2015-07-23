var $ = plywood.$;
var Expression = plywood.Expression;
var Dataset = plywood.Dataset;

var inputField = d3.select('#input');
var outputField = d3.select('#output');
var errorField = d3.select('#error');

var defaultQuery = [
  "$()",
  "  .def('diamonds',",
  "    $('diamonds').filter($('time').in({",
  "      start: new Date('2015-03-12T00:00:00'),",
  "      end: new Date('2015-03-19T00:00:00')",
  "    }))",
  "  )",
  "  .apply('Count', '$diamonds.count()')",
  "  .apply('TotalPrice', '$diamonds.sum($price)')",
  "  .apply('PriceAndTax', '$diamonds.sum($price) + $diamonds.sum($tax)')",
  "  .apply('PriceGoodCut', '$diamonds.filter($cut = \"good\").sum($price)')",
  "  .apply('Cuts',",
  "    $('diamonds').split('$cut', 'Cut')",
  "      .apply('Count', $('diamonds').count())",
  "      .sort('$Count', 'descending')",
  "      .limit(2)",
  "  )"
].join('\n');

try {
  var ev = eval(localStorage['query']);
  if (Expression.isExpression(ev)) {
    defaultQuery = localStorage['query'];
  }
} catch (e) {}

inputField.property("value", defaultQuery);

function saveToLocal() {
  localStorage['query'] = d3.select("#input").property("value");
}

context = {
  diamonds: Dataset.fromJS({
    source: 'druid',
    dataSource: 'diamonds',
    timeAttribute: 'time',
    allowEternity: true,
    allowSelectQueries: true,
    context: null,
    attributes: {
      time: { type: 'TIME' },
      color: { type: 'STRING' },
      cut: { type: 'STRING' },
      tags: { type: 'SET/STRING' },
      carat: { type: 'NUMBER' },
      height_bucket: { special: 'range', separator: ';', rangeSize: 0.05, digitsAfterDecimal: 2 },
      price: { type: 'NUMBER', filterable: false, splitable: false },
      tax: { type: 'NUMBER', filterable: false, splitable: false },
      unique_views: { special: 'unique', filterable: false, splitable: false }
    }
  })
};

function error(str) {
  if (str) {
    errorField.style('display', 'block').text(str);
  } else {
    errorField.style('display', null);
  }
}

function simulate() {
  var input = inputField.property("value");

  try {
    input = eval(input);
    if (!Expression.isExpression(input)) {
      throw new Error("not an expression");
    }
  } catch (e) {
    error("Could not parse as JS Expression");
    return;
  }

  saveToLocal();

  try {
    var res = input.simulateQueryPlan(context);
  } catch (e) {
    error(e.message);
    return;
  }

  var text = res
    .map(function(query) {
      if (typeof query === 'string') {
        return query
      } else {
        return JSON.stringify(query, null, 2);
      }
    })
    .join("\n\n// ---------------------\n\n") + "\n\n\n";

  error(null);
  outputField.text(text);
}

inputField.on('keyup', simulate);
inputField.on('change', simulate);
simulate();

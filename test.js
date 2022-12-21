const plywood = require('./build/plywood')
var ply = plywood.ply;
var $ = plywood.$;

var External = plywood.External;
var druidRequesterFactory = require('plywood-druid-requester').druidRequesterFactory;

const {
  verboseRequesterFactory,
} = plywood;

var druidRequester = druidRequesterFactory({
  host: 'localhost:8082' // Where ever your Druid may be
});

druidRequester = verboseRequesterFactory({
  requester: druidRequester,
});

var wikiDataset = External.fromJS({
  engine: 'druidsql',
  allowSelectQueries: true,
  allowEternity: true,
  source: 'ip_addresses',  // The datasource name in Druid
  timeAttribute: '__time',  // Druid's anonymous time attribute will be called 'time',
  context: {
    timeout: 3000 // The Druid context
  }
}, druidRequester);

var context = {
  wiki: wikiDataset,
  seventy: 70
};

var ex = ply()
  .apply("wiki", $('wiki').filter($("__time").in({
    start: new Date("2015-09-12T00:00:00Z"),
    end: new Date("2023-09-13T00:00:00Z")
  })))
  .apply(
    'columns',
    $('wiki')
      .split($('ip'), 'IP')
      .split($('IP').ipMatch('192.0.1.2', 'ipAddress'), 'DoesMatch')
      .apply('Count', $('wiki').count())
      .sort('$Count', 'descending')
      .limit(10)
  )

ex.compute(context).then(function(data) {
  // Log the data while converting it to a readable standard
  // console.log(JSON.stringify(data.toJS(), null, 2));
}).finally()

// ex.simulateQueryPlan(context).then(function(data) {
//   // Log the data while converting it to a readable standard
//   console.log(JSON.stringify(data.toJS(), null, 2));
// }).finally()

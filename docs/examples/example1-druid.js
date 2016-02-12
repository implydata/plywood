var druidRequesterFactory = require('plywood-druid-requester').druidRequesterFactory;
var plywood = require('../../build/plywood');
var ply = plywood.ply;
var $ = plywood.$;
var External = plywood.External;

var druidRequester = druidRequesterFactory({
  host: 'localhost:8082' // Where ever your Druid may be
});

// ----------------------------------

var context = {
  wiki: External.fromJS({
    engine: 'druid',
    dataSource: 'wikipedia',
    timeAttribute: 'time',
    requester: druidRequester
  })
};

var ex = ply()
  .apply("wiki",
    $('wiki').filter($("time").in({
      start: new Date("2015-08-26T00:00:00Z"),
      end: new Date("2015-08-27T00:00:00Z")
    }).and($('language').is('en')))
  )
  .apply('Count', $('wiki').count())
  .apply('TotalAdded', '$wiki.sum($added)');

ex.compute(context)
  .then(function(data) {
    // Log the data while converting it to a readable standard
    console.log(JSON.stringify(data.toJS(), null, 2));
  })
  .done();

// ----------------------------------

/*
Output:
[
  {
    "TotalAdded": 50493721,
    "Count": 127007
  }
]
*/

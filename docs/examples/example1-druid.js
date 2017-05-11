let druidRequesterFactory = require('plywood-druid-requester').druidRequesterFactory;
let plywood = require('../../build/plywood');
let ply = plywood.ply;
let $ = plywood.$;
let External = plywood.External;

let druidRequester = druidRequesterFactory({
  host: 'localhost:8082' // Where ever your Druid may be
});

// ----------------------------------

let context = {
  wiki: External.fromJS({
    engine: 'druid',
    source: 'wikipedia',
    timeAttribute: 'time',
    requester: druidRequester
  })
};

let ex = ply()
  .apply("wiki",
    $('wiki').filter($("time").overlap({
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
  });

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

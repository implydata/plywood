let druidRequesterFactory = require('plywood-druid-requester').druidRequesterFactory;
let plywood = require('../../build/plywood');
let ply = plywood.ply;
let $ = plywood.$;
let External = plywood.External;
let verboseRequesterFactory = plywood.verboseRequesterFactory;

// Let's add a request re-writer / decorator
let fancyRequestDecorator = ({ method, url, query }) => {
  if (method === 'POST' && query) {
    query.superDuperToken = '555';
  }
  return {
    url: url + '?principalId/3246325435',
    query
  }
};

let druidRequester = druidRequesterFactory({
  host: 'your-druid-host:8082', // Where ever your Druid may be
  requestDecorator: fancyRequestDecorator
});

druidRequester = verboseRequesterFactory({
  requester: druidRequester
});

// ----------------------------------

let context = {
  wiki: External.fromJS({
    engine: 'druid',
    source: 'wikipedia',  // The datasource name in Druid
    filter: $("__time").overlap({ start: new Date("2015-09-12T00:00:00Z"), end: new Date("2015-09-13T00:00:00Z") }),
    requester: druidRequester,
    exactResultsOnly: true // force groupBys
  })
};

let ex = $('wiki')
  .filter('$countryName == "United States"')
  .split('$channel', 'Language')
    .apply('Edits', '$wiki.count()')
    .sort('$Edits', 'descending')
    .limit(5);

ex.compute(context)
  .then(function(data) {
    // Log the data while converting it to a readable standard
    console.log(JSON.stringify(data.toJS(), null, 2));
  })
  .catch(function(e) {
    console.log('Error', e)
  });

// ----------------------------------

/*
{
  "keys": [
    "Language"
  ],
  "attributes": [
    {
      "name": "Language",
      "type": "STRING"
    },
    {
      "name": "Edits",
      "type": "NUMBER"
    }
  ],
  "data": [
    {
      "Language": "en",
      "Edits": 4876
    },
    {
      "Language": "fr",
      "Edits": 68
    },
    {
      "Language": "zh",
      "Edits": 44
    },
    {
      "Language": "simple",
      "Edits": 40
    },
    {
      "Language": "es",
      "Edits": 26
    }
  ]
}
*/

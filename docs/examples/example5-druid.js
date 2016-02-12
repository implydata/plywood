var druidRequesterFactory = require('plywood-druid-requester').druidRequesterFactory;
var plywood = require('../../build/plywood');
var ply = plywood.ply;
var $ = plywood.$;
var External = plywood.External;
var helper = plywood.helper;

WallTime = require('chronoshift').WallTime;
if (!WallTime.rules) {
  tzData = require("chronoshift/lib/walltime/walltime-data.js");
  WallTime.init(tzData.rules, tzData.zones);
}

var druidRequester = druidRequesterFactory({
  host: 'localhost:8082' // Where ever your Druid may be
});

druidRequester = helper.verboseRequesterFactory({
  requester: druidRequester
});

// ----------------------------------

var context = {
  wiki: External.fromJS({
    engine: 'druid',
    dataSource: 'wikipedia',  // The datasource name in Druid
    timeAttribute: 'time',  // Druid's anonymous time attribute will be called 'time'
    filter: $("time").in({ start: new Date("2015-09-01T00:00:00Z"), end: new Date("2015-11-01T00:00:00Z") }),
    requester: druidRequester
  })
};

var ex = $('wiki')
  .filter('$region != null and $country == "United States"')
  .split('$region', 'State')
    .apply('Edits', '$wiki.count()')
    .sort('$Edits', 'descending')
    .limit(5)
    .apply('DaysOfWeek',
      $('wiki').split($("time").timePart('DAY_OF_WEEK', 'America/New_York'), 'DayOfWeek')
        .apply('Edits', '$wiki.count()')
        .sort('$DayOfWeek', 'ascending')
    );

ex.compute(context)
  .then(function(data) {
    // Log the data while converting it to a readable standard
    console.log(JSON.stringify(data.toJS(), null, 2));
  })
  .done();

// ----------------------------------

/*
[
  {
    "State": "California",
    "Edits": 40086,
    "DaysOfWeek": [
      { "DayOfWeek": 0, "Edits": 5438 },
      { "DayOfWeek": 1, "Edits": 6513 },
      { "DayOfWeek": 2, "Edits": 6642 },
      { "DayOfWeek": 3, "Edits": 6335 },
      { "DayOfWeek": 4, "Edits": 6090 },
      { "DayOfWeek": 5, "Edits": 4628 },
      { "DayOfWeek": 6, "Edits": 4440 }
    ]
  },
  {
    "State": "New York",
    "Edits": 34574,
    "DaysOfWeek": [
      { "DayOfWeek": 0, "Edits": 5057 },
      { "DayOfWeek": 1, "Edits": 5668 },
      { "DayOfWeek": 2, "Edits": 5602 },
      { "DayOfWeek": 3, "Edits": 5055 },
      { "DayOfWeek": 4, "Edits": 4268 },
      { "DayOfWeek": 5, "Edits": 4167 },
      { "DayOfWeek": 6, "Edits": 4757 }
    ]
  },
  {
    "State": "Texas",
    "Edits": 20033,
    "DaysOfWeek": [
      { "DayOfWeek": 0, "Edits": 2746 },
      { "DayOfWeek": 1, "Edits": 3215 },
      { "DayOfWeek": 2, "Edits": 3467 },
      { "DayOfWeek": 3, "Edits": 3219 },
      { "DayOfWeek": 4, "Edits": 2816 },
      { "DayOfWeek": 5, "Edits": 2371 },
      { "DayOfWeek": 6, "Edits": 2199 }
    ]
  },
  {
    "State": "Florida",
    "Edits": 16408,
    "DaysOfWeek": [
      { "DayOfWeek": 0, "Edits": 2305 },
      { "DayOfWeek": 1, "Edits": 2797 },
      { "DayOfWeek": 2, "Edits": 2793 },
      { "DayOfWeek": 3, "Edits": 2451 },
      { "DayOfWeek": 4, "Edits": 2382 },
      { "DayOfWeek": 5, "Edits": 1840 },
      { "DayOfWeek": 6, "Edits": 1840 }
    ]
  },
  {
    "State": "New Jersey",
    "Edits": 16395,
    "DaysOfWeek": [
      { "DayOfWeek": 0, "Edits": 2145 },
      { "DayOfWeek": 1, "Edits": 2554 },
      { "DayOfWeek": 2, "Edits": 2314 },
      { "DayOfWeek": 3, "Edits": 2678 },
      { "DayOfWeek": 4, "Edits": 2451 },
      { "DayOfWeek": 5, "Edits": 2054 },
      { "DayOfWeek": 6, "Edits": 2199 }
    ]
  }
]
*/

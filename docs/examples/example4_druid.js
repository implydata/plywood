var druidRequesterFactory = require('plywood-druid-requester').druidRequesterFactory;
var plywood = require('../../build/plywood');
var $ = plywood.$;
var Dataset = plywood.Dataset;

WallTime = require('chronology').WallTime;
if (!WallTime.rules) {
  tzData = require("chronology/lib/walltime/walltime-data.js");
  WallTime.init(tzData.rules, tzData.zones);
}

var druidRequester = druidRequesterFactory({
  host: '10.153.211.100' // Where ever your Druid may be
});

// ----------------------------------

var context = {
  wiki: Dataset.fromJS({
    source: 'druid',
    dataSource: 'wikipedia_editstream',  // The datasource name in Druid
    timeAttribute: 'time',  // Druid's anonymous time attribute will be called 'time'
    requester: druidRequester
  })
};

var ex = $()
  .apply("wiki",
    $('wiki').filter($("time").in({
      start: new Date("2013-02-26T00:00:00Z"),
      end: new Date("2013-02-27T00:00:00Z")
    }))
  )
  .apply('ByHour',
    $('wiki').split($("time").timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour')
      .sort('$TimeByHour', 'ascending')
      .apply('Users',
        $('wiki').split('$user', 'User')
          .apply('Count', $('wiki').count())
          .sort('$Count', 'descending')
          .limit(3)
      )
  );

ex.compute(context).then(function(data) {
  // Log the data while converting it to a readable standard
  console.log(JSON.stringify(data.toJS(), null, 2));
}).done();

// ----------------------------------

/*
Output:
[
  {
    "ByHour": [
      {
        "TimeByHour": {
          "start": "2013-02-26T00:00:00.000Z",
          "end": "2013-02-26T01:00:00.000Z",
          "type": "TIME_RANGE"
        },
        "Users": [
          {
            "User": "Addbot",
            "Count": 15419
          },
          {
            "User": "EmausBot",
            "Count": 1126
          },
          {
            "User": "MerlIwBot",
            "Count": 815
          }
        ]
      },
      {
        "TimeByHour": {
          "start": "2013-02-26T01:00:00.000Z",
          "end": "2013-02-26T02:00:00.000Z",
          "type": "TIME_RANGE"
        },
        "Users": [
          {
            "User": "Addbot",
            "Count": 20089
          },
          {
            "User": "MerlIwBot",
            "Count": 1376
          },
          {
            "User": "ValterVBot",
            "Count": 798
          }
        ]
      },
      {
        "TimeByHour": {
          "start": "2013-02-26T02:00:00.000Z",
          "end": "2013-02-26T03:00:00.000Z",
          "type": "TIME_RANGE"
        },
        "Users": [
          {
            "User": "Addbot",
            "Count": 9504
          },
          {
            "User": "MerlIwBot",
            "Count": 1706
          },
          {
            "User": "ValterVBot",
            "Count": 785
          }
        ]
      },
      {
        "TimeByHour": {
          "start": "2013-02-26T03:00:00.000Z",
          "end": "2013-02-26T04:00:00.000Z",
          "type": "TIME_RANGE"
        },
        "Users": [
          {
            "User": "Addbot",
            "Count": 6810
          },
          {
            "User": "EmausBot",
            "Count": 962
          },
          {
            "User": "ValterVBot",
            "Count": 791
          }
        ]
      },
      '...'
    ]
  }
]
*/

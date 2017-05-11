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
    source: 'wikipedia',  // The datasource name in Druid
    timeAttribute: 'time',  // Druid's anonymous time attribute will be called 'time'
    requester: druidRequester
  })
};

let ex = ply()
  .apply("wiki",
    $('wiki').filter($("time").overlap({
      start: new Date("2015-08-26T00:00:00Z"),
      end: new Date("2015-08-27T00:00:00Z")
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
    "ByHour": [
      {
        "TimeByHour": {
          "start": "2015-08-26T00:00:00.000Z",
          "end": "2015-08-26T01:00:00.000Z",
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
          "start": "2015-08-26T01:00:00.000Z",
          "end": "2015-08-26T02:00:00.000Z",
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
          "start": "2015-08-26T02:00:00.000Z",
          "end": "2015-08-26T03:00:00.000Z",
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
          "start": "2015-08-26T03:00:00.000Z",
          "end": "2015-08-26T04:00:00.000Z",
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

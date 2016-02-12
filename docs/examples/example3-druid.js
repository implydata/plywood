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
    dataSource: 'wikipedia',  // The datasource name in Druid
    timeAttribute: 'time',  // Druid's anonymous time attribute will be called 'time'
    requester: druidRequester
  })
};

var ex = ply()
  .apply("wiki",
    $('wiki').filter($("time").in({
      start: new Date("2015-08-26T00:00:00Z"),
      end: new Date("2015-08-27T00:00:00Z")
    }))
  )
  .apply('Count', $('wiki').count())
  .apply('TotalAdded', '$wiki.sum($added)')
  .apply('Pages',
    $('wiki').split('$page', 'Page')
      .apply('Count', $('wiki').count())
      .sort('$Count', 'descending')
      .limit(6)
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
  })
  .done();

// ----------------------------------

/*
Output:
[
  {
    "TotalAdded": 68403467,
    "Count": 183026,
    "Pages": [
      {
        "Page": "Murders_of_Alison_Parker_and_Adam_Ward",
        "Count": 363,
        "Users": [
          {
            "Count": 98,
            "User": "2601:5C6:8001:C7CA:7132:79C6:14B0:3036"
          },
          {
            "Count": 55,
            "User": "Versus001"
          },
          {
            "Count": 44,
            "User": "Illegitimate Barrister"
          }
        ]
      },
      {
        "Page": "Wikipedia:Administrators'_noticeboard/Incidents",
        "Count": 312,
        "Users": [
          {
            "Count": 62,
            "User": "Cebr1979"
          },
          {
            "Count": 20,
            "User": "BlueSalix"
          },
          {
            "Count": 18,
            "User": "Hijiri88"
          }
        ]
      },
      {
        "Page": "Wikipedia:Version_1.0_Editorial_Team/Psychedelics,_dissociatives_and_deliriants_articles_by_quality_log",
        "Count": 307,
        "Users": [
          {
            "Count": 307,
            "User": "WP 1.0 bot"
          }
        ]
      },
      {
        "Page": "User:Cyde/List_of_candidates_for_speedy_deletion/Subpage",
        "Count": 275,
        "Users": [
          {
            "Count": 275,
            "User": "Cydebot"
          }
        ]
      },
      {
        "Page": "Wikipedia:Administrator_intervention_against_vandalism",
        "Count": 238,
        "Users": [
          {
            "Count": 41,
            "User": "HBC AIV helperbot"
          },
          {
            "Count": 29,
            "User": "HBC AIV helperbot11"
          },
          {
            "Count": 22,
            "User": "HBC AIV helperbot5"
          }
        ]
      },
      {
        "Page": "Wikipedia:Löschkandidaten/26._August_2015",
        "Count": 238,
        "Users": [
          {
            "Count": 43,
            "User": "80.187.103.152"
          },
          {
            "Count": 15,
            "User": "Nicola"
          },
          {
            "Count": 12,
            "User": "87.153.112.226"
          }
        ]
      }
    ]
  }
]
*/

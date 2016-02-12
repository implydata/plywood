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
        "Count": 363
      },
      {
        "Page": "Wikipedia:Administrators'_noticeboard/Incidents",
        "Count": 312
      },
      {
        "Page": "Wikipedia:Version_1.0_Editorial_Team/Psychedelics,_dissociatives_and_deliriants_articles_by_quality_log",
        "Count": 307
      },
      {
        "Page": "User:Cyde/List_of_candidates_for_speedy_deletion/Subpage",
        "Count": 275
      },
      {
        "Page": "Wikipedia:Administrator_intervention_against_vandalism",
        "Count": 238
      },
      {
        "Page": "Wikipedia:Löschkandidaten/26._August_2015",
        "Count": 238
      }
    ]
  }
]
*/

var druidRequesterFactory = require('plywood-druid-requester').druidRequesterFactory;
var plywood = require('../../build/plywood');
var $ = plywood.$;
var Dataset = plywood.Dataset;

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

ex.compute(context).then(function(data) {
  // Log the data while converting it to a readable standard
  console.log(JSON.stringify(data.toJS(), null, 2));
}).done();

// ----------------------------------

/*
Output:
[
  {
    "Count": 573775,
    "TotalAdded": 124184252,
    "Pages": [
      {
        "Page": "Wikipedia:Vandalismusmeldung",
        "Count": 177,
        "Users": [
          {
            "User": "SpBot",
            "Count": 17
          },
          {
            "User": "ArchivBot",
            "Count": 12
          },
          {
            "User": "-jkb-",
            "Count": 8
          }
        ]
      },
      {
        "Page": "Wikipedia:Administrator_intervention_against_vandalism",
        "Count": 124,
        "Users": [
          {
            "User": "HBC AIV helperbot7",
            "Count": 20
          },
          {
            "User": "HBC AIV helperbot5",
            "Count": 14
          },
          {
            "User": "Seaphoto",
            "Count": 7
          }
        ]
      },
      {
        "Page": "Wikipedia:Auskunft",
        "Count": 124,
        "Users": [
          {
            "User": "Gerold Broser",
            "Count": 12
          },
          {
            "User": "Grey Geezer",
            "Count": 10
          },
          {
            "User": "Rotkaeppchen68",
            "Count": 7
          }
        ]
      },
      {
        "Page": "Wikipedia:Löschkandidaten/26._Februar_2013",
        "Count": 88,
        "Users": [
          {
            "User": "109.48.74.182",
            "Count": 7
          },
          {
            "User": "FzBot",
            "Count": 7
          },
          {
            "User": "Brainswiffer",
            "Count": 4
          }
        ]
      },
      {
        "Page": "Wikipedia:Reference_desk/Science",
        "Count": 88,
        "Users": [
          {
            "User": "StuRat",
            "Count": 6
          },
          {
            "User": "Clover345",
            "Count": 4
          },
          {
            "User": "Nimur",
            "Count": 4
          }
        ]
      },
      {
        "Page": "Wikipedia:Administrators'_noticeboard",
        "Count": 87,
        "Users": [
          {
            "User": "Delicious carbuncle",
            "Count": 4
          },
          {
            "User": "Nyttend",
            "Count": 4
          },
          {
            "User": "Retrolord",
            "Count": 4
          }
        ]
      }
    ]
  }
]
*/

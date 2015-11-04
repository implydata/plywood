var plywood = require('../../build/plywood');
var ply = plywood.ply;
var $ = plywood.$;
var External = plywood.External;

// Define an external
var context = {
  wiki: External.fromJS({
    engine: 'druid',
    dataSource: 'wikipedia',  // The datasource name in Druid
    timeAttribute: 'time',  // Druid's anonymous time attribute will be called 'time'
    attributes: [
      { name: 'time', type: 'TIME' },
      { name: 'page', type: 'STRING' },
      { name: 'added', type: 'NUMBER' }
    ]
  })
};

// Create an expression
var ex = ply()
  .apply("wiki",
    $('wiki').filter(
      $("time").in({ start: new Date("2015-08-26T00:00:00Z"), end: new Date("2015-08-27T00:00:00Z") })
        .and($('language').is('en'))
    )
  )
  .apply('Count', $('wiki').count())
  .apply('TotalAdded', '$wiki.sum($added)')
  .apply('Pages',
    $('wiki').split('$page', 'Page')
      .apply('TotalAdded', '$wiki.sum($added)')
      .sort('$Count', 'descending')
      .limit(6)
  );

// Let's see what queries this would make
console.log(ex.simulateQueryPlan(context));

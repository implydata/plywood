var plywood = require('plywood');
var ply = plywood.ply;
var $ = plywood.$;
var External = plywood.External;

// Define an external or two
var mysqlExternal = External.fromJS({
  engine: 'mysql',
  table: 'wikipedia',
  attributes: [
    { name: 'time', type: 'TIME' },
    { name: 'page', type: 'STRING' },
    { name: 'language', type: 'STRING' },
    { name: 'added', type: 'NUMBER' }
  ]
});

var druidExternal = External.fromJS({
  engine: 'druid',
  dataSource: 'wikipedia',
  timeAttribute: 'time',
  attributes: [
    { name: 'time', type: 'TIME' },
    { name: 'page', type: 'STRING' },
    { name: 'language', type: 'STRING' },
    { name: 'added', type: 'NUMBER' }
  ]
});

// Create an expression
var ex = ply()
  .apply("wiki",
    $('wiki').filter(
      $("time").in({
          start: new Date("2015-08-26T00:00:00Z"),
          end: new Date("2015-08-27T00:00:00Z")
        })
        .and($('language').in(['English', 'Spanish']))
    )
  )
  .apply('TotalAdded', '$wiki.sum($added)')
  .apply('Pages',
    $('wiki').split('$page', 'Page')
      .apply('TotalAdded', '$wiki.sum($added)')
      .sort('$TotalAdded', 'descending')
      .limit(6)
  );

// Let's see what queries this would make against these databases
console.log("The query plans are:");
console.log(ex.simulateQueryPlan({ wiki: mysqlExternal }));
console.log(ex.simulateQueryPlan({ wiki: druidExternal }));

'Fin.';

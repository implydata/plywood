let plywood = require('plywood');
let ply = plywood.ply;
let $ = plywood.$;
let External = plywood.External;

// Define an external or two
let mysqlExternal = External.fromJS({
  engine: 'mysql',
  source: 'wikipedia',
  attributes: [
    { name: 'time', type: 'TIME' },
    { name: 'page', type: 'STRING' },
    { name: 'language', type: 'STRING' },
    { name: 'added', type: 'NUMBER' }
  ]
});

let druidExternal = External.fromJS({
  engine: 'druid',
  source: 'wikipedia',
  attributes: [
    { name: 'time', type: 'TIME' },
    { name: 'page', type: 'STRING' },
    { name: 'language', type: 'STRING' },
    { name: 'added', type: 'NUMBER' }
  ]
});

// Create an expression
let ex = ply()
  .apply("wiki",
    $('wiki').filter(
      $("__time").overlap({
          start: new Date("2015-08-26T00:00:00Z"),
          end: new Date("2015-08-27T00:00:00Z")
        })
        .and($('language').is(['English', 'Spanish']))
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

var plywood = require('../../build/plywood');
var ply = plywood.ply;
var $ = plywood.$;
var Dataset = plywood.Dataset;

var diamondsData = require('../../data/diamonds.js');

// ----------------------------------

var context = {
  diamonds: Dataset.fromJS({
    data: diamondsData
  }).hide()
};

var ex = ply()
  .apply("diamonds", $('diamonds').filter($("color").is('D')))
  .apply('Count', $('diamonds').count())
  .apply('TotalPrice', '$diamonds.sum($price)');

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
    "Count": 6775,
    "TotalPrice": 21476439
  }
]
*/

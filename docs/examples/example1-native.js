let plywood = require('../../build/plywood');
let ply = plywood.ply;
let $ = plywood.$;
let Dataset = plywood.Dataset;

let diamondsData = require('../../data/diamonds.js');

// ----------------------------------

let context = {
  diamonds: Dataset.fromJS({
    data: diamondsData
  }).hide()
};

let ex = ply()
  .apply("diamonds", $('diamonds').filter($("color").is('D')))
  .apply('Count', $('diamonds').count())
  .apply('TotalPrice', '$diamonds.sum($price)');

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
    "Count": 6775,
    "TotalPrice": 21476439
  }
]
*/

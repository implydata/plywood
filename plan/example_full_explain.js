myDiamonds = Dataset.fromJS([
  { "carat": 0.23, "cut": "Ideal",   "color": "E", "clarity": "SI2", "price": 326 },
  { "carat": 0.21, "cut": "Premium", "color": "E", "clarity": "SI1", "price": 326 },
  { "carat": 0.23, "cut": "Good",    "color": "E", "clarity": "VS1", "price": 328 }
]);

$() // [{} = base]
  .apply('two', 2)
  .apply("diamonds",
    $(myDiamonds)
      .apply("priceOver2", "$price / $two")
  )
  // [{
  //   diamonds: [
  //     { "carat": 0.23, "cut": "Ideal",   "color": "E", "clarity": "SI2", "price": 326, priceOver2: 163 },
  //     { "carat": 0.21, "cut": "Premium", "color": "E", "clarity": "SI1", "price": 326, priceOver2: 163 },
  //     { "carat": 0.23, "cut": "Good",    "color": "E", "clarity": "VS1", "price": 328, priceOver2: 164  }
  //   ]
  // }]

  .apply('Count', '$diamonds.count()')
  // [{
  //   diamonds: [
  //     { "carat": 0.23, "cut": "Ideal",   "color": "E", "clarity": "SI2", "price": 326, priceOver2: 163 },
  //     { "carat": 0.21, "cut": "Premium", "color": "E", "clarity": "SI1", "price": 326, priceOver2: 163 },
  //     { "carat": 0.23, "cut": "Good",    "color": "E", "clarity": "VS1", "price": 328, priceOver2: 164  }
  //   ]
  //   Count: 3
  // }]

  .apply('TotalPrice', $('diamonds').sum('$priceOver2'))
  .apply('TotalPrice', $('diamonds').sum('$price * $carat'))
  // [{
  //   diamonds: [
  //     { "carat": 0.23, "cut": "Ideal",   "color": "E", "clarity": "SI2", "price": 326, priceOver2: 163 },
  //     { "carat": 0.21, "cut": "Premium", "color": "E", "clarity": "SI1", "price": 326, priceOver2: 163 },
  //     { "carat": 0.23, "cut": "Good",    "color": "E", "clarity": "VS1", "price": 328, priceOver2: 164  }
  //   ]
  //   Count: 3
  //   TotalPrice: 490
  // }]

  .apply('Cuts',
    $("diamonds").group("$cut").label('Cut')
      // [
      //   {
      //     Cut: 'good'
      //   }
      //   {
      //     Cut: 'v good'
      //   }
      //   {
      //     Cut: 'ideal'
      //   }
      // ]

      .apply('diamonds', $('diamonds').filter('$cut = $^Cut'))
      // [
      //   {
      //     Cut: 'good'
      //     diamonds: <dataset cut = good>
      //   }
      //   {
      //     Cut: 'v good'
      //     diamonds: <dataset cut = v good>
      //   }
      //   {
      //     Cut: 'ideal'
      //     diamonds: <dataset cut = ideal>
      //   }
      // ]

      .apply('Count', $('diamonds').count())
      // [
      //   {
      //     Cut: 'good'
      //     diamonds: <dataset cut = good>
      //     Count: 213
      //   }
      //   {
      //     Cut: 'v good'
      //     diamonds: <dataset cut = v good>
      //     Count: 21
      //   }
      //   {
      //     Cut: 'ideal'
      //     diamonds: <dataset cut = ideal>
      //     Count: 13
      //   }
      // ]

      .apply('AvgPrice', '$diamonds.sum($price) / $diamonds.count()')
      .sort('$AvgPrice', 'descending')
      .limit(3)
      .apply('somethingElse', '$diamonds.sum($x)')
  )
  .compute()
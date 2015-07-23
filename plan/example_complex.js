$() // [{}]
  .container('div.example')

  .apply("Diamonds", $(sqlDriverClient).filter("$color = 'D'"))
  // [{ Diamonds: <Dataset> }]

  .apply('Stage', Shape.rectangle(800, 600))
  // [{ Diamonds: <Dataset>, stage: { type: 'shape', shape: 'rectangle', width: 800, height: 600 } }]

  .apply('Count', '$Diamonds.count()')
  // [{ Diamonds: <Dataset>, stage: '<shape>', Count: 2342 }]

  .apply('VerticalScale', Scale.linear())

  .apply('ColorScale', Scale.color())

  .apply('Cuts',
    $("Diamonds").split("$cut", 'Cut')
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

      .apply('Diamonds', $('^Diamonds').filter('$cut = $^Cut'))
      // [
      //   {
      //     Cut: 'good'
      //     Diamonds: <dataset cut=good>
      //   }
      //   {
      //     Cut: 'v good'
      //     Diamonds: <dataset cut=v good>
      //   }
      //   {
      //     Cut: 'ideal'
      //     Diamonds: <dataset cut=ideal>
      //   }
      // ]

      .apply('Count', $('diamonds').count())
      .apply('AvgPrice', '$diamonds.sum("price") / $diamonds.count()')
      .sort('AvgPrice', 'descending')
      .limit(10)
      .apply('AccAvgPrice', 'AvgPrice')
      .apply('stage', Layout.horizontal('$stage', { gap: 3 }))
      .train('verticalScale', 'domain', '$AvgPrice')
      .train('verticalScale', 'range', '$stage.height')
      .train('color', 'domain', '$Cut')
      .apply('maxPrice', '$self.max($price)')
      .apply('barStage', function(d) {
        return Transform.margin(d.stage, {
          bottom: 0,
          height: d.verticalScale(d.AvgPrice)
        })
      })
      .apply('barStage1', Transform.margin('stage', {
        bottom: 0,
        height: '$verticalScale($AvgPrice)'
      }))
      .apply('barStage2', "stage.margin(bottom=0, height=$verticalScale($AvgPrice))")
      .render(Mark.box('barStage', {
        type: 'box',
        fill: '#f0f0f0',
        stroke: use('color')
      }))
      .apply('pointStage', Transform.margin('barStage', {
        bottom: 6
      }))
      .render(Mark.label('$pointStage', {
        text: '$Cut',
        color: '$color',
        anchor: 'middle',
        baseline: 'bottom'
      })) // Used without name
    )
    .compute();


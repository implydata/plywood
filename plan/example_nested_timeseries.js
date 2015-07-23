var populationDriver = null;

var query = $("data")
  .split("$country", 'Country')
    .apply('Revenue', '$data.sum($revenue)')
    .sort('Revenue', 'descending')
    .limit(3)
    .apply('Times',
      $("data").split("$timestamp.timeRange('PT1H', 'Etc/UTC')", 'Time')
        .apply('Revenue', '$data.sum($revenue)')
        .sort('Time', 'ascending')
    )

query.compute(populationDriver).then(function(data) {
  console.log(data)
});

// =>

[
  {
    Country: 'United States'
    Revenue: 22124
    Times: [
      {
        Time: TimeRange('2015-02-20T00:00:00', '2015-02-20T01:00:00')
        Revenue: 463
      }
      {
        Time: TimeRange('2015-02-20T01:00:00', '2015-02-20T02:00:00')
        Revenue: 245
      }
      // ...
    ]
  }
  {
    Country: 'United Kingdom'
    Revenue: 14525
    Times: [
      {
        Time: TimeRange('2015-02-20T00:00:00', '2015-02-20T01:00:00')
        Revenue: 210
      }
      {
        Time: TimeRange('2015-02-20T01:00:00', '2015-02-20T02:00:00')
        Revenue: 110
      }
      // ...
    ]
  }
  // ...
]

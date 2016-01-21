var Dataset = plywood.Dataset;
var $ = plywood.$;

function translate(x, y) {
  return "translate(" + x + "," + y + ")";
}

var dayOfWeekLabels = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'];


var stateByDayOfWeek = $('main')
  .filter($('time').in({ start: new Date('2015-11-01T00:00:00Z'), end: null }))
  .filter('$region != null and $country == "United States"')
  .split('$region', 'State')
    .apply('Edits', '$main.count()')
    .sort('$Edits', 'descending')
    .limit(5)
    .apply('DaysOfWeek',
      $('main').split($("time").timePart('DAY_OF_WEEK', 'Etc/UTC'), 'DayOfWeek')
        .apply('Edits', '$main.count()')
        .sort('$DayOfWeek', 'ascending')
    );

d3.xhr('http://172.31.0.165:9090/plywood')
  .header("Content-Type", "application/json")
  .response(function(request) { return Dataset.fromJS(JSON.parse(request.responseText)); })
  .post(
    JSON.stringify({
      dataSource: 'wiki',
      expression: stateByDayOfWeek
    }),
    function(err, dataset) {
      if (err) {
        console.log(err);
        return;
      }
      render(dataset);
    }
  );


var margin = {top: 20, right: 20, bottom: 30, left: 40},
  width = 960 - margin.left - margin.right,
  height = 500 - margin.top - margin.bottom;

var x0 = d3.scale.ordinal()
  .rangeRoundBands([0, width], .1);

var x1 = d3.scale.ordinal();

var y = d3.scale.linear()
  .range([height, 0]);

var color = d3.scale.ordinal()
  .range(["#98abc5", "#8a89a6", "#7b6888", "#6b486b", "#a05d56", "#d0743c", "#ff8c00"]);

var xAxis = d3.svg.axis()
  .scale(x0)
  .orient("bottom");

var yAxis = d3.svg.axis()
  .scale(y)
  .orient("left")
  .tickFormat(d3.format(".2s"));

var svg = d3.select("body").append("svg")
  .attr("width", width + margin.left + margin.right)
  .attr("height", height + margin.top + margin.bottom)
  .append("g")
  .attr("transform", translate(margin.left, margin.top));

var days = [0, 1, 2, 3, 4, 5, 6];

function render(dataset) {
  var data = dataset.toJS();

  x0.domain(data.map(function(d) { return d.State; }));
  x1.domain(days).rangeRoundBands([0, x0.rangeBand()]);

  y.domain([
    0,
    d3.max(data, function(d) {
      return d3.max(d.DaysOfWeek, function(d) { return d.Edits; });
    })
  ]);

  svg.append("g")
    .attr("class", "x axis")
    .attr("transform", translate(0, height))
    .call(xAxis);

  svg.append("g")
    .attr("class", "y axis")
    .call(yAxis)
    .append("text")
      .attr("transform", "rotate(-90)")
      .attr("y", 6)
      .attr("dy", ".71em")
      .style("text-anchor", "end")
      .text("Edits");

  // Create a <g> for each state
  var states = svg.selectAll(".state")
    .data(data); // Split on 'state'

  states.enter().append("g").attr('class', 'state');

  states
    .attr("transform", function(d) { return translate(x0(d.State), 0); });

  states.exit().remove();

  // Within each <g> make a <rect> for the days
  var daysOfWeek = states.selectAll(".day-of-week")
    .data(function(d) { return d.DaysOfWeek; }); // Split on day quarter

  daysOfWeek.enter().append("rect").attr('class', 'day-of-week');

  daysOfWeek
    .attr("width", x1.rangeBand())
    .attr("x", function(d) { return x1(d.DayOfWeek); })
    .attr("y", function(d) { return y(d.Edits); })
    .attr("height", function(d) { return height - y(d.Edits); })
    .style("fill", function(d) { return color(d.DayOfWeek); });

  daysOfWeek.exit().remove();


  var legend = svg.selectAll(".legend").data(days);

  legend.enter().append("g")
    .attr("class", "legend")
    .attr("transform", function(d, i) { return translate(0, i * 20); });

  legend.append("rect")
    .attr("x", width - 18)
    .attr("width", 18)
    .attr("height", 18)
    .style("fill", color);

  legend.append("text")
    .attr("x", width - 24)
    .attr("y", 9)
    .attr("dy", ".35em")
    .style("text-anchor", "end")
    .text(function(d) { return dayOfWeekLabels[d]; });
}

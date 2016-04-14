{// starts with function(plywood, chronoshift)
var ply = plywood.ply;
var $ = plywood.$;
var r = plywood.r;
var Expression = plywood.Expression;
var FilterAction = plywood.FilterAction;
var ApplyAction = plywood.ApplyAction;
var SortAction = plywood.SortAction;
var LimitAction = plywood.LimitAction;
var MatchAction = plywood.MatchAction;

var Timezone = chronoshift.Timezone;

var Set = plywood.Set;

var dataRef = $('data');

// See here: https://www.drupal.org/node/141051
var reservedWords = {
  ALL: 1, AND: 1, AS: 1, ASC: 1,
  BETWEEN: 1, BY: 1,
  CONTAINS: 1, CREATE: 1,
  DELETE: 1, DESC: 1, DESCRIBE: 1, DISTINCT: 1, DROP: 1,
  EXISTS: 1, EXPLAIN: 1, ESCAPE: 1,
  FALSE: 1, FROM: 1,
  GROUP: 1,
  HAVING: 1,
  IN: 1, INNER: 1, INSERT: 1, INTO: 1, IS: 1,
  JOIN: 1,
  LEFT: 1, LIKE: 1, LIMIT: 1, LOOKUP: 1,
  MATCH: 1,
  NOT: 1, NULL: 1,
  ON: 1, OR: 1, ORDER: 1,
  REPLACE: 1, REGEXP: 1,
  SELECT: 1, SET: 1, SHOW: 1,
  TABLE: 1, TRUE: 1,
  UNION: 1, UPDATE: 1,
  VALUES: 1,
  WHERE: 1
};

var unsupportedVerbs = {
  ALTER: 1,
  CALL: 1,
  CREATE: 1,
  DEALLOCATE: 1,
  DELETE: 1,
  DO: 1,
  DROP: 1,
  EXECUTE: 1,
  HANDLER: 1,
  INSERT: 1,
  LOAD: 1,
  LOCK: 1,
  PREPARE: 1,
  RENAME: 1,
  REPLACE: 1,
  SAVEPOINT: 1,
  START: 1,
  TRUNCATE: 1,
  UNLOCK: 1,
  UPDATE: 1
};

var intervalUnits = {
  MICROSECOND: 1,
  SECOND: 1,
  MINUTE: 1,
  HOUR: 1,
  DAY: 1,
  WEEK: 1,
  MONTH: 1,
  QUARTER: 1,
  YEAR: 1
}

var dateFormats = {
  '%Y-%m-%d %H:%i:%s': 'PT1S',
  '%Y-%m-%d %H:%i:00': 'PT1M',
  '%Y-%m-%d %H:00:00': 'PT1H',
  '%Y-%m-%d': 'P1D',
  '%Y-%m-01': 'P1M',
  '%Y-01-01': 'P1Y',
};

function upgrade(v) {
  if (!Expression.isExpression(v)) return r(v);
  return v;
}

var notImplemented = function() { error('not implemented yet'); };
var fns = {
  ABSOLUTE: function(op) { return upgrade(op).absolute(); },
  OVERLAP: function(op, ex) { return upgrade(op).overlap(ex); },
  SQRT: function(op) { return upgrade(op).power(0.5); },
  EXP: function(ex) { return r(Math.E).power(ex); },
  POWER: function(op, ex) { return upgrade(op).power(ex); },
  NOW: function() { return r(new Date()); },
  CURDATE: function() { return r(chronoshift.day.floor(new Date(), Timezone.UTC)); },
  FALLBACK: function(op, ex) { return upgrade(op).fallback(ex); },
  MATCH: function(op, reg) { return upgrade(op).match(reg); },
  EXTRACT: function(op, reg) { return upgrade(op).extract(reg); },
  CONCAT: function() { return Expression.concat(Array.prototype.map.call(arguments, upgrade)); },
  SUBSTRING: function(op, i, n) { return upgrade(op).substr(i, n); },
  TIME_FLOOR: function(op, d, tz) { return upgrade(op).timeFloor(d, tz); },
  TIME_SHIFT: function(op, d, s, tz) { return upgrade(op).timeShift(d, s, tz); },
  TIME_RANGE: function(op, d, s, tz) { return upgrade(op).timeRange(d, s, tz); },
  TIME_BUCKET: function(op, d, tz) { return upgrade(op).timeBucket(d, tz); },
  NUMBER_BUCKET: function(op, s, o) { return upgrade(op).numberBucket(s, o); },
  TIME_PART: function(op, part, tz) { return upgrade(op).timePart(part, tz); },
  LOOKUP: function(op, name) { return upgrade(op).lookup(name); },
  PI: function() { return r(Math.PI); },
  STD: notImplemented,
  DATE_FORMAT: function(op, format) {
    var duration = dateFormats[format.replace(/ 00:00:00$/, '')];
    if (!duration) error('unsupported format: ' + format);
    return upgrade(op).timeFloor(duration);
  },

  YEAR: function(op, tz) { return upgrade(op).timePart('YEAR', tz); },
  MONTH: function(op, tz) { return upgrade(op).timePart('MONTH_OF_YEAR', tz); },
  WEEK_OF_YEAR: function(op, tz) { return upgrade(op).timePart('WEEK_OF_YEAR', tz); },
  DAY_OF_YEAR: function(op, tz) { return upgrade(op).timePart('DAY_OF_YEAR', tz); },
  DAY_OF_MONTH: function(op, tz) { return upgrade(op).timePart('DAY_OF_MONTH', tz); },
  DAY_OF_WEEK: function(op, tz) { return upgrade(op).timePart('DAY_OF_WEEK', tz); },
  WEEKDAY: notImplemented,
  HOUR: function(op, tz) { return upgrade(op).timePart('HOUR_OF_DAY', tz); },
  MINUTE: function(op, tz) { return upgrade(op).timePart('MINUTE_OF_HOUR', tz); },
  SECOND: function(op, tz) { return upgrade(op).timePart('SECOND_OF_MINUTE', tz); },
  DATE: function(op, tz) { return upgrade(op).timeFloor('P1D', tz); },
  TIMESTAMP: function(op) { return upgrade(op).bumpStringLiteralToTime(); },
  TIME: function() { error('time literals are not supported'); },
  DATE_ADD: function(op, d, tz) { return d === 0 ? upgrade(op) : error('only zero interval supported in date math'); },
  DATE_SUB: function(op, d, tz) { return d === 0 ? upgrade(op) : error('only zero interval supported in date math'); },
};
fns.ABS = fns.ABSOLUTE;
fns.POW = fns.POWER;
fns.IFNULL = fns.FALLBACK;
fns.SUBSTR = fns.SUBSTRING;
fns.CURRENT_TIMESTAMP = fns.NOW;
fns.LOCALTIME = fns.NOW;
fns.LOCALTIMESTAMP = fns.NOW;
fns.UTC_TIMESTAMP = fns.NOW;
fns.SYSDATE = fns.NOW;
fns.CURRENT_DATE = fns.CURDATE;
fns.UTC_DATE = fns.CURDATE;
fns.DAY_OF_YEAR = fns.DAY_OF_YEAR;
fns.DOY = fns.DAY_OF_YEAR;
fns.DOW = fns.DAY_OF_WEEK;
fns.DAYOFMONTH = fns.DAY_OF_MONTH;
fns.DAY = fns.DAY_OF_MONTH;
fns.WEEKOFYEAR = fns.WEEK_OF_YEAR;
fns.WEEK = fns.WEEK_OF_YEAR;
fns.ADDDATE = fns.DATE_ADD;
fns.SUBDATE = fns.DATE_SUB;
fns.STDDEV = fns.STD;
fns.STDDEV_POP = fns.STD;

var objectHasOwnProperty = Object.prototype.hasOwnProperty;
function reserved(str) {
  return objectHasOwnProperty.call(reservedWords, str.toUpperCase());
}

function makeDate(type, v) {
  try {
    return chronoshift.parseSQLDate(type, v);
  } catch (e) {
    var isoDate = chronoshift.parseISODate(v);
    if (isoDate) {
      if (type === 'd') isoDate = chronoshift.day.floor(isoDate, Timezone.UTC);
      return isoDate;
    }
    error(e.message);
  }
}

function extractGroupByColumn(columns, groupBy, index) {
  var label = null;
  var otherColumns = [];
  for (var i = 0; i < columns.length; i++) {
    var column = columns[i];
    if (groupBy.equals(column.expression)) {
      if (label) error('already have a label');
      label = column.name;
    } else {
      otherColumns.push(column);
    }
  }
  if (!label) label = 'split' + index;
  return {
    label: label,
    otherColumns: otherColumns
  };
}

function upgradeGroupBys(distinct, columns, groupBys) {
  if (Array.isArray(columns)) { // Not *
    if (!groupBys) {
      // Support for not having a group by clause if there are aggregates in the columns
      // A having an aggregate columns is the same as having "GROUP BY ''"

      var hasAggregate = columns.some(function(column) {
        var columnExpression = column.expression;
        return columnExpression.isOp('chain') &&
          columnExpression.actions.some(function(action) { return action.isAggregate(); })
      })
      if (hasAggregate) {
        return [Expression.EMPTY_STRING];
      } else if (distinct) {
        return columns.map(function(column) { return column.expression });
      }

    } else {
      return groupBys.map(function(groupBy) {
        if (groupBy.isOp('literal') && groupBy.type === 'NUMBER') {
          // Support for not having a group by clause refer to a select column by index

          var groupByColumn = columns[groupBy.value - 1];
          if (!groupByColumn) error("Unknown column '" + groupBy.value + "' in group by statement");

          return groupByColumn.expression;
        } else {
          return groupBy;
        }
      });
    }
  }

  return groupBys;
}

function staticColumn(column) {
  return column.expression.getFreeReferences().length === 0;
}

function constructQuery(distinct, columns, from, where, groupBys, having, orderBy, limit) {
  if (!columns) error('Can not have empty column list');

  var query = null;

  if (!distinct && Array.isArray(columns) && !from && !where && !groupBys && columns.every(staticColumn)) {
    // This is a SELECT 1+1; type query
    query = ply();
    for (var i = 0; i < columns.length; i++) {
      query = query.performAction(columns[i]);
    }

  } else {
    from = from ? $(from) : dataRef;

    if (where) {
      from = from.filter(where);
    }

    groupBys = upgradeGroupBys(distinct, columns, groupBys);


    if (!groupBys) {
      // Select query
      query = from;

      if (Array.isArray(columns)) {
        var attributes = [];
        for (var i = 0; i < columns.length; i++) {
          var column = columns[i];
          query = query.performAction(column);
          attributes.push(column.name);
        }
        query = query.select.apply(query, attributes);
      }

    } else {
      // Group By query
      if (columns === '*') error('can not SELECT * with a GROUP BY');

      if (groupBys.length === 1 && groupBys[0].isOp('literal')) {
        query = ply().apply('data', from);
      } else {
        var splits = {};
        for (var i = 0; i < groupBys.length; i++) {
          var groupBy = groupBys[i];
          var extract = extractGroupByColumn(columns, groupBy, i);
          columns = extract.otherColumns;
          splits[extract.label] = groupBy;
        }
        query = from.split(splits, 'data');
      }

      if (Array.isArray(columns)) {
        for (var i = 0; i < columns.length; i++) {
          query = query.performAction(columns[i]);
        }
      }
    }
  }

  if (having) {
    query = query.performAction(having);
  }
  if (orderBy) {
    query = query.performAction(orderBy);
  }
  if (limit) {
    query = query.performAction(limit);
  }

  return query;
}

function makeListMap1(head, tail) {
  if (head == null) return [];
  return [head].concat(tail.map(function(t) { return t[1] }));
}

function naryExpressionFactory(op, head, tail) {
  if (!tail.length) return head;
  return head[op].apply(head, tail.map(function(t) { return t[1]; }));
}

function naryExpressionWithAltFactory(op, head, tail, altToken, altOp) {
  if (!tail.length) return head;
  for (var i = 0; i < tail.length; i++) {
    var t = tail[i];
    head = head[t[0] === altToken ? altOp : op].call(head, t[1]);
  }
  return head;
}

}// Start grammar

start
  = _ queryParse:Query QueryTerminator? { return queryParse; }

Query
  = queryParse:(SelectQuery / DescribeQuery / OtherQuery / UnsupportedQuery)
    {
      return queryParse;
    }
  / ex:Expression
    {
      return {
        verb: null,
        expression: ex
      }
    }

OtherQuery
  = verb:(ShowToken / SetToken) rest:$(.*)
    {
      return {
        verb: verb,
        rest: rest
      };
    }

DescribeQuery
  = (DescribeToken / DescToken) table:RelaxedNamespacedRef
    {
      return {
        verb: 'DESCRIBE',
        table: table.name
      };
    }

UnsupportedQuery
  = verb:Name &{ return unsupportedVerbs[verb.toUpperCase()]; } _ rest:$(.*)
    {
      return {
        verb: verb.toUpperCase(),
        rest: rest
      };
    }

SelectQuery
  = SelectToken distinct:DistinctToken? columns:Columns? from:FromClause? where:WhereClause? groupBys:GroupByClause? having:HavingClause? orderBy:OrderByClause? limit:LimitClause?
    {
      return {
        verb: 'SELECT',
        expression: constructQuery(distinct, columns, from, where, groupBys, having, orderBy, limit),
        table: from
      };
    }

SelectSubQuery
  = SelectToken distinct:DistinctToken? columns:Columns? where:WhereClause? groupBys:GroupByClause having:HavingClause? orderBy:OrderByClause? limit:LimitClause?
    { return constructQuery(distinct, columns, null, where, groupBys, having, orderBy, limit); }

Columns
  = StarToken
    { return '*'; }
  / head:Column tail:(Comma Column)*
    { return makeListMap1(head, tail); }

Column
  = ex:Expression as:As?
    {
      return new ApplyAction({
        name: as || text().replace(/^\W+|\W+$/g, '').replace(/\W+/g, '_'),
        expression: ex
      });
    }

As
  = AsToken name:(String / Ref) { return name; }

FromClause
  = FromToken table:RelaxedNamespacedRef
    { return table.name; }

WhereClause
  = WhereToken filter:Expression
    { return filter; }

GroupByClause
  = GroupToken ByToken head:Expression tail:(Comma Expression)*
    { return makeListMap1(head, tail); }

HavingClause
  = HavingToken having:Expression
    { return new FilterAction({ expression: having }); }

OrderByClause
  = OrderToken ByToken orderBy:Expression direction:Direction? tail:(Comma Expression Direction?)*
    {
      if (tail.length) error('plywood does not currently support multi-column ORDER BYs');
      return new SortAction({ expression: orderBy, direction: direction || 'ascending' });
    }

Direction
  = AscToken / DescToken

LimitClause
  = LimitToken limit:Number
    { return new LimitAction({ limit: limit }); }

QueryTerminator
  = ";" _

/*
Expressions are defined below in acceding priority order

  Or (OR)
  And (AND)
  Not (NOT)
  Comparison (=, <=>, <, >, <=, >=, <>, !=, IS, LIKE, BETWEEN, IN, CONTAINS, REGEXP)
  Additive (+, -)
  Multiplicative (*), Division (/)
  Unary identity (+), negation (-)
*/

Expression = OrExpression


OrExpression
  = head:AndExpression tail:(OrToken AndExpression)*
    { return naryExpressionFactory('or', head, tail); }


AndExpression
  = head:NotExpression tail:(AndToken NotExpression)*
    { return naryExpressionFactory('and', head, tail); }


NotExpression
  = not:NotToken? ex:ComparisonExpression
    {
      if (not) ex = ex.not();
      return ex;
    }


ComparisonExpression
  = ex:AdditiveExpression rhs:ComparisonExpressionRhs?
    {
      if (rhs) {
        ex = ex[rhs.call].apply(ex, rhs.args);
        if (rhs.not) ex = ex.not();
      }
      return ex;
    }

ComparisonExpressionRhs
  = not:NotToken? rhs:ComparisonExpressionRhsNotable
    {
      rhs.not = not;
      return rhs;
    }
  / IsToken not:NotToken? rhs:AdditiveExpression
    {
      return { call: 'is', args: [rhs], not: not };
    }
  / op:ComparisonOp _ lhs:AdditiveExpression
    {
      return { call: op, args: [lhs] };
    }

ComparisonExpressionRhsNotable
  = BetweenToken start:LiteralExpression AndToken end:LiteralExpression
    {
      var range = { start: start.value, end: end.value, bounds: '[]' };
      return { call: 'in', args: [range] };
    }
  / InToken list:(InListLiteralExpression / AdditiveExpression)
    {
      return { call: 'in', args: [list] };
    }
  / ContainsToken string:String
    {
      return { call: 'contains', args: [string, 'ignoreCase'] };
    }
  / LikeToken string:String escape:(EscapeToken String)?
    {
      var escapeStr = escape ? escape[1] : '\\';
      if (escapeStr.length > 1) error('Invalid escape string: ' + escapeStr);
      return { call: 'match', args: [MatchAction.likeToRegExp(string, escapeStr)] };
    }
  / RegExpToken string:String
    {
      return { call: 'match', args: [string] };
    }

ComparisonOp
  = "="   { return 'is'; }
  / "<=>" { return 'is'; }
  / "<>"  { return 'isnt'; }
  / "!="  { return 'isnt'; }
  / "<="  { return 'lessThanOrEqual'; }
  / ">="  { return 'greaterThanOrEqual'; }
  / "<"   { return 'lessThan'; }
  / ">"   { return 'greaterThan'; }


AdditiveExpression
  = head:MultiplicativeExpression tail:(AdditiveOp MultiplicativeExpression)*
    { return naryExpressionWithAltFactory('add', head, tail, '-', 'subtract'); }

AdditiveOp = op:("+" / "-") !"+" _ { return op; }


MultiplicativeExpression
  = head:UnaryExpression tail:(MultiplicativeOp UnaryExpression)*
    { return naryExpressionWithAltFactory('multiply', head, tail, '/', 'divide'); }

MultiplicativeOp = op:("*" / "/") _ { return op; }


UnaryExpression
  = op:AdditiveOp !Number ex:BasicExpression
    {
      // !Number is to make sure that -3 parses as literal(-3) and not literal(3).negate()
      var negEx = ex.negate(); // Always negate (even with +) just to make sure it is possible
      return op === '-' ? negEx : ex;
    }
  / BasicExpression


BasicExpression
  = LiteralExpression
  / AggregateExpression
  / FunctionCallExpression
  / OpenParen sub:(Expression / SelectSubQuery) CloseParen { return sub; }
  / RefExpression


AggregateExpression
  = CountToken OpenParen distinct:DistinctToken? exd:ExpressionMaybeFiltered? CloseParen
    {
      if (!exd) {
        if (distinct) error('COUNT DISTINCT must have an expression');
        return dataRef.count();
      } else if (exd.ex === '*') {
        if (distinct) error('COUNT DISTINCT can not be used with *');
        return exd.data.count();
      } else {
        return distinct ? exd.data.countDistinct(exd.ex) : exd.data.filter(exd.ex.isnt(null)).count()
      }
    }
  / fn:AggregateFn OpenParen distinct:DistinctToken? exd:ExpressionMaybeFiltered CloseParen
    {
      if (distinct) error('can not use DISTINCT for ' + fn + ' aggregator');
      if (exd.ex === '*') error('can not use * for ' + fn + ' aggregator');
      return exd.data[fn](exd.ex);
    }
  / QuantileToken OpenParen distinct:DistinctToken? exd:ExpressionMaybeFiltered Comma value:Number CloseParen
    {
      if (distinct) error('can not use DISTINCT for quantile aggregator');
      if (exd.ex === '*') error('can not use * for quantile aggregator');
      return exd.data.quantile(exd.ex, value);
    }
  / CustomToken OpenParen value:String filter:WhereClause? CloseParen
    {
      var d = dataRef;
      if (filter) d = d.filter(filter);
      return d.custom(value);
    }

AggregateFn
  = SumToken / AvgToken / MinToken / MaxToken / CountDistinctToken

ExpressionMaybeFiltered
  = ex:(StarToken / Expression) filter:WhereClause?
    {
      var data = dataRef;
      if (filter) data = data.filter(filter);
      return { ex: ex, data: data };
    }


FunctionCallExpression
  = fn:Fn OpenParen params:Params CloseParen
    { return fn.apply(null, params); }

Fn
  = name:Name &{ return fns[name.toUpperCase()]; }
    { return fns[name.toUpperCase()]; }

Params
  = head:Param? tail:(Comma Param)*
    { return makeListMap1(head, tail); }

Param = Number / String / Interval / Expression;

RefExpression
  = ref:NamespacedRef { return $(ref.name); }

RelaxedNamespacedRef
  = ns:(Ref Dot)? name:RelaxedRef
    {
      return {
        namespace: ns ? ns[0] : null,
        name: name
      };
    }

NamespacedRef
  = ns:(Ref Dot)? name:Ref
    {
      return {
        namespace: ns ? ns[0] : null,
        name: name
      };
    }

RelaxedRef
  = name:RelaxedName !{ return reserved(name); }
    { return name }
  / BacktickRef

Ref
  = name:Name !{ return reserved(name); }
    { return name }
  / BacktickRef

BacktickRef
  = "`" name:$([^`]+) "`" _
    { return name }

NameOrString = Name / String

StringOrNumber = String / Number


LiteralExpression
  = OpenCurly type:(DToken / TToken / TsToken) v:String CloseCurly
    { return r(makeDate(type, v)); }
  / type:(DateToken / TimeToken / TimestampToken) v:String
    { return r(makeDate(type, v)); }
  / v:(Number / String / ListLiteral / NullToken / TrueToken / FalseToken)
    { return r(v); }

ListLiteral
  = OpenCurly head:StringOrNumber? tail:(Comma StringOrNumber)* CloseCurly
    { return Set.fromJS(makeListMap1(head, tail)); }

InListLiteralExpression
  = OpenParen head:StringOrNumber tail:(Comma StringOrNumber)* CloseParen
    { return r(Set.fromJS(makeListMap1(head, tail))); }

String "String"
  = "'" chars:NotSQuote "'" _ { return chars; }
  / "'" chars:NotSQuote { error("Unmatched single quote"); }
  / '"' chars:NotDQuote '"' _ { return chars; }
  / '"' chars:NotDQuote { error("Unmatched double quote"); }


Interval
  = IntervalToken n:Number unit:Name &{ return intervalUnits[unit] }
    {
      if (n !== 0) error('only zero intervals supported for now');
      return 0;
    }


/* Tokens */

NullToken          = "NULL"i           !IdentifierPart _ { return null; }
TrueToken          = "TRUE"i           !IdentifierPart _ { return true; }
FalseToken         = "FALSE"i          !IdentifierPart _ { return false; }

SelectToken        = "SELECT"i         !IdentifierPart _ { return 'SELECT'; }
DescribeToken      = "DESCRIBE"i       !IdentifierPart _ { return 'DESCRIBE'; }
ShowToken          = "SHOW"i           !IdentifierPart _ { return 'SHOW'; }
SetToken           = "SET"i            !IdentifierPart _ { return 'SET'; }

FromToken          = "FROM"i           !IdentifierPart _
AsToken            = "AS"i             !IdentifierPart _
OnToken            = "ON"i             !IdentifierPart _
LeftToken          = "LEFT"i           !IdentifierPart _
InnerToken         = "INNER"i          !IdentifierPart _
JoinToken          = "JOIN"i           !IdentifierPart _
UnionToken         = "UNION"i          !IdentifierPart _
WhereToken         = "WHERE"i          !IdentifierPart _
GroupToken         = "GROUP"i          !IdentifierPart _
ByToken            = "BY"i             !IdentifierPart _
OrderToken         = "ORDER"i          !IdentifierPart _
HavingToken        = "HAVING"i         !IdentifierPart _
LimitToken         = "LIMIT"i          !IdentifierPart _

AscToken           = "ASC"i            !IdentifierPart _ { return SortAction.ASCENDING;  }
DescToken          = "DESC"i           !IdentifierPart _ { return SortAction.DESCENDING; }

BetweenToken       = "BETWEEN"i        !IdentifierPart _
InToken            = "IN"i             !IdentifierPart _
IsToken            = "IS"i             !IdentifierPart _
LikeToken          = "LIKE"i           !IdentifierPart _
ContainsToken      = "CONTAINS"i       !IdentifierPart _
RegExpToken        = "REGEXP"i         !IdentifierPart _
EscapeToken        = "ESCAPE"i         !IdentifierPart _

NotToken           = "NOT"i            !IdentifierPart _
AndToken           = "AND"i            !IdentifierPart _
OrToken            = "OR"i             !IdentifierPart _

DistinctToken      = "DISTINCT"i       !IdentifierPart _
StarToken          = "*"               !IdentifierPart _ { return '*'; }

CountToken         = "COUNT"i          !IdentifierPart _ { return 'count'; }
CountDistinctToken = "COUNT_DISTINCT"i !IdentifierPart _ { return 'countDistinct'; }
SumToken           = "SUM"i            !IdentifierPart _ { return 'sum'; }
AvgToken           = "AVG"i            !IdentifierPart _ { return 'average'; }
MinToken           = "MIN"i            !IdentifierPart _ { return 'min'; }
MaxToken           = "MAX"i            !IdentifierPart _ { return 'max'; }
QuantileToken      = "QUANTILE"i       !IdentifierPart _ { return 'quantile'; }
CustomToken        = "CUSTOM"i         !IdentifierPart _ { return 'custom'; }

DateToken          = "DATE"i           !IdentifierPart _ { return 'd'; }
TimeToken          = "TIME"i           !IdentifierPart _ { return 't'; }
TimestampToken     = "TIMESTAMP"i      !IdentifierPart _ { return 'ts'; }
DToken             = "D"i              !IdentifierPart _ { return 'd'; }
TToken             = "T"i              !IdentifierPart _ { return 't'; }
TsToken            = "TS"i             !IdentifierPart _ { return 'ts'; }

IntervalToken      = "INTERVAL"i       !IdentifierPart _

IdentifierPart = [a-z_]i

/* Numbers */

Number "Number"
  = n:$(Int Fraction? Exp?) _ { return parseFloat(n); }

Int
  = $("-"? [1-9] Digits)
  / $("-"? Digit)

Fraction
  = $("." Digits)

Exp
  = $("e"i [+-]? Digits)

Digits
  = $ Digit+

Digit
  = [0-9]


/* Extra */

OpenParen "("
  = "(" _

CloseParen ")"
  = ")" _

OpenCurly "{"
  = "{" _

CloseCurly "}"
  = "}" _

Comma
  = "," _

Dot
  = "." _

Name "Name"
  = name:$([a-z_]i [a-z0-9_]i*) _ { return name; }

RelaxedName "RelaxedName"
  = name:$([a-z_\-:*/]i [a-z0-9_\-:*/]i*) _ { return name; }

NotSQuote "NotSQuote"
  = $([^']*)

NotDQuote "NotDQuote"
  = $([^"]*)

_ "Whitespace"
  = $ ([ \t\r\n] / SingleLineComment / InlineComment)*

InlineComment
  = "/*" (!CommentTerminator .)* CommentTerminator

SingleLineComment
  = ("-- " / "#") (!LineTerminator .)*

CommentTerminator
  = "*/"

LineTerminator
  = [\n\r]

{// starts with function(plywood)
var ply = plywood.ply;
var $ = plywood.$;
var r = plywood.r;
var Expression = plywood.Expression;
var FilterAction = plywood.FilterAction;
var ApplyAction = plywood.ApplyAction;
var SortAction = plywood.SortAction;
var LimitAction = plywood.LimitAction;
var MatchAction = plywood.MatchAction;

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
}

var aggregates = {
  count: 1,
  sum: 1, min: 1, max: 1,
  average: 1,
  countDistinct: 1,
  quantile: 1,
  custom: 1,
  split: 1
}

var objectHasOwnProperty = Object.prototype.hasOwnProperty;
function reserved(str) {
  return objectHasOwnProperty.call(reservedWords, str.toUpperCase());
}

function parseYear(v) {
  if (v.length === 2) {
    v = parseInt(v, 10);
    return (v < 70 ? 2000 : 1900) + v;
  } else if (v.length === 4) {
    return parseInt(v, 10);
  } else {
    throw new Error('Invalid year in date');
  }
}

function parseMonth(v) {
  v = parseInt(v, 10);
  if (v <= 0 || 12 < v) throw new Error('Invalid month in date');
  return v - 1;
}

function parseDay(v) {
  v = parseInt(v, 10);
  if (v <= 0 || 31 < v) throw new Error('Invalid day in date');
  return v;
}

function parseHour(v) {
  v = parseInt(v, 10);
  if (v < 0 || 24 < v) throw new Error('Invalid hour in date');
  return v;
}

function parseMinute(v) {
  v = parseInt(v, 10);
  if (v < 0 || 60 < v) throw new Error('Invalid minute in date');
  return v;
}

function parseSecond(v) {
  v = parseInt(v, 10);
  if (v < 0 || 60 < v) throw new Error('Invalid second in date');
  return v;
}

function parseMillisecond(v) {
  if (!v) return 0;
  return parseInt(v.substr(0, 3), 10);
}

function makeDate(type, v) {
  if (type === 't') error('time literals are not supported');
  try {
    var m, d;
    if (type === 'ts') {
      if (m = v.match(/^(\d{2}(?:\d{2})?)(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})$/)) {
        d = Date.UTC(parseYear(m[1]), parseMonth(m[2]), parseDay(m[3]), parseHour(m[4]), parseMinute(m[5]), parseSecond(m[6]));
      } else if (m = v.match(/^(\d{2}(?:\d{2})?)[~!@#$%^&*()_+=:.\-\/](\d{1,2})[~!@#$%^&*()_+=:.\-\/](\d{1,2})[T ](\d{1,2})[~!@#$%^&*()_+=:.\-\/](\d{1,2})[~!@#$%^&*()_+=:.\-\/](\d{1,2})(?:\.(\d{1,6}))?$/)) {
        d = Date.UTC(parseYear(m[1]), parseMonth(m[2]), parseDay(m[3]), parseHour(m[4]), parseMinute(m[5]), parseSecond(m[6]), parseMillisecond(m[7]));
      } else {
        throw new Error('Invalid timestamp');
      }
    } else {
      if (m = v.match(/^(\d{2}(?:\d{2})?)(\d{2})(\d{2})$/)) {
        d = Date.UTC(parseYear(m[1]), parseMonth(m[2]), parseDay(m[3]));
      } else if (m = v.match(/^(\d{2}(?:\d{2})?)[~!@#$%^&*()_+=:.\-\/](\d{1,2})[~!@#$%^&*()_+=:.\-\/](\d{1,2})$/)) {
        d = Date.UTC(parseYear(m[1]), parseMonth(m[2]), parseDay(m[3]));
      } else {
        throw new Error('Invalid date');
      }
    }
    return new Date(d);
  } catch (e) {
    error(e.message);
  }
}

function parseISODate(v) {
  var m = v.match(/^(\d{4})-(\d\d)-(\d\d)(?:[T ](\d\d)?(?::(\d\d))?(?::(\d\d))?(?:.(\d{1,3}))?)?Z?$/);
  if (!m) return null;
  try {
    return new Date(Date.UTC(
      parseYear(m[1]),
      parseMonth(m[2]),
      parseDay(m[3]),
      parseHour(m[4] || '0'),
      parseMinute(m[5] || '0'),
      parseSecond(m[6] || '0'),
      parseMillisecond(m[7] || '0')
    ));
  } catch (e) {
    return null;
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

function constructQuery(distinct, columns, from, where, groupBys, having, orderBy, limit) {
  if (!columns) error('Can not have empty column list');
  from = from ? $(from) : dataRef;

  if (where) {
    from = from.filter(where);
  }

  if (Array.isArray(columns)) { // Not *
    if (!groupBys) {
      // Support for not having a group by clause if there are aggregates in the columns
      // A having an aggregate columns is the same as having "GROUP BY ''"

      var hasAggregate = columns.some(function(column) {
        var columnExpression = column.expression;
        return columnExpression.isOp('chain') &&
          columnExpression.actions.length &&
          aggregates[columnExpression.actions[0].action];
      })
      if (hasAggregate) {
        groupBys = [Expression.EMPTY_STRING];
      } else if (distinct) {
        groupBys = columns.map(function(column) { return column.expression });
      }

    } else {
      groupBys = groupBys.map(function(groupBy) {
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

  var query = null;
  if (!groupBys) {
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
  return [head].concat(tail.map(function(t) { return t[1] }));
}

function naryExpressionFactory(op, head, tail) {
  if (!tail.length) return head;
  return head[op].apply(head, tail.map(function(t) { return t[3]; }));
}

function naryExpressionWithAltFactory(op, head, tail, altToken, altOp) {
  if (!tail.length) return head;
  for (var i = 0; i < tail.length; i++) {
    var t = tail[i];
    head = head[t[1] === altToken ? altOp : op].call(head, t[3]);
  }
  return head;
}

}// Start grammar

start
  = _ queryParse:SelectQuery _ { return queryParse; }
  / _ queryParse:DescribeQuery _ { return queryParse; }
  / _ queryParse:OtherQuery _ { return queryParse; }
  / _ ex:Expression _
    {
      return {
        verb: null,
        expression: ex
      }
    }

OtherQuery
  = verb:(UpdateToken / ShowToken / SetToken) rest:Rest
    {
      return {
        verb: verb,
        rest: rest
      };
    }

DescribeQuery
  = DescribeToken _ table:NamespacedRef _ QueryTerminator? _
    {
      return {
        verb: 'DESCRIBE',
        table: table
      };
    }

SelectQuery
  = SelectToken distinct:Distinct? columns:Columns? from:FromClause? where:WhereClause? groupBys:GroupByClause? having:HavingClause? orderBy:OrderByClause? limit:LimitClause? QueryTerminator? _
    {
      return {
        verb: 'SELECT',
        expression: constructQuery(distinct, columns, from, where, groupBys, having, orderBy, limit),
        table: from
      };
    }

SelectSubQuery
  = SelectToken distinct:Distinct? columns:Columns? where:WhereClause? groupBys:GroupByClause having:HavingClause? orderBy:OrderByClause? limit:LimitClause?
    { return constructQuery(distinct, columns, null, where, groupBys, having, orderBy, limit); }

Columns
  = _ StarToken
    { return '*'; }
  / _ head:Column tail:(Comma Column)*
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
  = _ AsToken _ name:(String / Ref) { return name; }

Distinct
  = _ DistinctToken

FromClause
  = _ FromToken _ table:NamespacedRef
    { return table; }

WhereClause
  = _ WhereToken _ filter:Expression
    { return filter; }

GroupByClause
  = _ GroupToken __ ByToken _ head:Expression tail:(Comma Expression)*
    { return makeListMap1(head, tail); }

HavingClause
  = _ HavingToken _ having:Expression
    { return new FilterAction({ expression: having }); }

OrderByClause
  = _ OrderToken __ ByToken _ orderBy:Expression direction:Direction? tail:(Comma Expression Direction?)*
    {
      if (tail.length) error('plywood does not currently support multi-column ORDER BYs');
      return new SortAction({ expression: orderBy, direction: direction || 'ascending' });
    }

Direction
  = _ dir:(AscToken / DescToken) { return dir; }

LimitClause
  = _ LimitToken _ limit:Number
    { return new LimitAction({ limit: limit }); }

QueryTerminator
  = _ ";"

Rest
  = __ rest:$(.*)
    { return rest; }

/*
Expressions are defined below in acceding priority order

  Or (OR)
  And (AND)
  Not (NOT)
  Comparison (=, <, >, <=, >=, <>, !=, IS, LIKE, BETWEEN, IN, CONTAINS, REGEXP)
  Additive (+, -)
  Multiplicative (*), Division (/)
  Unary identity (+), negation (-)
*/

Expression = OrExpression


OrExpression
  = head:AndExpression tail:(_ OrToken _ AndExpression)*
    { return naryExpressionFactory('or', head, tail); }


AndExpression
  = head:NotExpression tail:(_ AndToken _ NotExpression)*
    { return naryExpressionFactory('and', head, tail); }


NotExpression
  = NotToken _ ex:ComparisonExpression { return ex.not(); }
  / ComparisonExpression


ComparisonExpression
  = lhs:AdditiveExpression not:(_ NotToken)? __ BetweenToken __ start:LiteralExpression __ AndToken __ end:LiteralExpression
    {
      var ex = lhs.in({ start: start.value, end: end.value, bounds: '[]' });
      if (not) ex = ex.not();
      return ex;
    }
  / lhs:AdditiveExpression _ IsToken not:(_ NotToken)? _ rhs:LiteralExpression
    {
      var ex = lhs.is(rhs);
      if (not) ex = ex.not();
      return ex;
    }
  / lhs:AdditiveExpression not:(_ NotToken)? _ InToken _ list:ListLiteral
    {
      var ex = lhs.in(list);
      if (not) ex = ex.not();
      return ex;
    }
  / lhs:AdditiveExpression not:(_ NotToken)? _ ContainsToken _ string:String
    {
      var ex = lhs.contains(string, 'ignoreCase');
      if (not) ex = ex.not();
      return ex;
    }
  / lhs:AdditiveExpression not:(_ NotToken)? _ LikeToken _ string:String escape:(_ EscapeToken _ String)?
    {
      var escapeStr = escape ? escape[3] : '\\';
      if (escapeStr.length > 1) error('Invalid escape string: ' + escapeStr);
      var ex = lhs.match(MatchAction.likeToRegExp(string, escapeStr));
      if (not) ex = ex.not();
      return ex;
    }
  / lhs:AdditiveExpression not:(_ NotToken)? _ RegExpToken _ string:String
    {
      var ex = lhs.match(string);
      if (not) ex = ex.not();
      return ex;
    }
  / lhs:AdditiveExpression rest:(_ ComparisonOp _ AdditiveExpression)?
    {
      if (!rest) return lhs;
      return lhs[rest[1]](rest[3]);
    }

ComparisonOp
  = "="  { return 'is'; }
  / "<>" { return 'isnt'; }
  / "!=" { return 'isnt'; }
  / "<=" { return 'lessThanOrEqual'; }
  / ">=" { return 'greaterThanOrEqual'; }
  / "<"  { return 'lessThan'; }
  / ">"  { return 'greaterThan'; }

ListLiteral
  = "(" head:StringOrNumber tail:(Comma StringOrNumber)* ")"
    { return r(Set.fromJS(makeListMap1(head, tail))); }


AdditiveExpression
  = head:MultiplicativeExpression tail:(_ AdditiveOp _ MultiplicativeExpression)*
    { return naryExpressionWithAltFactory('add', head, tail, '-', 'subtract'); }

AdditiveOp = op:[+-] ![+] { return op; }


MultiplicativeExpression
  = head:UnaryExpression tail:(_ MultiplicativeOp _ UnaryExpression)*
    { return naryExpressionWithAltFactory('multiply', head, tail, '/', 'divide'); }

MultiplicativeOp = [*/]


UnaryExpression
  = op:AdditiveOp _ !Number ex:BasicExpression
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
  / OpenParen _ ex:Expression CloseParen { return ex; }
  / OpenParen _ selectSubQuery:SelectSubQuery CloseParen { return selectSubQuery; }
  / RefExpression


AggregateExpression
  = CountToken OpenParen distinct:Distinct? _ ex:(StarToken / Expression)? CloseParen
    {
      if (!ex || ex === '*') {
        if (distinct) error('COUNT DISTINCT must have expression');
        return dataRef.count();
      } else {
        return distinct ? dataRef.countDistinct(ex) : dataRef.filter(ex.isnt(null)).count()
      }
    }
  / fn:AggregateFn OpenParen distinct:Distinct? _ ex:Expression CloseParen
    {
      if (distinct) error('can not use DISTINCT for ' + fn + ' aggregator');
      return dataRef[fn](ex);
    }
  / QuantileToken OpenParen _ ex:Expression Comma value: Number CloseParen
    { return dataRef.quantile(ex, value); }
  / CustomToken OpenParen _ value: String CloseParen
    { return dataRef.custom(value); }

AggregateFn
  = SumToken / AvgToken / MinToken / MaxToken / CountDistinctToken


FunctionCallExpression
  = NumberBucketToken OpenParen _ operand:Expression Comma size:Number Comma offset:Number CloseParen
    { return operand.numberBucket(size, offset); }
  / fn:(TimeBucketToken / TimeFloorToken) OpenParen _ operand:Expression Comma duration:NameOrString timezone:TimezoneParameter? CloseParen
    { return operand[fn](duration, timezone); }
  / TimePartToken OpenParen _ operand:Expression Comma part:NameOrString timezone:TimezoneParameter? CloseParen
    { return operand.timePart(part, timezone); }
  / fn:(TimeShiftToken / TimeRangeToken) OpenParen _ operand:Expression Comma duration:NameOrString Comma step:Number timezone:TimezoneParameter? CloseParen
    { return operand[fn](duration, step, timezone); }
  / SubstrToken OpenParen _ operand:Expression Comma position:Number Comma length:Number CloseParen
    { return operand.substr(position, length); }
  / ExtractToken OpenParen _ operand:Expression Comma regexp:String CloseParen
    { return operand.extract(regexp); }
  / LookupToken OpenParen _ operand:Expression Comma lookup:String CloseParen
    { return operand.lookup(lookup); }
  / ConcatToken OpenParen head:Expression tail:(Comma Expression)* CloseParen
    { return Expression.concat(makeListMap1(head, tail)); }
  / (IfNullToken/FallbackToken) OpenParen _ operand:Expression Comma _ fallbackValue:Expression CloseParen
    { return operand.fallback(fallbackValue);}
  / MatchToken OpenParen operand:Expression Comma regexp:String CloseParen
    { return operand.match(regexp); }
  / NowToken OpenParen CloseParen
    { return r(new Date()); }
  / (AbsToken/AbsoluteToken ) OpenParen _ operand:Expression _ CloseParen
    { return operand.absolute(); }
  / (PowToken/PowerToken) OpenParen _ operand:Expression Comma exponent:Number CloseParen
    { return operand.power(exponent); }
  / ExpToken OpenParen _ exponent:Expression CloseParen
    { return r(Math.E).power(exponent); }
  / SqrtToken OpenParen _ operand:Expression CloseParen
    { return operand.power(0.5); }
  / OverlapToken OpenParen _ lhs:Expression Comma rhs:Expression CloseParen
    { return lhs.overlap(rhs); }

TimezoneParameter
  = Comma timezone:NameOrString { return timezone }

RefExpression
  = ref:NamespacedRef { return $(ref); }

NamespacedRef
  = (Ref _ "." _)? name:Ref
    {
      return name; // ToDo: do not ignore namespace
    }

Ref
  = name:Name !{ return reserved(name); }
    { return name }
  / "`" name:$([^`]+) "`"
    { return name }

NameOrString = Name / String

StringOrNumber = String / Number


LiteralExpression
  = OpenCurly _ type:(DToken / TToken / TsToken) _ v:String CloseCurly
    { return r(makeDate(type, v)); }
  / type:(DateToken / TimeToken / TimestampToken) _ v:String
    { return r(makeDate(type, v)); }
  / number:Number
    { return r(number); }
  / string:String
    { return r(parseISODate(string) || string); }
  / v:(NullToken / TrueToken / FalseToken)
    { return r(v); }


String "String"
  = "'" chars:NotSQuote "'" { return chars; }
  / "'" chars:NotSQuote { error("Unmatched single quote"); }
  / '"' chars:NotDQuote '"' { return chars; }
  / '"' chars:NotDQuote { error("Unmatched double quote"); }

/* Tokens */

NullToken          = "NULL"i           !IdentifierPart { return null; }
TrueToken          = "TRUE"i           !IdentifierPart { return true; }
FalseToken         = "FALSE"i          !IdentifierPart { return false; }

SelectToken        = "SELECT"i         !IdentifierPart { return 'SELECT'; }
DescribeToken      = "DESCRIBE"i       !IdentifierPart { return 'DESCRIBE'; }
UpdateToken        = "UPDATE"i         !IdentifierPart { return 'UPDATE'; }
ShowToken          = "SHOW"i           !IdentifierPart { return 'SHOW'; }
SetToken           = "SET"i            !IdentifierPart { return 'SET'; }

FromToken          = "FROM"i           !IdentifierPart
AsToken            = "AS"i             !IdentifierPart
OnToken            = "ON"i             !IdentifierPart
LeftToken          = "LEFT"i           !IdentifierPart
InnerToken         = "INNER"i          !IdentifierPart
JoinToken          = "JOIN"i           !IdentifierPart
UnionToken         = "UNION"i          !IdentifierPart
WhereToken         = "WHERE"i          !IdentifierPart
GroupToken         = "GROUP"i          !IdentifierPart
ByToken            = "BY"i             !IdentifierPart
OrderToken         = "ORDER"i          !IdentifierPart
HavingToken        = "HAVING"i         !IdentifierPart
LimitToken         = "LIMIT"i          !IdentifierPart

AscToken           = "ASC"i            !IdentifierPart { return SortAction.ASCENDING;  }
DescToken          = "DESC"i           !IdentifierPart { return SortAction.DESCENDING; }

BetweenToken       = "BETWEEN"i        !IdentifierPart
InToken            = "IN"i             !IdentifierPart
IsToken            = "IS"i             !IdentifierPart
LikeToken          = "LIKE"i           !IdentifierPart
ContainsToken      = "CONTAINS"i       !IdentifierPart
RegExpToken        = "REGEXP"i         !IdentifierPart
EscapeToken        = "ESCAPE"i         !IdentifierPart

NotToken           = "NOT"i            !IdentifierPart
AndToken           = "AND"i            !IdentifierPart
OrToken            = "OR"i             !IdentifierPart

DistinctToken      = "DISTINCT"i       !IdentifierPart
StarToken          = "*"               !IdentifierPart { return '*'; }

AbsToken           = "ABS"i            !IdentifierPart { return 'absolute'; }
AbsoluteToken      = "ABSOLUTE"i       !IdentifierPart { return 'absolute'; }
CountToken         = "COUNT"i          !IdentifierPart { return 'count'; }
CountDistinctToken = "COUNT_DISTINCT"i !IdentifierPart { return 'countDistinct'; }
SumToken           = "SUM"i            !IdentifierPart { return 'sum'; }
AvgToken           = "AVG"i            !IdentifierPart { return 'average'; }
MinToken           = "MIN"i            !IdentifierPart { return 'min'; }
MaxToken           = "MAX"i            !IdentifierPart { return 'max'; }
PowerToken         = "POWER"i          !IdentifierPart { return 'power'; }
PowToken           = "POW"i            !IdentifierPart { return 'power'; }
ExpToken           = "EXP"i            !IdentifierPart { return 'power'; }
SqrtToken          = "SQRT"i           !IdentifierPart { return 'power'; }
QuantileToken      = "QUANTILE"i       !IdentifierPart { return 'quantile'; }
CustomToken        = "CUSTOM"i         !IdentifierPart { return 'custom'; }

TimeFloorToken     = "TIME_FLOOR"i     !IdentifierPart { return 'timeFloor'; }
TimeShiftToken     = "TIME_SHIFT"i     !IdentifierPart { return 'timeShift'; }
TimeRangeToken     = "TIME_RANGE"i     !IdentifierPart { return 'timeRange'; }
TimeBucketToken    = "TIME_BUCKET"i    !IdentifierPart { return 'timeBucket'; }
NumberBucketToken  = "NUMBER_BUCKET"i  !IdentifierPart { return 'numberBucket'; }
TimePartToken      = "TIME_PART"i      !IdentifierPart { return 'timePart'; }
SubstrToken        = "SUBSTR"i "ING"i? !IdentifierPart { return 'substr'; }
ExtractToken       = "EXTRACT"i        !IdentifierPart { return 'extract'; }
ConcatToken        = "CONCAT"i         !IdentifierPart { return 'concat'; }
LookupToken        = "LOOKUP"i         !IdentifierPart { return 'lookup'; }
IfNullToken        = "IFNULL"i         !IdentifierPart { return 'fallback'; }
FallbackToken      = "FALLBACK"i       !IdentifierPart { return 'fallback'; }
MatchToken         = "MATCH"i          !IdentifierPart { return 'match'; }
OverlapToken       = "OVERLAP"i        !IdentifierPart { return 'overlap'; }

NowToken           = "NOW"i            !IdentifierPart
DateToken          = "DATE"i           !IdentifierPart { return 'd'; }
TimeToken          = "TIME"i           !IdentifierPart { return 't'; }
TimestampToken     = "TIMESTAMP"i      !IdentifierPart { return 'ts'; }
DToken             = "D"i              !IdentifierPart { return 'd'; }
TToken             = "T"i              !IdentifierPart { return 't'; }
TsToken            = "TS"i             !IdentifierPart { return 'ts'; }

IdentifierPart = [A-Za-z_]

/* Numbers */

Number "Number"
  = n: $(Int Fraction? Exp?) { return parseFloat(n); }

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
  = "("

CloseParen ")"
  = _ ")"

OpenCurly "{"
  = "{"

CloseCurly "}"
  = _ "}"

Comma
  = _ "," _

Name "Name"
  = $([a-zA-Z_] [a-z0-9A-Z_]*)

NotSQuote "NotSQuote"
  = $([^']*)

NotDQuote "NotDQuote"
  = $([^"]*)

_ "Whitespace"
  = $ ([ \t\r\n] / SingleLineComment / InlineComment)*

__ "Mandatory Whitespace"
  = $ ([ \t\r\n] / SingleLineComment / InlineComment)+

InlineComment
  = "/*" (!CommentTerminator .)* CommentTerminator

SingleLineComment
  = ("-- " / "#") (!LineTerminator .)*

CommentTerminator
  = "*/"

LineTerminator
  = [\n\r]

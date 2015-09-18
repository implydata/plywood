{// starts with function(plywood)
var ply = plywood.ply;
var $ = plywood.$;
var r = plywood.r;
var Expression = plywood.Expression;
var FilterAction = plywood.FilterAction;
var ApplyAction = plywood.ApplyAction;
var SortAction = plywood.SortAction;
var LimitAction = plywood.LimitAction;

var dataRef = $('data');
var dateRegExp = /^\d\d\d\d-\d\d-\d\d(?:T(?:\d\d)?(?::\d\d)?(?::\d\d)?(?:.\d\d\d)?)?Z?$/;

// See here: https://www.drupal.org/node/141051
var reservedWords = {
  ALL: 1, AND: 1,  AS: 1, ASC: 1, AVG: 1,
  BETWEEN: 1, BY: 1,
  CONTAINS: 1, CREATE: 1,
  DELETE: 1, DESC: 1, DISTINCT: 1, DROP: 1,
  EXISTS: 1, EXPLAIN: 1,
  FALSE: 1, FROM: 1,
  GROUP: 1,
  HAVING: 1,
  IN: 1, INNER: 1,  INSERT: 1, INTO: 1, IS: 1,
  JOIN: 1,
  LEFT: 1, LIKE: 1, LIMIT: 1,
  MAX: 1, MIN: 1,
  NOT: 1, NULL: 1, NUMBER_BUCKET: 1,
  ON: 1, OR: 1, ORDER: 1,
  QUANTILE: 1,
  REPLACE: 1,
  SELECT: 1, SET: 1, SHOW: 1, SUM: 1,
  TABLE: 1, TIME_BUCKET: 1, TRUE: 1,
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

function extractGroupByColumn(columns, groupBy) {
  var label = null;
  var applyColumns = [];
  for (var i = 0; i < columns.length; i++) {
    var column = columns[i];
    if (groupBy.equals(column.expression)) {
      if (label) error('already have a label');
      label = column.name;
    } else {
      applyColumns.push(column);
    }
  }
  if (!label) label = 'split';
  return {
    label: label,
    applyColumns: applyColumns
  };
}

function constructQuery(columns, from, where, groupBy, having, orderBy, limit) {
  if (!columns) error('Can not have empty column list');
  from = from || dataRef;

  if (where) {
    from = from.filter(where);
  }

  // Support for not having a group by clause is there are aggregates in the columns
  // A redneck check for aggregate columns is the same as having "GROUP BY 1"
  if (!groupBy) {
    var hasAggregate = columns.some(function(column) {
      var columnExpression = column.expression;
      return columnExpression.isOp('chain') &&
        columnExpression.actions.length &&
        aggregates[columnExpression.actions[0].action];
    })
    if (hasAggregate) groupBy = Expression.ONE;
  }

  var query = null;
  if (!groupBy) {
    query = from;
  } else {
    if (groupBy.isOp('literal')) {
      query = ply().apply('data', from);
    } else {
      var extract = extractGroupByColumn(columns, groupBy);
      columns = extract.applyColumns;
      query = from.split(groupBy, extract.label, 'data');
    }
  }

  for (var i = 0; i < columns.length; i++) {
    query = query.performAction(columns[i]);
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

function naryExpressionFactory(op, head, tail) {
  if (!tail.length) return head;
  return head[op].apply(head, tail.map(function(t) { return t[3]; }));
}

function naryExpressionWithAltFactory(op, head, tail, altToken, altOp) {
  if (!tail.length) return head;
  for (var i = 0; i < tail.length; i++) {
    t = tail[i];
    head = head[t[1] === altToken ? altOp : op].call(head, t[3]);
  }
  return head;
}

}// Start grammar

start
  = _ query:SQLQuery _ { return query; }

SQLQuery
  = SelectToken columns:Columns? from:FromClause? where:WhereClause? groupBy:GroupByClause? having:HavingClause? orderBy:OrderByClause? limit:LimitClause?
    { return constructQuery(columns, from, where, groupBy, having, orderBy, limit); }

SQLSubQuery
  = SelectToken columns:Columns? where:WhereClause? groupBy:GroupByClause having:HavingClause? orderBy:OrderByClause? limit:LimitClause?
    { return constructQuery(columns, null, where, groupBy, having, orderBy, limit); }

Columns
  = _ head:Column tail:(_ "," _ Column)*
    { return [head].concat(tail.map(function(t) { return t[3] })); }

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

FromClause
  = _ FromToken _ table:RefExpression
    { return table; }

WhereClause
  = _ WhereToken _ filter:Expression
    { return filter; }

GroupByClause
  = _ GroupToken __ ByToken _ groupBy:Expression tail:(_ "," _ Expression)*
    {
      if (tail.length) error('plywood does not currently support multi-dimensional GROUP BYs');
      return groupBy;
    }

HavingClause
  = _ HavingToken _ having:Expression
    { return new FilterAction({ expression: having }); }

OrderByClause
  = _ OrderToken __ ByToken _ orderBy:Expression direction:Direction? tail:(_ "," _ Expression Direction?)*
    {
      if (tail.length) error('plywood does not currently support multi-column ORDER BYs');
      return new SortAction({ expression: orderBy, direction: direction || 'ascending' });
    }

Direction
  = _ dir:(AscToken / DescToken) { return dir; }

LimitClause
  = _ LimitToken _ limit:Number
    { return new LimitAction({ limit: limit }); }

/*
Expressions are defined below in acceding priority order

  Or (OR)
  And (AND)
  Not (NOT)
  Comparison (=, <, >, <=, >=, <>, !=, IS, LIKE, BETWEEN, IN, CONTAINS)
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
  = lhs:AdditiveExpression __ BetweenToken __ start:AdditiveExpression __ AndToken __ end:AdditiveExpression
    {
      if (start.op !== 'literal') error('between start must be a literal');
      if (end.op !== 'literal') error('between end must be a literal');
      return lhs.in({ start: start.value, end: end.value, bounds: '[]' });
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
  / "in" { return 'in'; }
  / "<=" { return 'lessThanOrEqual'; }
  / ">=" { return 'greaterThanOrEqual'; }
  / "<"  { return 'lessThan'; }
  / ">"  { return 'greaterThan'; }


AdditiveExpression
  = head:MultiplicativeExpression tail:(_ AdditiveOp _ MultiplicativeExpression)*
    { return naryExpressionWithAltFactory('add', head, tail, '-', 'subtract'); }

AdditiveOp = op:[+-] ![+] { return op; }


MultiplicativeExpression
  = head:UnaryExpression tail:(_ MultiplicativeOp _ UnaryExpression)*
    { return naryExpressionWithAltFactory('multiply', head, tail, '/', 'divide'); }

MultiplicativeOp = [*/]


UnaryExpression
  = op:AdditiveOp _ ex:BasicExpression
    {
      var negEx = ex.negate(); // Always negate (even with +) just to make sure it is possible
      return op === '-' ? negEx : ex;
    }
  / BasicExpression


BasicExpression
  = LiteralExpression
  / AggregateExpression
  / FunctionCallExpression
  / "(" _ ex:Expression _ ")" { return ex; }
  / "(" _ subQuery:SQLSubQuery _ ")" { return subQuery; }
  / RefExpression


AggregateExpression
  = CountToken "(" _ (StarToken / Expression)? _ ")"
    { return dataRef.count(); }
  / CountToken "(" _ DistinctToken _ ex:Expression _ ")"
    { return dataRef.countDistinct(ex); }
  / fn:AggregateFn "(" _ ex:Expression _ ")"
    { return dataRef[fn](ex); }
  / QuantileToken "(" _ ex:Expression _ "," _ value: Number ")"
    { return dataRef.quantile(ex, value); }
  / CustomToken "(" _ value: String ")"
    { return dataRef.custom(value); }

AggregateFn
  = SumToken / AvgToken / MinToken / MaxToken / CountDistinctToken


FunctionCallExpression
  = NumberBucketToken "(" _ operand:Expression _ "," _ size:Number _ "," _ offset:Number ")"
    { return operand.numberBucket(size, offset); }
  / TimeBucketToken "(" _ operand:Expression _ "," _ duration:NameOrString _ "," _ timezone:NameOrString ")"
    { return operand.timeBucket(duration, timezone); }
  / TimePartToken "(" _ operand:Expression _ "," _ part:NameOrString _ "," _ timezone:NameOrString ")"
      { return operand.timePart(part, timezone); }
  / SubstrToken "(" _ operand:Expression _ "," _ position:Number _ "," _ length:Number ")"
    { return operand.substr(position, length); }


RefExpression
  = ref:Ref { return $(ref); }

Ref
  = name:Name !{ return reserved(name); }
    { return name }
  / "`" name:$([^`]+) "`"
    { return name }

NameOrString = Name / String


LiteralExpression
  = number:Number { return r(number); }
  / string:String
    {
      if (dateRegExp.test(string)) {
        var date = new Date(string);
        if (!isNaN(date)) {
          return r(date);
        } else {
          return r(string);
        }
      } else {
        return r(string);
      }
    }
  / v:(NullToken / TrueToken / FalseToken) { return r(v); }


String "String"
  = "'" chars:NotSQuote "'" { return chars; }
  / "'" chars:NotSQuote { error("Unmatched single quote"); }
  / '"' chars:NotDQuote '"' { return chars; }
  / '"' chars:NotDQuote { error("Unmatched double quote"); }

/* Tokens */

NullToken          = "NULL"i           !IdentifierPart { return null; }
TrueToken          = "TRUE"i           !IdentifierPart { return true; }
FalseToken         = "FALSE"i          !IdentifierPart { return false; }

SelectToken        = "SELECT"i         !IdentifierPart
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

AscToken           = "ASC"i            !IdentifierPart { return 'ascending';  }
DescToken          = "DESC"i           !IdentifierPart { return 'descending'; }

BetweenToken       = "BETWEEN"i        !IdentifierPart
InToken            = "IN"i             !IdentifierPart
IsToken            = "IS"i             !IdentifierPart
LikeToken          = "LIKE"i           !IdentifierPart
ContainsToken      = "CONTAINS"i       !IdentifierPart

NotToken           = "NOT"i            !IdentifierPart
AndToken           = "AND"i            !IdentifierPart
OrToken            = "OR"i             !IdentifierPart

DistinctToken      = "DISTINCT"i       !IdentifierPart
StarToken          = "*"               !IdentifierPart

CountToken         = "COUNT"i          !IdentifierPart { return 'count'; }
CountDistinctToken = "COUNT_DISTINCT"i !IdentifierPart { return 'countDistinct'; }
SumToken           = "SUM"i            !IdentifierPart { return 'sum'; }
AvgToken           = "AVG"i            !IdentifierPart { return 'average'; }
MinToken           = "MIN"i            !IdentifierPart { return 'min'; }
MaxToken           = "MAX"i            !IdentifierPart { return 'max'; }
QuantileToken      = "QUANTILE"i       !IdentifierPart { return 'quantile'; }
CustomToken        = "CUSTOM"i         !IdentifierPart { return 'custom'; }

TimeBucketToken    = "TIME_BUCKET"i    !IdentifierPart
NumberBucketToken  = "NUMBER_BUCKET"i  !IdentifierPart
TimePartToken      = "TIME_PART"i      !IdentifierPart
SubstrToken        = "SUBSTR"i         !IdentifierPart
ConcatToken        = "CONCAT"i         !IdentifierPart

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

Name "Name"
  = $([a-zA-Z_] [a-z0-9A-Z_]*)

NotSQuote "NotSQuote"
  = $([^']*)

NotDQuote "NotDQuote"
  = $([^"]*)

_ "Whitespace"
  = $ ([ \t\r\n] / SingleLineComment)*

__ "Mandatory Whitespace"
  = $ ([ \t\r\n] / SingleLineComment)+

SingleLineComment
  = "--" (!LineTerminator .)*

LineTerminator
  = [\n\r]

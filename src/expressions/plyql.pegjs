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

var objectHasOwnProperty = Object.prototype.hasOwnProperty;
function reserved(str) {
  return objectHasOwnProperty.call(reservedWords, str.toUpperCase());
}

function makeDate(type, v) {
  try {
    return chronoshift.parseSQLDate(type, v);
  } catch (e) {
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
          columnExpression.actions.some(function(action) { return action.isAggregate(); })
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

function makeList(head, tail) {
  return [head].concat(tail);
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
  = _ queryParse:SupportedQuery QueryTerminator? { return queryParse; }

SupportedQuery
  = queryParse:SelectQuery { return queryParse; }
  / queryParse:DescribeQuery { return queryParse; }
  / queryParse:OtherQuery { return queryParse; }
  / ex:Expression
    {
      return {
        verb: null,
        expression: ex
      }
    }

OtherQuery
  = verb:(UpdateToken / ShowToken / SetToken) rest:$(.*)
    {
      return {
        verb: verb,
        rest: rest
      };
    }

DescribeQuery
  = DescribeToken table:RelaxedNamespacedRef
    {
      return {
        verb: 'DESCRIBE',
        table: table.name
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
  = GroupToken ByToken head:Expression tail:ExpressionParameter*
    { return makeList(head, tail); }

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
  = NumberBucketToken OpenParen operand:Expression size:NumberParameter offset:NumberParameter CloseParen
    { return operand.numberBucket(size, offset); }
  / fn:(TimeBucketToken / TimeFloorToken) OpenParen operand:Expression duration:NameOrStringParameter timezone:NameOrStringParameter? CloseParen
    { return operand[fn](duration, timezone); }
  / TimePartToken OpenParen operand:Expression part:NameOrStringParameter timezone:NameOrStringParameter? CloseParen
    { return operand.timePart(part, timezone); }
  / fn:(TimeShiftToken / TimeRangeToken) OpenParen operand:Expression duration:NameOrStringParameter step:NumberParameter timezone:NameOrStringParameter? CloseParen
    { return operand[fn](duration, step, timezone); }
  / SubstrToken OpenParen operand:Expression position:NumberParameter length:NumberParameter CloseParen
    { return operand.substr(position, length); }
  / ExtractToken OpenParen operand:Expression regexp:StringParameter CloseParen
    { return operand.extract(regexp); }
  / LookupToken OpenParen operand:Expression lookup:StringParameter CloseParen
    { return operand.lookup(lookup); }
  / ConcatToken OpenParen head:Expression tail:ExpressionParameter* CloseParen
    { return Expression.concat(makeList(head, tail)); }
  / (IfNullToken/FallbackToken) OpenParen operand:Expression fallbackValue:ExpressionParameter CloseParen
    { return operand.fallback(fallbackValue);}
  / MatchToken OpenParen operand:Expression regexp:StringParameter CloseParen
    { return operand.match(regexp); }
  / NowToken OpenParen CloseParen
    { return r(new Date()); }
  / (AbsToken/AbsoluteToken ) OpenParen operand:Expression CloseParen
    { return operand.absolute(); }
  / (PowToken/PowerToken) OpenParen operand:Expression exponent:NumberParameter CloseParen
    { return operand.power(exponent); }
  / ExpToken OpenParen exponent:Expression CloseParen
    { return r(Math.E).power(exponent); }
  / SqrtToken OpenParen operand:Expression CloseParen
    { return operand.power(0.5); }
  / OverlapToken OpenParen lhs:Expression rhs:ExpressionParameter CloseParen
    { return lhs.overlap(rhs); }

ExpressionParameter = Comma ex:Expression { return ex; }

NumberParameter = Comma num:Number { return num; }

StringParameter = Comma str:String { return str; }

NameOrStringParameter = Comma timezone:NameOrString { return timezone; }


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

/* Tokens */

NullToken          = "NULL"i           !IdentifierPart _ { return null; }
TrueToken          = "TRUE"i           !IdentifierPart _ { return true; }
FalseToken         = "FALSE"i          !IdentifierPart _ { return false; }

SelectToken        = "SELECT"i         !IdentifierPart _ { return 'SELECT'; }
DescribeToken      = "DESCRIBE"i       !IdentifierPart _ { return 'DESCRIBE'; }
UpdateToken        = "UPDATE"i         !IdentifierPart _ { return 'UPDATE'; }
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

AbsToken           = "ABS"i            !IdentifierPart _ { return 'absolute'; }
AbsoluteToken      = "ABSOLUTE"i       !IdentifierPart _ { return 'absolute'; }
CountToken         = "COUNT"i          !IdentifierPart _ { return 'count'; }
CountDistinctToken = "COUNT_DISTINCT"i !IdentifierPart _ { return 'countDistinct'; }
SumToken           = "SUM"i            !IdentifierPart _ { return 'sum'; }
AvgToken           = "AVG"i            !IdentifierPart _ { return 'average'; }
MinToken           = "MIN"i            !IdentifierPart _ { return 'min'; }
MaxToken           = "MAX"i            !IdentifierPart _ { return 'max'; }
PowerToken         = "POWER"i          !IdentifierPart _ { return 'power'; }
PowToken           = "POW"i            !IdentifierPart _ { return 'power'; }
ExpToken           = "EXP"i            !IdentifierPart _ { return 'power'; }
SqrtToken          = "SQRT"i           !IdentifierPart _ { return 'power'; }
QuantileToken      = "QUANTILE"i       !IdentifierPart _ { return 'quantile'; }
CustomToken        = "CUSTOM"i         !IdentifierPart _ { return 'custom'; }

TimeFloorToken     = "TIME_FLOOR"i     !IdentifierPart _ { return 'timeFloor'; }
TimeShiftToken     = "TIME_SHIFT"i     !IdentifierPart _ { return 'timeShift'; }
TimeRangeToken     = "TIME_RANGE"i     !IdentifierPart _ { return 'timeRange'; }
TimeBucketToken    = "TIME_BUCKET"i    !IdentifierPart _ { return 'timeBucket'; }
NumberBucketToken  = "NUMBER_BUCKET"i  !IdentifierPart _ { return 'numberBucket'; }
TimePartToken      = "TIME_PART"i      !IdentifierPart _ { return 'timePart'; }
SubstrToken        = "SUBSTR"i "ING"i? !IdentifierPart _ { return 'substr'; }
ExtractToken       = "EXTRACT"i        !IdentifierPart _ { return 'extract'; }
ConcatToken        = "CONCAT"i         !IdentifierPart _ { return 'concat'; }
LookupToken        = "LOOKUP"i         !IdentifierPart _ { return 'lookup'; }
IfNullToken        = "IFNULL"i         !IdentifierPart _ { return 'fallback'; }
FallbackToken      = "FALLBACK"i       !IdentifierPart _ { return 'fallback'; }
MatchToken         = "MATCH"i          !IdentifierPart _ { return 'match'; }
OverlapToken       = "OVERLAP"i        !IdentifierPart _ { return 'overlap'; }

NowToken           = "NOW"i            !IdentifierPart _
DateToken          = "DATE"i           !IdentifierPart _ { return 'd'; }
TimeToken          = "TIME"i           !IdentifierPart _ { return 't'; }
TimestampToken     = "TIMESTAMP"i      !IdentifierPart _ { return 'ts'; }
DToken             = "D"i              !IdentifierPart _ { return 'd'; }
TToken             = "T"i              !IdentifierPart _ { return 't'; }
TsToken            = "TS"i             !IdentifierPart _ { return 'ts'; }

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

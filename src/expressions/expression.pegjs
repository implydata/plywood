{// starts with function(plywood)
var $ = plywood.$;
var Expression = plywood.Expression;
var LiteralExpression = plywood.LiteralExpression;
var RefExpression = plywood.RefExpression;

var possibleCalls = {
  'is': 1,
  'in': 1,
  'lessThanOrEqual': 1,
  'greaterThanOrEqual': 1,
  'lessThan': 1,
  'greaterThan': 1,
  'add': 1,
  'multiply': 1,
  'subtract': 1,
  'divide': 1,
  'not': 1,
  'match': 1,
  'numberBucket': 1,
  'timeBucket': 1,
  'substr': 1,
  'timePart': 1,
  'filter': 1,
  'apply': 1,
  'sort': 1,
  'limit': 1,
  'count': 1,
  'countDistinct': 1,
  'sum': 1,
  'max': 1,
  'min': 1,
  'average': 1,
  'quantile': 1,
  'split': 1
};

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
  = _ ex:Expression _ { return ex; }

/*
Expressions are defined below in acceding priority order

  Or (or)
  And (and)
  Not (not)
  Comparison (=, <, >, <=, >=, <>, !=, in)
  Additive (+, -)
  Multiplicative (*), Division (/)
  CallChain ( $x.filter(...) )
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
  = lhs:ConcatenationExpression rest:(_ ComparisonOp _ ConcatenationExpression)?
    {
      if (!rest) return lhs;
      return lhs[rest[1]](rest[3]);
    }

ComparisonOp
  = "="  { return 'is'; }
  / "!=" { return 'isnt'; }
  / "in" { return 'in'; }
  / "<=" { return 'lessThanOrEqual'; }
  / ">=" { return 'greaterThanOrEqual'; }
  / "<"  { return 'lessThan'; }
  / ">"  { return 'greaterThan'; }


ConcatenationExpression
  = head:AdditiveExpression tail:(_ ConcatToken _ AdditiveExpression)*
      { return naryExpressionFactory('concat', head, tail); }


AdditiveExpression
  = head:MultiplicativeExpression tail:(_ AdditiveOp _ MultiplicativeExpression)*
    { return naryExpressionWithAltFactory('add', head, tail, '-', 'subtract'); }

AdditiveOp = op:[+-] ![+] { return op; }


MultiplicativeExpression
  = head:UnaryExpression tail:(_ MultiplicativeOp _ UnaryExpression)*
    { return naryExpressionWithAltFactory('multiply', head, tail, '/', 'divide'); }

MultiplicativeOp = [*/]


UnaryExpression
  = op:AdditiveOp _ ex:CallChainExpression
    {
      var negEx = ex.negate(); // Always negate (even with +) just to make sure it is possible
      return op === '-' ? negEx : ex;
    }
  / CallChainExpression


CallChainExpression
  = lhs:BasicExpression tail:(_ "." _ CallFn "(" _ Params? _ ")")*
    {
      if (!tail.length) return lhs;
      var operand = lhs;
      for (var i = 0, n = tail.length; i < n; i++) {
        var part = tail[i];
        var op = part[3];
        if (!possibleCalls[op]) error('no such call: ' + op);
        var params = part[6] || [];
        operand = operand[op].apply(operand, params);
      }
      return operand;
    }

Params
  = head:Param tail:(_ "," _ Param)*
    { return [head].concat(tail.map(function(t) { return t[3] })); }

Param
  = Number / Name / String / Expression


BasicExpression
  = "(" _ ex:Expression _ ")" { return ex; }
  / LiteralExpression
  / RefExpression
  / "$()" { return $(); }


RefExpression
  = "$" name:$("^"* SimpleName (":" TypeName)?)
    { return RefExpression.parse(name); }
  / "$" name:$("^"* "{" [^}]+ "}" (":" TypeName)?)
    { return RefExpression.parse(name); }

LiteralExpression
  = value:Number { return new LiteralExpression({ value: value }); }
  / value:String { return new LiteralExpression({ value: value }); }
  / value:(NullToken/FalseToken/TrueToken) { return new LiteralExpression({ value: value }); }


String "String"
  = "'" chars:NotSQuote "'" { return chars; }
  / "'" chars:NotSQuote { error("Unmatched single quote"); }
  / '"' chars:NotDQuote '"' { return chars; }
  / '"' chars:NotDQuote { error("Unmatched double quote"); }


/* Tokens */

NullToken         = "null"    !IdentifierPart { return null; }
FalseToken        = "false"   !IdentifierPart { return false; }
TrueToken         = "true"    !IdentifierPart { return true; }

NotToken          = "not"     !IdentifierPart
AndToken          = "and"     !IdentifierPart
OrToken           = "or"      !IdentifierPart
ConcatToken       = "++"      !IdentifierPart

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

CallFn "CallFn"
  = $([a-zA-Z]+)

Name "Name"
  = $([a-z0-9A-Z_]+)

SimpleName "Simple Name"
  = $([a-zA-Z_] [a-z0-9A-Z_]*)

TypeName "TypeName"
  = $([A-Z_/]+)

NotSQuote "NotSQuote"
  = $([^']*)

NotDQuote "NotDQuote"
  = $([^"]*)

_ "Whitespace"
  = $([ \t\r\n]*)

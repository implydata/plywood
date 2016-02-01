{// starts with function(plywood)
var ply = plywood.ply;
var $ = plywood.$;
var r = plywood.r;
var Expression = plywood.Expression;
var LiteralExpression = plywood.LiteralExpression;
var RefExpression = plywood.RefExpression;
var Set = plywood.Set;
var Action = plywood.Action;

var possibleCalls = {};
for (var key in Action.classMap) possibleCalls[key] = 1;
possibleCalls['negate'] = 1;
possibleCalls['isnt'] = 1;

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
  = "==" { return 'is'; }
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
  = op:AdditiveOp _ !Number ex:CallChainExpression
    {
      // !Number is to make sure that -3 parses as literal(-3) and not literal(3).negate()
      var negEx = ex.negate(); // Always negate (even with +) just to make sure it is possible
      return op === '-' ? negEx : ex;
    }
  / CallChainExpression


CallChainExpression
  = lhs:BasicExpression tail:(_ "." _ CallFn OpenParen _ Params? CloseParen)*
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
  = head:Expression tail:(Comma Expression)*
    { return makeListMap1(head, tail); }

BasicExpression
  = OpenParen _ ex:Expression CloseParen { return ex; }
  / "ply" OpenParen CloseParen { return ply(); }
  / RefExpression
  / LiteralExpression


RefExpression
  = "$" name:$("^"* Name (":" TypeName)?)
    { return RefExpression.parse(name); }
  / "$" name:$("^"* "{" [^}]+ "}" (":" TypeName)?)
    { return RefExpression.parse(name); }

LiteralExpression
  = value:(NullToken/FalseToken/TrueToken) { return r(value); }
  / value:Number { return r(value); }
  / value:Name { return r(value); }
  / value:String { return r(value); }
  / value:StringSet { return r(value); }
  / value:NumberSet { return r(value); }


StringSet "StringSet"
  = "[" head:StringOrNull tail:(Comma StringOrNull)* "]"
    { return Set.fromJS(makeListMap1(head, tail)); }

StringOrNull = String / NullToken;


NumberSet "NumberSet"
  = "[" head:NumberOrNull tail:(Comma NumberOrNull)* "]"
    { return Set.fromJS(makeListMap1(head, tail)); }

NumberOrNull = Number / NullToken;


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

OpenParen "("
  = "("

CloseParen ")"
  = _ ")"

Comma
  = _ "," _

ReservedWord
  = ( "ply" / "false" / "true" ) ![A-Za-z_]

CallFn "CallFn"
  = $([a-zA-Z]+)

Name "Name"
  = $(!ReservedWord [a-zA-Z_] [a-z0-9A-Z_]*)

TypeName "TypeName"
  = $([A-Z_/]+)

NotSQuote "NotSQuote"
  = $([^']*)

NotDQuote "NotDQuote"
  = $([^"]*)

_ "Whitespace"
  = $([ \t\r\n]*)

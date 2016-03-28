{// starts with function(plywood, chronoshift)
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
possibleCalls['sqrt'] = 1;

function makeListMap1(head, tail) {
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
  = _ ex:Expression { return ex; }

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
  = lhs:ConcatExpression rest:(ComparisonOp _ ConcatExpression)?
    {
      if (!rest) return lhs;
      return lhs[rest[0]](rest[2]);
    }

ComparisonOp "Comparison"
  = "==" { return 'is'; }
  / "!=" { return 'isnt'; }
  / "in" { return 'in'; }
  / "<=" { return 'lessThanOrEqual'; }
  / ">=" { return 'greaterThanOrEqual'; }
  / "<"  { return 'lessThan'; }
  / ">"  { return 'greaterThan'; }


ConcatExpression
  = head:AdditiveExpression tail:(ConcatToken AdditiveExpression)*
    { return naryExpressionFactory('concat', head, tail); }


AdditiveExpression
  = head:MultiplicativeExpression tail:(AdditiveOp MultiplicativeExpression)*
    { return naryExpressionWithAltFactory('add', head, tail, '-', 'subtract'); }

AdditiveOp = op:("+" / "-") !"+" _ { return op; }


MultiplicativeExpression
  = head:ExponentialExpression tail:(MultiplicativeOp ExponentialExpression)*
    { return naryExpressionWithAltFactory('multiply', head, tail, '/', 'divide'); }

MultiplicativeOp = op:("*" / "/") _ { return op; }


ExponentialExpression
  = ex:UnaryExpression rhs:(ExponentialOp ExponentialExpression)?
    {
      if (rhs) ex = ex.power(rhs[1]);
      return ex;
    }

ExponentialOp = "^" _


UnaryExpression
  = op:AdditiveOp _ !Number ex:CallChainExpression
    {
      // !Number is to make sure that -3 parses as literal(-3) and not literal(3).negate()
      var negEx = ex.negate(); // Always negate (even with +) just to make sure it is possible
      return op === '-' ? negEx : ex;
    }
  / CallChainExpression
  / Pipe _ ex:Expression _ Pipe { return ex.absolute(); }


CallChainExpression
  = lhs:BasicExpression tail:(Dot Name OpenParen Params? CloseParen)*
    {
      if (!tail.length) return lhs;
      var operand = lhs;
      for (var i = 0, n = tail.length; i < n; i++) {
        var part = tail[i];
        var op = part[1];
        if (!possibleCalls[op]) error('no such call: ' + op);
        var params = part[3] || [];
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
  = "$" name:$("^"* NamePart (":" TypeName)?) _
    { return RefExpression.parse(name); }
  / "$" name:$("^"* "{" [^}]+ "}" (":" TypeName)?) _
    { return RefExpression.parse(name); }

LiteralExpression
  = value:(NullToken / FalseToken / TrueToken) { return r(value); }
  / value:Number { return r(value); }
  / value:Name { return r(value); }
  / value:String { return r(value); }
  / value:StringSet { return r(value); }
  / value:NumberSet { return r(value); }


StringSet "StringSet"
  = OpenBracket head:StringOrNull tail:(Comma StringOrNull)* CloseBracket
    { return Set.fromJS(makeListMap1(head, tail)); }

StringOrNull
  = String / NullToken;


NumberSet "NumberSet"
  = OpenBracket head:NumberOrNull tail:(Comma NumberOrNull)* CloseBracket
    { return Set.fromJS(makeListMap1(head, tail)); }

NumberOrNull = Number / NullToken;


String "String"
  = "'" chars:NotSQuote "'" _ { return chars; }
  / "'" chars:NotSQuote { error("Unmatched single quote"); }
  / '"' chars:NotDQuote '"' _ { return chars; }
  / '"' chars:NotDQuote { error("Unmatched double quote"); }


/* Tokens */

NullToken         = "null"    !IdentifierPart _ { return null; }
FalseToken        = "false"   !IdentifierPart _ { return false; }
TrueToken         = "true"    !IdentifierPart _ { return true; }

NotToken          = "not"     !IdentifierPart _
AndToken          = "and"     !IdentifierPart _
OrToken           = "or"      !IdentifierPart _
ConcatToken       = "++"      !IdentifierPart _

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

OpenBracket "["
  = "[" _

CloseBracket "]"
  = "]" _

OpenParen "("
  = "(" _

CloseParen ")"
  = ")" _

Comma
  = "," _

Dot
  = "." _

Pipe
  = "|" _

ReservedWord
  = ( "ply" / "false" / "true" ) ![a-z_]i

Name "Name"
  = name:NamePart _ { return name; }

NamePart
  = $(!ReservedWord [a-z_]i [a-z0-9_]i*)

TypeName "TypeName"
  = $([A-Z_/]+)

NotSQuote "NotSQuote"
  = $([^']*)

NotDQuote "NotDQuote"
  = $([^"]*)

_ "Whitespace"
  = $([ \t\r\n]*)

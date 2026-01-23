from typing import cast
from lark import Transformer, v_args
from .lib import expr_nodes as node

import operator as pyops

import logging
logger = logging.getLogger(__name__)
from .lib.types import value as value


@v_args(inline=True)
class ExprTransformer(Transformer) :
    def value(self, x) -> node.ExprNode :
        return x

    def col_name(self, x) :
        logger.debug(f"col_name: {x}")
        if x.type == "DQ_STR" :
            return node.ColumnName(x.value[1:-1])
        return node.ColumnName(x.value)

    def lit_str(self, x) :
        return node.Literal(x.value[1:-1], 'str')
    def lit_int(self, x) :
        return node.Literal(int(x.value), 'int')
    def lit_float(self, x) :
        return node.Literal(float(x.value), 'float')

    def true(self) :
        return node.Literal(True, 'bool')
    def false(self) :
        return node.Literal(False, 'bool')
    def null(self) :
        return node.Literal(None, 'int')

    def add(self, x, y) :
        return node.Operation('math', pyops.add, x, y)
    def sub(self, x, y) :
        return node.Operation('math', pyops.sub, x, y)
    def mul(self, x, y) :
        return node.Operation('math', pyops.mul, x, y)
    def div(self, x, y) :
        return node.Operation('math', pyops.truediv, x, y)
    def mod(self, x, y) :
        return node.Operation('math', pyops.mod, x, y)

    def bnot(self, x) :
        return node.MonoOperation(value.v_not, x)
    def negative(self, x) :
        return node.MonoOperation(value.v_negate, x)

    def isnull(self, x) :
        return node.MonoOperation(value.v_isnull, x)

    def isnotnull(self, x) :
        return node.MonoOperation(value.v_not, node.MonoOperation(value.v_isnull, x))

    def relop(self, left, op, right) :
        return node.Operation('rel', op, left, right)

    def logop(self, left, op, right) :
        return node.Operation('log', op, left, right)

    def nvl(self, *args) :
        return node.NVLOp(*args)

    def substring(self, arg, start, length = None) :
        return node.Substring(arg, start, length)

    def strlen(self, x) :
        return node.MonoOperation(value.v_strlen, x)

    def upper(self, x) :
        return node.MonoOperation(value.v_upper, x)

    def lower(self, x) :
        return node.MonoOperation(value.v_lower, x)

    def tostr(self, x) :
        return node.MonoOperation(value.v_tostr, x)

    def toint(self, x) :
        return node.MonoOperation(value.v_toint, x)

    def in_base(self, notkw, test_value, *args) :
        op = node.INStmt(test_value, args)
        if notkw :
            op = node.MonoOperation(value.v_not, op)
        return op

    def instmt(self, test_value, *args) :
        return self.in_base(False, test_value, *args)

    def notinstmt(self, test_value, *args) :
        return self.in_base(True, test_value, *args)

    def RELOPERATOR(self, x) :
        if x.value == "="  or x.value == "==" :
            return pyops.eq
        elif x.value == "<" :
            return pyops.lt
        elif x.value == ">" :
            return pyops.gt
        elif x.value == "<=" :
            return pyops.le
        elif x.value == ">=" :
            return pyops.ge
        elif x.value == "!=" :
            return pyops.ne
        else :
            raise ValueError(f"Unknown relational operator {x.value}")

    def datavar(self, varname) :
        return node.DataVar(varname.value.lower())

    def and_clause(self, left, right) :
        return node.Operation('log', value.v_and, left, right)

    def or_clause(self, left, right) :
        return node.Operation('log', value.v_or, left, right)

    def caseleg(self, when, then) :
        return node.CaseLeg(when, then)

    def case(self, *branches) :
        if not isinstance(branches[-1], node.CaseLeg) :
            default = cast(node.ExprNode, branches[-1])
            legs = cast(list[node.CaseLeg], branches[:-1])
        else :
            default = node.Literal(None, 'int')
            legs = cast(list[node.CaseLeg], branches)
        return node.CaseStmt(legs, default)

    def between_base(self, notkw, x, low, high) :
        op = node.Operation('rel', value.v_and,
                            node.Operation('rel', pyops.ge, x, low), node.Operation('rel', pyops.le, x, high))
        if notkw :
            op = node.MonoOperation(value.v_not, op)
        return op
    def notbetween(self, x, low, high) :
        return self.between_base(True, x, low, high)
    def between(self, x, low, high) :
        return self.between_base(False, x, low, high)

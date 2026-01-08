from dataclasses import dataclass
from typing import Any, cast
from lark import Transformer, v_args
from .lib import expr_nodes as node

import operator as pyops

import logging
logger = logging.getLogger(__name__)


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
    def true(self) :
        return node.Literal(True, 'bool')
    def false(self) :
        return node.Literal(False, 'bool')
    def null(self) :
        return node.Literal(None, 'null')

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
        return node.MonoOperation(pyops.not_, x)

    def relop(self, left, op, right) :
        return node.Operation('rel', op, left, right)

    def logop(self, left, op, right) :
        return node.Operation('log', op, left, right)

    def RELOPERATOR(self, x) :
        if x.value == "=" :
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

    def LOGOPERATOR(self, x) :
        if x.value == "and" :
            return pyops.and_
        elif x.value == "or" :
            return pyops.or_
        else :
            raise ValueError(f"Unknown logical operator {x.value}")

    def caseleg(self, when, then) :
        return node.CaseLeg(when, then)

    def case(self, *branches) :
        if not isinstance(branches[-1], node.CaseLeg) :
            default = cast(node.ExprNode, branches[-1])
            legs = cast(list[node.CaseLeg], branches[:-1])
        else :
            default = node.Literal(None, 'null')
            legs = cast(list[node.CaseLeg], branches)
        return node.CaseStmt(legs, default)

    def between(self, x, low, high) :
        return node.Operation('rel', pyops.and_, node.Operation('rel', pyops.ge, x, low), node.Operation('rel', pyops.le, x, high))
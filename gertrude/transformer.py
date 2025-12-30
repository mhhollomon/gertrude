from dataclasses import dataclass
from typing import Any
from lark import Transformer, v_args
from .globals import ExprNode, Operation

import logging
import operator as pyops


logger = logging.getLogger(__name__)

@dataclass
class ColumnName(ExprNode) :
    name : str

    def calc(self, row : dict[str, Any]) :
        try :
            return row[self.name]
        except KeyError :
            raise KeyError(f"Column {self.name} not found in row")

    def to_python(self) :
        return f"row['{self.name}']"

@dataclass
class Literal(ExprNode) :
    value : Any
    vtype : str

    def calc(self, row : dict[str, Any]) :
        return self.value

    def to_python(self) :
        return repr(self.value)

@dataclass
class MonoOperation(ExprNode) :
    op : Any
    arg : ExprNode

    def calc(self, row : dict[str, Any]) :
        return self.op(self.arg.calc(row))

    def to_python(self) :
        return f"{self.op.__name__}({self.arg.to_python()})"

@v_args(inline=True)
class ExprTransformer(Transformer) :
    def value(self, x) -> ExprNode :
        return x

    def col_name(self, x) :
        logger.debug(f"col_name: {x}")
        if x.type == "DQ_STR" :
            return ColumnName(x.value[1:-1])
        return ColumnName(x.value)

    def lit_str(self, x) :
        return Literal(x.value[1:-1], 'str')
    def lit_int(self, x) :
        return Literal(int(x.value), 'int')
    def true(self, _) :
        return Literal(True, 'bool')
    def false(self, _) :
        return Literal(False, 'bool')
    def null(self, _) :
        return Literal(None, 'null')

    def add(self, x, y) :
        return Operation('math', pyops.add, x, y)
    def sub(self, x, y) :
        return Operation('math', pyops.sub, x, y)
    def mul(self, x, y) :
        return Operation('math', pyops.mul, x, y)
    def div(self, x, y) :
        return Operation('math', pyops.truediv, x, y)

    def bnot(self, x) :
        return MonoOperation(pyops.not_, x)

    def relop(self, left, op, right) :
        return Operation('rel', op, left, right)

    def logop(self, left, op, right) :
        return Operation('log', op, left, right)

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
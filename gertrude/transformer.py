from dataclasses import dataclass
from typing import Any, Callable
from lark import Transformer, v_args
from .globals import ExprNode

import logging


logger = logging.getLogger(__name__)

@dataclass
class ColumnName(ExprNode) :
    name : str

    def calc(self, row : dict[str, Any]) :
        return row[self.name]

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
class Operation(ExprNode) :
    op : Callable[[Any, Any], Any]
    left : ExprNode
    right : ExprNode

    def calc(self, row : dict[str, Any]) :
        return self.op(self.left.calc(row), self.right.calc(row))

    def to_python(self) :
        return f"({self.left.to_python()} {self.op.__name__} {self.right.to_python()})"

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
        from operator import add
        return Operation(add, x, y)
    def sub(self, x, y) :
        from operator import sub
        return Operation(sub, x, y)
    def mul(self, x, y) :
        from operator import mul
        return Operation(mul, x, y)
    def div(self, x, y) :
        from operator import truediv
        return Operation(truediv, x, y)
from dataclasses import dataclass
from typing import Any
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
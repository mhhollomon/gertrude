from dataclasses import dataclass
from typing import Any
from lark import Transformer, v_args

import logging
logger = logging.getLogger(__name__)

@dataclass 
class ColumnName :
    name : str

@dataclass
class Literal :
    value : Any
    vtype : str

@v_args(inline=True)
class ExprTransformer(Transformer) :
    def value(self, x) :
        return x
    
    def col_name(self, x) :
        logger.debug(f"col_name: {x}")
        return ColumnName(x.value)

    def literal(self, x) :
        return Literal(x.value, 'str')
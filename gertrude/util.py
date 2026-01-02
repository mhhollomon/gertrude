from typing import NamedTuple

from .lib.expr_nodes import ExprNode
from .expression import expr_parse


class SortSpec(NamedTuple) :
    expr : ExprNode
    order : str

def asc(expr : str) -> SortSpec :
    return SortSpec(expr_parse(expr), "asc")

def desc(expr : str) -> SortSpec :
    return SortSpec(expr_parse(expr), "desc")
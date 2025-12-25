from lark import Lark
from pathlib import Path
from .transformer import ExprTransformer
from .globals import ExprNode

import logging
logger = logging.getLogger(__name__)

_PARSER = None
def _get_parser() :
    global _PARSER

    if _PARSER is None :
        with open(Path(__file__).parent / "grammar.lark", "r") as f :
           grammar_text = f.read()

        _PARSER = Lark(grammar_text, parser="lalr", start="expr")

    return _PARSER

def expr_parse(text : str) -> ExprNode:
    tree = _get_parser().parse(text)
    logger.debug(f"tree = {tree}")
    ast = ExprTransformer().transform(tree)
    logger.debug(f"ast = {ast}")
    return ast
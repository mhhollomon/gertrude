from gertrude.expression import expr_parse
from gertrude.lib.expr_nodes import ExprNode, CaseStmt
import pytest
import logging

@pytest.fixture(scope="function", autouse=True)
def setup_logging(request, caplog):
    caplog.set_level(logging.DEBUG, logger="gertrude.runner")
    caplog.set_level(logging.DEBUG, logger="gertrude.expression")
    caplog.set_level(logging.DEBUG, logger="gertrude.transformer")
    request.caplog = caplog
    yield

def test_simple_math() :
    expr = expr_parse("1 + 2")
    assert isinstance(expr, ExprNode)
    assert expr.calc({}).value == 3

    expr = expr_parse("1 + 2 * 3")
    assert isinstance(expr, ExprNode)
    assert expr.calc({}).value == 7

    expr = expr_parse("1 + 2 * 3 + 4")
    assert isinstance(expr, ExprNode)
    assert expr.calc({}).value == 11

def test_null_math() :
    expr = expr_parse("1 + Null")
    assert isinstance(expr, ExprNode)
    assert expr.calc({}).value == None

def test_mixed_types() :
    expr = expr_parse("1 + Null + 2")
    assert isinstance(expr, ExprNode)
    assert expr.calc({}).value == None

    expr = expr_parse("1 + 'String'")
    assert isinstance(expr, ExprNode)
    with pytest.raises(TypeError) :
        expr.calc({})

    expr = expr_parse("Null + 'String'")
    assert isinstance(expr, ExprNode)
    assert expr.calc({}).value == None

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


def test_nvl() :
    expr = expr_parse("nvl(1,2)")
    assert isinstance(expr, ExprNode)
    assert expr.name == "nvl"
    assert expr.calc({}).value == 1

    expr = expr_parse("nvl(Null,2)")
    assert expr.calc({}).value == 2

def test_in() :
    expr = expr_parse("1 in (1,2,3)")
    assert expr.calc({}).value == True

    expr = expr_parse("1 in (2,3,4)")
    assert expr.calc({}).value == False
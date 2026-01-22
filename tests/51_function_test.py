from gertrude.expression import expr_parse
from gertrude.lib.expr_nodes import ExprNode, CaseStmt
import pytest
import logging

@pytest.fixture(scope="function", autouse=True)
def setup_logging(request, caplog):
    caplog.set_level(logging.DEBUG, logger="gertrude.runner")
    caplog.set_level(logging.DEBUG, logger="gertrude.expression")
    caplog.set_level(logging.DEBUG, logger="gertrude.transformer")
    caplog.set_level(logging.DEBUG, logger="gertrude.lib.expr_nodes")
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

def test_not_in():
    expr = expr_parse("1 not in (1,2,3)")
    assert expr.calc({}).value == False

    expr = expr_parse("1 not in (2,3,4)")
    assert expr.calc({}).value == True

def test_substring() :
    expr = expr_parse("substr('Hello World', 1, 5)")
    assert expr.calc({}).value == "Hello"

    expr = expr_parse("substr('Hello World', 7)")
    assert expr.calc({}).value == "World"

    expr = expr_parse("substr('Hello World', 7, NULL)")
    assert expr.calc({}).value == "World"

    expr = expr_parse("substr('Hello World', 7, 1)")
    assert expr.calc({}).value == "W"

    expr = expr_parse("'Hello ' + substr('Hello World', 7)")
    assert expr.calc({}).value == "Hello World"

def test_strlen():
    expr = expr_parse("strlen('Hello World')")
    assert expr.calc({}).value == 11

    expr = expr_parse("strlen(False)")
    with pytest.raises(TypeError) :
        expr.calc({})

    expr = expr_parse("strlen(Null)")
    assert expr.calc({}).value == None

def test_upper_and_lower() :
    expr = expr_parse("upper('Hello World')")
    assert expr.calc({}).value == "HELLO WORLD"

    expr = expr_parse("lower('Hello World')")
    assert expr.calc({}).value == "hello world"

def test_to_str() :
    expr = expr_parse("str(1.53)")
    assert expr.calc({}).value == "1.53"

    expr = expr_parse("str(Null)")
    assert expr.calc({}).value == None

    expr = expr_parse("str(False)")
    assert expr.calc({}).value == "False"
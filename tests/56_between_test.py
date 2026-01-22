from typing import Any
from gertrude.expression import expr_parse
import pytest
import logging

from gertrude.lib.types.value import Value

def v(value : Any) -> Value :
    return Value(type(value), value)

@pytest.fixture(scope="function", autouse=True)
def setup_logging(request, caplog):
    caplog.set_level(logging.DEBUG, logger="gertrude.runner")
    caplog.set_level(logging.DEBUG, logger="gertrude.expression")
    caplog.set_level(logging.DEBUG, logger="gertrude.transformer")
    request.caplog = caplog
    yield

def test_between(caplog) :
    expr = "value between 2 and 3"
    ast = expr_parse(expr)

    assert ast.calc({"value" : v(2)}).value == True
    assert ast.calc({"value" : v(3)}).value == True
    assert ast.calc({"value" : v(4)}).value == False

    expr = "value between 3 and 2"
    ast = expr_parse(expr)

    assert ast.calc({"value" : v(2)}).value == False
    assert ast.calc({"value" : v(3)}).value == False
    assert ast.calc({"value" : v(4)}).value == False

    expr = "value between 3+2 and 2*6"
    ast = expr_parse(expr)

    assert ast.calc({"value" : v(4)}).value == False
    assert ast.calc({"value" : v(6)}).value == True
    assert ast.calc({"value" : v(8)}).value == True
    assert ast.calc({"value" : v(12)}).value == True
    assert ast.calc({"value" : v(13)}).value == False

    expr = "value+5 between 3+2 and 2*6"
    ast = expr_parse(expr)

    assert ast.calc({"value" : v(-1)}).value == False
    assert ast.calc({"value" : v(1)}).value == True
    assert ast.calc({"value" : v(3)}).value == True
    assert ast.calc({"value" : v(7)}).value == True
    assert ast.calc({"value" : v(8)}).value == False


def test_not_between(caplog) :
    expr = "value not between 2 and 3"
    ast = expr_parse(expr)

    assert ast.calc({"value" : v(2)}).value == False
    assert ast.calc({"value" : v(3)}).value == False
    assert ast.calc({"value" : v(4)}).value == True
from gertrude.expression import expr_parse
import pytest
import logging

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

    assert ast.calc({"value" : 2}) == True
    assert ast.calc({"value" : 3}) == True
    assert ast.calc({"value" : 4}) == False

    expr = "value between 3 and 2"
    ast = expr_parse(expr)

    assert ast.calc({"value" : 2}) == False
    assert ast.calc({"value" : 3}) == False
    assert ast.calc({"value" : 4}) == False

    expr = "value between 3+2 and 2*6"
    ast = expr_parse(expr)

    assert ast.calc({"value" : 4}) == False
    assert ast.calc({"value" : 6}) == True
    assert ast.calc({"value" : 8}) == True
    assert ast.calc({"value" : 12}) == True
    assert ast.calc({"value" : 13}) == False

    expr = "value+5 between 3+2 and 2*6"
    ast = expr_parse(expr)

    assert ast.calc({"value" : -1}) == False
    assert ast.calc({"value" : 1}) == True
    assert ast.calc({"value" : 3}) == True
    assert ast.calc({"value" : 7}) == True
    assert ast.calc({"value" : 8}) == False

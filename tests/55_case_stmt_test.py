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


def test_case_stmt_parse() :
    ast = expr_parse("case when 1 then 2 when 3 then 4 else 5 end")
    assert isinstance(ast, CaseStmt)
    assert ast.name == "case"
    assert len(ast.legs) == 2
    assert ast.default.name == "int"

def test_no_default() :
    ast = expr_parse("case when 1 then 2 when 3 then 4 end")
    assert isinstance(ast, CaseStmt)
    assert ast.name == "case"
    assert len(ast.legs) == 2
    assert ast.default.name == "null"

def test_logic() :
    ast = expr_parse("case when year % 400 = 0 then True when year % 100 = 0 then False when year % 4 = 0 then True else False end")
    assert ast.calc({ "year" : 2000 }) == True
    assert ast.calc({ "year" : 1900 }) == False
    assert ast.calc({ "year" : 2001 }) == False
from gertrude.lib.types.value import Value
from gertrude.expression import expr_parse
from gertrude.lib.expr_nodes import CaseStmt
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
    assert ast.default.calc({}).value == 5

def test_no_default() :
    ast = expr_parse("case when 1 then 2 when 3 then 4 end")
    assert isinstance(ast, CaseStmt)
    assert ast.name == "case"
    assert len(ast.legs) == 2
    assert ast.default.calc({}).value == None

def test_logic() :
    ast = expr_parse("case when year % 400 = 0 then True when year % 100 = 0 then False when year % 4 = 0 then True else False end")
    assert ast.calc({ "year" : Value(int, 2000) }).value == True
    assert ast.calc({ "year" : Value(int, 1900) }).value == False
    assert ast.calc({ "year" : Value(int, 2001) }).value == False

    ast = expr_parse("(year % 400 = 0) or ((year % 100 != 0) and (year % 4 = 0))")
    assert ast.calc({ "year" : Value(int, 2000) }).value == True
    assert ast.calc({ "year" : Value(int, 1900) }).value == False
    assert ast.calc({ "year" : Value(int, 2001) }).value == False
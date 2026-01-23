
from datetime import datetime, timezone

from gertrude.expression import expr_parse


def test_timestamp() :

    expr = expr_parse("current_timestamp")
    # need to get rid of microseconds
    assert expr.calc({}).value[:-3] == datetime.now().isoformat()[:-3]

    expr = expr_parse("current_utc_timestamp")
    # need to get rid of microseconds and the timezone offset
    assert expr.calc({}).value[:-3] == datetime.now(timezone.utc).isoformat()[:-13]

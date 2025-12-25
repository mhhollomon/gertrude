import sys

import logging
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger(__name__)

from gertrude import Database, cspec

def test_query(tmp_path, caplog) :
    caplog.set_level(logging.DEBUG, logger="gertrude.runner")
    caplog.set_level(logging.DEBUG, logger="gertrude.expression")
    caplog.set_level(logging.DEBUG, logger="gertrude.transformer")
    logger.debug("---- test_query")
    db_path = tmp_path / "db"
    db = Database.create(db_path, comment="first")
    table = db.add_table("test", [
        cspec("id", "int"), cspec("name", "str")
    ])

    table.insert({"id" : 1, "name" : "bob"})
    table.insert({"id" : 2, "name" : "alice"})
    table.insert({"id" : 3, "name" : "charlie"})

    query = db.query("test").sort("id")
    data = list(query.run())
    assert data == [{"id" : 1, "name" : "bob"}, {"id" : 2, "name" : "alice"}, {"id" : 3, "name" : "charlie"}]

    query = db.query("test").filter(("id", 2))
    data = list(query.run())
    assert data == [{"id" : 2, "name" : "alice"}]

    query = db.query("test").filter(("id", 2)).select("name")
    data = list(query.run())
    assert data == [{"name" : "alice"}]
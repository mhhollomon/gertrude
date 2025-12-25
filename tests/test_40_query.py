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

    query = db.query("test").filter(("id", 2)).select(("new-name" , "name"), ("literal", "'hello'"), ("litint", "42"))
    data = list(query.run())
    assert data == [{"new-name" : "alice", "literal" : "hello", "litint" : 42}]


def test_math_query(tmp_path, caplog) :
    caplog.set_level(logging.DEBUG, logger="gertrude.runner")
    caplog.set_level(logging.DEBUG, logger="gertrude.expression")
    caplog.set_level(logging.DEBUG, logger="gertrude.transformer")
    db_path = tmp_path / "db"
    db = Database.create(db_path)
    table = db.add_table("test", [
        cspec("emp", "str"), 
        cspec("salary", "float"), 
        cspec("bonus", "float")
        ]
    )

    table.insert({"emp" : "bob", "salary" : 1000.0, "bonus" : 100.0})
    table.insert({"emp" : "alice", "salary" : 2000.0, "bonus" : 200.0})
    table.insert({"emp" : "charlie", "salary" : 3000.0, "bonus" : 300.0})

    data = db.query("test").filter(("emp", "bob")).select("emp", ("total", "salary + bonus")).run()

    assert list(data) == [{"emp" : "bob", "total" : 1100.0}]

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

    query = db.query("test").filter("id = 2")
    data = list(query.run())
    assert data == [{"id" : 2, "name" : "alice"}]

    query = db.query("test").filter("id = 2").select("name")
    data = list(query.run())
    assert data == [{"name" : "alice"}]

    query = db.query("test").filter("id = 2").select(("new-name" , "name"), ("literal", "'hello'"), ("litint", "42"))
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

    data = db.query("test").filter("emp = 'bob'").select("emp", ("total", "salary + bonus")).run()

    assert list(data) == [{"emp" : "bob", "total" : 1100.0}]

def test_math_query2(tmp_path, caplog) :
    caplog.set_level(logging.DEBUG, logger="gertrude.runner")
    caplog.set_level(logging.DEBUG, logger="gertrude.expression")
    caplog.set_level(logging.DEBUG, logger="gertrude.transformer")
    db_path = tmp_path / "db"
    db = Database.create(db_path)
    table = db.add_table("test", [
        cspec("stock", "str"), 
        cspec("price", "float"), 
        cspec("floor", "float")
        ]
    )

    table.insert({"stock" : "a", "price" : 100.0, "floor" : 10.0})
    table.insert({"stock" : "b", "price" : 200.0, "floor" : 20.0})
    table.insert({"stock" : "c", "price" : 300.0, "floor" : 30.0})

    data = db.query("test").filter("stock = 'a'").select("stock", ("total", "10 * price - floor")).run()
    assert list(data) == [{"stock" : "a", "total" : 990.0}]

def test_string_query(tmp_path, caplog) :
    caplog.set_level(logging.DEBUG, logger="gertrude.runner")
    caplog.set_level(logging.DEBUG, logger="gertrude.expression")
    caplog.set_level(logging.DEBUG, logger="gertrude.transformer")
    db_path = tmp_path / "db"
    db = Database.create(db_path)

    table = db.add_table("my_table", [
        cspec("first_name", "str"),
        cspec("last_name", "str"),
        cspec("dept", "str"),
        cspec("salary", "float"),
        cspec("bonus", "float")
        ])
    
    table.insert({"first_name" : "bob", "last_name" : "smith", "dept" : "sales", "salary" : 1000.0, "bonus" : 100.0})
    table.insert({"first_name" : "alice", "last_name" : "jones", "dept" : "sales", "salary" : 2000.0, "bonus" : 200.0})
    table.insert({"first_name" : "charlie", "last_name" : "brown", "dept" : "marketing", "salary" : 3000.0, "bonus" : 300.0})

    query = db.query("my_table").filter("dept = 'sales'")\
        .add_column("name", "last_name + ', ' + first_name")\
        .sort("name")\
        .select("name", ("total comp", "salary + bonus"))

    data = query.run()
    assert list(data) == [{"name" : "jones, alice", "total comp" : 2200.0}, {"name" : "smith, bob", "total comp" : 1100.0}]


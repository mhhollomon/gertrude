
import logging
import pytest

from gertrude import Database, cspec
from gertrude.util import desc

@pytest.fixture(scope="function", autouse=True)
def setup_logging(request, caplog):
    caplog.set_level(logging.DEBUG, logger="gertrude.runner")
    caplog.set_level(logging.DEBUG, logger="gertrude.expression")
    caplog.set_level(logging.DEBUG, logger="gertrude.transformer")
    request.cls.caplog = caplog
    yield

@pytest.fixture(scope="function", autouse=True)
def setup_database(request, tmp_path) :
    db_path = tmp_path / "db"
    db = Database.create(db_path)
    request.cls.db = db
    yield
    import shutil
    shutil.rmtree(db_path)

@pytest.mark.usefixtures("setup_logging", "setup_database")
class TestQuery() :
    db : Database

    def test_query(self) :
        table = self.db.add_table("test", [
            cspec("id", "int"), cspec("name", "str")
        ])

        table.insert({"id" : 1, "name" : "bob"})
        table.insert({"id" : 2, "name" : "alice"})
        table.insert({"id" : 3, "name" : "charlie"})

        query = self.db.query("test").sort("id")
        data = list(query.run())
        assert data == [{"id" : 1, "name" : "bob"}, {"id" : 2, "name" : "alice"}, {"id" : 3, "name" : "charlie"}]

        query = self.db.query("test").filter("id = 2")
        data = list(query.run())
        assert data == [{"id" : 2, "name" : "alice"}]

        query = self.db.query("test").filter("id = 2").select("name")
        data = list(query.run())
        assert data == [{"name" : "alice"}]

        query = self.db.query("test").filter("id = 2").select(("new-name" , "name"), ("literal", "'hello'"), ("litint", "42"))
        data = list(query.run())
        assert data == [{"new-name" : "alice", "literal" : "hello", "litint" : 42}]


    def test_math_query(self) :
        table = self.db.add_table("test", [
            cspec("emp", "str"),
            cspec("salary", "float"),
            cspec("bonus", "float")
            ]
        )

        table.insert({"emp" : "bob", "salary" : 1000.0, "bonus" : 100.0})
        table.insert({"emp" : "alice", "salary" : 2000.0, "bonus" : 200.0})
        table.insert({"emp" : "charlie", "salary" : 3000.0, "bonus" : 300.0})

        data = self.db.query("test").filter("emp = 'bob'").select("emp", ("total", "salary + bonus")).run()

        assert list(data) == [{"emp" : "bob", "total" : 1100.0}]

    def test_math_query2(self) :
        table = self.db.add_table("test", [
            cspec("stock", "str"),
            cspec("price", "float"),
            cspec("floor", "float")
            ]
        )

        table.insert({"stock" : "a", "price" : 100.0, "floor" : 10.0})
        table.insert({"stock" : "b", "price" : 200.0, "floor" : 20.0})
        table.insert({"stock" : "c", "price" : 300.0, "floor" : 30.0})

        data = self.db.query("test").filter("stock = 'a'").select("stock", ("total", "10 * price - floor")).run()
        assert list(data) == [{"stock" : "a", "total" : 990.0}]

    def test_string_query(self) :

        table = self.db.add_table("my_table", [
            cspec("first_name", "str"),
            cspec("last_name", "str"),
            cspec("dept", "str"),
            cspec("salary", "float"),
            cspec("bonus", "float")
            ])

        table.insert({"first_name" : "bob", "last_name" : "smith", "dept" : "sales", "salary" : 1000.0, "bonus" : 100.0})
        table.insert({"first_name" : "alice", "last_name" : "jones", "dept" : "sales", "salary" : 2000.0, "bonus" : 200.0})
        table.insert({"first_name" : "charlie", "last_name" : "brown", "dept" : "marketing", "salary" : 3000.0, "bonus" : 300.0})

        query = self.db.query("my_table").filter("dept = 'sales'")\
            .add_column("name", "last_name + ', ' + first_name")\
            .sort("name")\
            .select("name", ("total comp", "salary + bonus"))

        data = query.run()
        assert list(data) == [{"name" : "jones, alice", "total comp" : 2200.0}, {"name" : "smith, bob", "total comp" : 1100.0}]

    def test_not_query(self) :
        table = self.db.add_table("test", [
            cspec("id", "int"), cspec("name", "str")
        ])

        table.insert({"id" : 1, "name" : "bob"})
        table.insert({"id" : 2, "name" : "alice"})
        table.insert({"id" : 3, "name" : "charlie"})

        query = self.db.query("test").filter("not (name = 'alice')").sort("id")
        data = list(query.run())
        assert data == [{"id" : 1, "name" : "bob"}, {"id" : 3, "name" : "charlie"}]

    def test_index_query(self) :
        table = self.db.add_table("test", [
            cspec("id", "int", pk=True), cspec("name", "str")
        ])

        table.insert({"id" : 1, "name" : "bob"})
        table.insert({"id" : 2, "name" : "alice"})
        table.insert({"id" : 3, "name" : "charlie"})

        query = self.db.query("test").filter("id >= 2").sort("id")
        data = list(query.run())
        assert data == [{"id" : 2, "name" : "alice"}, {"id" : 3, "name" : "charlie"}]
        plan = query.show_plan()
        print("\n".join(plan))
        assert len(plan) == 3
        assert plan[-1].startswith("to_dict(")

    def test_distinct_query(self) :
        table = self.db.add_table("test", [
            cspec("id", "int"), cspec("name", "str")
        ])

        table.insert({"id" : 1, "name" : "bob"})
        table.insert({"id" : 2, "name" : "alice"})
        table.insert({"id" : 3, "name" : "bob"})

        # The sort is just to make the output stable.
        query = self.db.query("test").sort("name", "id").distinct("name")
        data = list(query.run())
        assert data == [{"id" : 2, "name" : "alice"}, {"id" : 1, "name" : "bob"}]

        query = self.db.query("test").sort("id","name").distinct()
        data = list(query.run())
        assert data == [{"id" : 1, "name" : "bob"}, {"id" : 2, "name" : "alice"},
                        {"id" : 3, "name" : "bob"}]


    def test_sort_query(self) :
        table = self.db.add_table("test", [
            cspec("id", "int"), cspec("name", "str")
        ])

        table.insert({"id" : 1, "name" : "bob"})
        table.insert({"id" : 2, "name" : "alice"})
        table.insert({"id" : 3, "name" : "bob"})

        query = self.db.query("test").sort(desc("id"))
        data = list(query.run())

        assert data == [{"id" : 3, "name" : "bob"}, {"id" : 2, "name" : "alice"},
                        {"id" : 1, "name" : "bob"}]

        table = self.db.add_table("order", [
            cspec("cust", "int"), cspec("item", "str"), cspec("qty", "int")
        ])

        table.insert({"cust" : 1, "item" : "a", "qty" : 10})
        table.insert({"cust" : 1, "item" : "b", "qty" : 20})
        table.insert({"cust" : 1, "item" : "c", "qty" : 30})
        table.insert({"cust" : 2, "item" : "a", "qty" : 40})
        table.insert({"cust" : 2, "item" : "b", "qty" : 50})
        table.insert({"cust" : 2, "item" : "c", "qty" : 60})

        query = self.db.query("order").sort("cust", desc("qty"))
        data = list(query.run())

        assert data == [{"cust" : 1, "item" : "c", "qty" : 30},
                        {"cust" : 1, "item" : "b", "qty" : 20},
                        {"cust" : 1, "item" : "a", "qty" : 10},
                        {"cust" : 2, "item" : "c", "qty" : 60},
                        {"cust" : 2, "item" : "b", "qty" : 50},
                        {"cust" : 2, "item" : "a", "qty" : 40}]
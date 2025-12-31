from gertrude import Database, cspec
import logging
import pytest

from gertrude.index import Index
from gertrude.table import Table

@pytest.fixture(scope="class", autouse=True)
def setup_database(request, tmp_path_factory) :
    tmp_path = tmp_path_factory.mktemp("test-index-tmp-path")
    db_path = tmp_path / "db"
    db = Database.create(db_path)
    request.cls.db = db
    table = db.add_table("test", [
        cspec("id", "int"), cspec("name", "str")
    ])

    request.cls.table = table

    # duplication on purpose
    table.insert({"id" : 1, "name" : "bob"})
    table.insert({"id" : 1, "name" : "bob"})

    table.insert({"id" : 2, "name" : "alice"})
    table.insert({"id" : 3, "name" : "charlie"})

    request.cls.index = table.add_index("name_index", "name")

    yield
    import shutil
    shutil.rmtree(db_path)

@pytest.mark.usefixtures("setup_database")
class TestIndex() :
    db : Database
    table : Table
    index : Index

    def test_index_scan(self, caplog) :
        caplog.set_level(logging.DEBUG, logger="gertrude.index")

        table = self.table
        index = self.index

        index.print_tree()

        data = list(table.index_scan("name_index"))
        assert data == [{"id" : 2, "name" : "alice"}, {"id" : 1, "name" : "bob"}, {"id" : 1, "name" : "bob"}, {"id" : 3, "name" : "charlie"}]

        data = list(table.index_scan("name_index", "bob", op="<="))
        assert data == [{"id" : 2, "name" : "alice"}, {"id" : 1, "name" : "bob"}, {"id" : 1, "name" : "bob"}]

        data = list(table.index_scan("name_index", "bob", op="<"))
        assert data == [{"id" : 2, "name" : "alice"}]

        data = list(table.index_scan("name_index", "bob", op=">="))
        assert data == [ {"id" : 1, "name" : "bob"}, {"id" : 1, "name" : "bob"}, {"id" : 3, "name" : "charlie"}]

        data = list(table.index_scan("name_index", "bob", op=">"))
        assert data == [{"id" : 3, "name" : "charlie"}]

        data = list(table.index_scan("name_index", "bob", op="="))
        assert data == [{"id" : 1, "name" : "bob"}, {"id" : 1, "name" : "bob"}]

    def test_missing_key(self, caplog) :
        caplog.set_level(logging.DEBUG, logger="gertrude.index")

        self.index.print_tree()

        data = list(self.table.index_scan("name_index", "carl", op=">"))
        assert data == [{"id" : 3, "name" : "charlie"}]

        data = list(self.table.index_scan("name_index", "carl", op="<="))
        assert data == [{"id" : 2, "name" : "alice"}, {"id" : 1, "name" : "bob"}, {"id" : 1, "name" : "bob"}]

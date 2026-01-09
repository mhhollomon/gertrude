from gertrude import Database, cspec
import logging
import pytest

from gertrude.index import Index
from gertrude.table import Table

@pytest.fixture(scope="function", autouse=True)
def setup_database(request, tmp_path, caplog) :

    caplog.set_level(logging.DEBUG, logger="gertrude.index")

    db_path = tmp_path / "db"
    db = Database.create(db_path)
    request.db = db
    table = db.add_table("test", [
        cspec("id", "int"), cspec("name", "str")
    ])


    # duplication on purpose
    table.insert({"id" : 1, "name" : "bob"})
    table.insert({"id" : 1, "name" : "bob"})

    table.insert({"id" : 2, "name" : "alice"})
    table.insert({"id" : 3, "name" : "charlie"})

    index = table.add_index("name_index", "name")

    yield {"db" : db, "table" : table, "index" : index}
    import shutil
    shutil.rmtree(db_path)

def test_index_scan(setup_database, caplog) :
    caplog.set_level(logging.DEBUG, logger="gertrude.index")

    table = setup_database['table']
    index = setup_database['index']

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

def test_missing_key(setup_database, caplog) :
    caplog.set_level(logging.DEBUG, logger="gertrude.index")

    table = setup_database['table']
    index = setup_database['index']

    index.print_tree()

    data = list(table.index_scan("name_index", "carl", op=">"))
    assert data == [{"id" : 3, "name" : "charlie"}]

    data = list(table.index_scan("name_index", "carl", op="<="))
    assert data == [{"id" : 2, "name" : "alice"}, {"id" : 1, "name" : "bob"}, {"id" : 1, "name" : "bob"}]

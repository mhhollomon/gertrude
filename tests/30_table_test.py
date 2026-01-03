from gertrude import Database, cspec
import logging

def test_delete_row_no_index(caplog, tmp_path) :
    db_path = tmp_path / "db"
    db = Database.create(db_path, comment="first")
    table = db.add_table("test", [
        cspec("id", "int"), cspec("name", "str")
    ])

    table.insert({"id" : 1, "name" : "bob"})
    table.insert({"id" : 2, "name" : "alice"})
    table.insert({"id" : 3, "name" : "charlie"})

    caplog.set_level(logging.DEBUG, logger="gertrude.table")

    assert table.delete({"id" : 2, "name" : "alice"})

    data = sorted([x for x in table.scan()], key=lambda x : x["id"])
    assert data == [{"id" : 1, "name" : "bob"}, {"id" : 3, "name" : "charlie"}]

def test_delete_row_with_index(caplog, tmp_path) :
    db_path = tmp_path / "db"
    db = Database.create(db_path, comment="first")
    table = db.add_table("test", [
        cspec("id", "int"), cspec("name", "str")
    ])

    table.insert({"id" : 1, "name" : "bob"})
    table.insert({"id" : 2, "name" : "alice"})
    table.insert({"id" : 3, "name" : "charlie"})

    table.add_index("name_index", "name")

    caplog.set_level(logging.DEBUG, logger="gertrude.table")
    caplog.set_level(logging.DEBUG, logger="gertrude.table")

    assert table.delete({"id" : 2, "name" : "alice"})

    data = sorted([x for x in table.scan()], key=lambda x : x["id"])
    assert data == [{"id" : 1, "name" : "bob"}, {"id" : 3, "name" : "charlie"}]

    data = list(table.index_scan("name_index"))
    assert data == [{"id" : 1, "name" : "bob"}, {"id" : 3, "name" : "charlie"}]

    table.indexes["name_index"].print_tree()

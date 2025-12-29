from gertrude import Database, cspec, KeyBound
import logging

def test_index_scan(caplog, tmp_path) :
    db_path = tmp_path / "db"
    db = Database.create(db_path, comment="first")
    table = db.add_table("test", [
        cspec("id", "int"), cspec("name", "str")
    ])

    table.insert({"id" : 1, "name" : "bob"})
    table.insert({"id" : 2, "name" : "alice"})
    table.insert({"id" : 3, "name" : "charlie"})

    index=table.add_index("name_index", "name")
    caplog.set_level(logging.DEBUG, logger="gertrude.index")


    index.print_tree()

    data = list(table.index_scan("name_index"))
    assert data == [{"id" : 2, "name" : "alice"}, {"id" : 1, "name" : "bob"}, {"id" : 3, "name" : "charlie"}]

    data = list(table.index_scan("name_index", "bob", key_bound=KeyBound.UPPER))
    assert data == [{"id" : 2, "name" : "alice"}, {"id" : 1, "name" : "bob"}]

    data = list(table.index_scan("name_index", "bob", key_bound=KeyBound.UPPER, include_key=False))
    assert data == [{"id" : 2, "name" : "alice"}]

    data = list(table.index_scan("name_index", "bob", key_bound=KeyBound.LOWER))
    assert data == [ {"id" : 1, "name" : "bob"}, {"id" : 3, "name" : "charlie"}]

    data = list(table.index_scan("name_index", "bob", key_bound=KeyBound.LOWER, include_key=False))
    assert data == [{"id" : 3, "name" : "charlie"}]
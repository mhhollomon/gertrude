from gertrude import Database, cspec
import logging
import pytest


def test_non_unique_index(tmp_path, caplog) :

    caplog.set_level(logging.DEBUG, logger="gertrude.index")
    caplog.set_level(logging.DEBUG, logger="gertrude.lib.cache")
    caplog.set_level(logging.DEBUG, logger="gertrude.lib.packer")

    db_path = tmp_path / "db"

    # Use small  fanout so we can atually see if
    # the index create code works.
    db = Database.create(db_path, index_fanout=6)
    table = db.add_table("test", [
        cspec("id", "int")
    ])

    table.insert({"id" : 1})
    table.insert({"id" : 2})
    table.insert({"id" : 3})
    table.insert({"id" : 3})
    for i in range(10) :
        table.insert({"id" : 2})

    index = table.add_index("id_index", "id")

    index.print_tree()

    data = db.query("test").filter("id = 2").run()

    assert len(data) == 11
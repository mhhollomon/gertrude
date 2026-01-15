import logging
from gertrude import Database, cspec
from gertrude.lib.types.colref import ColRef

def test_columns(tmp_path, caplog) :
    db = Database.create(tmp_path / "db")
    table = db.add_table("test", [
        cspec("id", "int"), cspec("name", "str")
    ])

    caplog.set_level(logging.DEBUG, logger="gertrude.lib.plan")
    caplog.set_level(logging.DEBUG, logger="gertrude.lib.types.colref")

    assert table.columns() == set([ColRef("id"), ColRef("name")])

    query = db.query("test")
    assert query.columns() == set([ColRef("id"), ColRef("name")])

    query = db.query("test").select("name")
    assert query.columns() == set([ColRef("name")])

    q2 = db.query("test").rename_columns(("name", "new_name"))
    assert q2.columns() == set([ColRef("id"), ColRef("new_name")])

    query = db.query("test").filter("name = 'bob'").sort("id").limit(10).distinct()
    assert query.columns() == set([ColRef("id"), ColRef("name")])

    query = db.query("test").join(q2, on="id", rename=False)
    assert query.columns() == set([ColRef("id"), ColRef("name"), ColRef("new_name")])

    query = db.query("test").join(db.query("test"), on="id", rename=True)
    assert query.columns() == set([ColRef("id_left"), ColRef("id_right"), ColRef("name_left"), ColRef("name_right")])

    query = db.query("test").join(db.query("test"), on="id", rename=("_a", "_b"))
    assert query.columns() == set([ColRef("id_a"), ColRef("id_b"), ColRef("name_a"), ColRef("name_b")])
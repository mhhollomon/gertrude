from gertrude import Database, cspec
import pytest

def test_defaults(tmp_path) :
    db_path = tmp_path / "db"
    db = Database.create(db_path, comment="first")
    table = db.add_table("test", [
        cspec("id", "int", default=1),
        cspec("name", "str", default="bob"),
        cspec('comment', 'str')
    ])

    table.insert({"name" : "alice"})
    table.insert({"name" : "charlie"})
    table.insert({"id" :2})

    data = sorted(list(table.scan()), key=lambda x : x["name"])
    assert data == [{"id" : 1, "name" : "alice", "comment" : None},{"id" : 2, "name" : "bob", "comment" : None}, {"id" : 1, "name" : "charlie", "comment" : None}]

@pytest.mark.skip
def test_callable_default(tmp_path) :
    db_path = tmp_path / "db"
    db = Database.create(db_path, comment="first")
    table = db.add_table("test", [
        cspec("id", "int", default=lambda : 1),
        cspec("name", "str", default=lambda : "bob")
    ])

    table.insert({"name" : "alice"})
    table.insert({"name" : "charlie"})
    table.insert({"id" :2})

    data = sorted(list(table.scan()), key=lambda x : x["name"])
    assert data == [{"id" : 1, "name" : "alice"},{"id" : 2, "name" : "bob"}, {"id" : 1, "name" : "charlie"}]
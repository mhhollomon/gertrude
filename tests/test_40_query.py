from gertrude import Database, cspec

def test_db_create(tmp_path) :
    db_path = tmp_path / "db"
    db = Database.create(db_path, comment="first")
    table = db.add_table("test", [
        cspec("id", "int"), cspec("name", "str")
    ])

    table.insert({"id" : 1, "name" : "bob"})
    table.insert({"id" : 2, "name" : "alice"})
    table.insert({"id" : 3, "name" : "charlie"})

    query = db.query("test")
    data = list(query.run())
    assert data == [{"id" : 1, "name" : "bob"}, {"id" : 2, "name" : "alice"}, {"id" : 3, "name" : "charlie"}]

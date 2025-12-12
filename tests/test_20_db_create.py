from gertrude import Database

def test_db_create(tmp_path) :
    db_path = tmp_path / "db"
    db = Database(db_path, "first")
    assert db.db_path == db_path
    assert db.db_path.exists()
    assert db.db_path.is_dir()
    assert (db_path / "gertrude.conf").read_text() == '{"schema_version": 1, "gertrude_version": "0.0.1", "comment": "first"}'
    assert (db_path / "tables").is_dir()

    db2 = Database(db_path, "second")
    assert db2.db_path == db_path
    # make sure it didn't rewrite the file
    assert (db_path / "gertrude.conf").read_text() == '{"schema_version": 1, "gertrude_version": "0.0.1", "comment": "first"}'

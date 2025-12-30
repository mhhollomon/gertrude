from gertrude import Database
from gertrude.globals import GERTRUDE_VERSION

def test_db_create(tmp_path) :
    db_path = tmp_path / "db"
    db = Database.create(db_path, comment="first")
    assert db.db_path == db_path
    assert db.db_path.exists()
    assert db.db_path.is_dir()
    assert (db_path / "gertrude.conf").read_text() == \
        f'{{"schema_version": 1, "gertrude_version": "{GERTRUDE_VERSION}", "comment": "first"}}'
    assert (db_path / "tables").is_dir()

    db2 = Database.open(db_path)
    assert db2.db_path == db_path
    # make sure it didn't rewrite the file
    assert (db_path / "gertrude.conf").read_text() == \
        f'{{"schema_version": 1, "gertrude_version": "{GERTRUDE_VERSION}", "comment": "first"}}'

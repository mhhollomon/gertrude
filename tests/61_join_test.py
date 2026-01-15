import logging
from gertrude import Database, cspec

def test_join(tmp_path, caplog) :
    db = Database.create(tmp_path / "db")
    emp_table = db.add_table("employees", [
        cspec("id", "int"), cspec("name", "str")
    ])
    proj_table = db.add_table("projects", [
        cspec("id", "int"), cspec("name", "str"), cspec("emp_id", "int")
    ])

    caplog.set_level(logging.DEBUG, logger="gertrude.table")

    emp_table.insert({"id" : 1, "name" : "bob"})
    emp_table.insert({"id" : 2, "name" : "alice"})
    emp_table.insert({"id" : 3, "name" : "charlie"})
    emp_table.insert({"id" : 4, "name" : "dave"})

    proj_table.insert({"id" : 1, "name" : "p1", "emp_id" : 1})
    proj_table.insert({"id" : 2, "name" : "p2", "emp_id" : 2})
    proj_table.insert({"id" : 3, "name" : "p3", "emp_id" : 3})


    projects = db.query("projects").rename_columns(("id", "pid"), ("name", "pname"))

    caplog.set_level(logging.DEBUG, logger="gertrude.lib.plan")


    query = db.query("employees").join(projects, ("id","emp_id")).sort("id")
    data = list(query.run())

    assert data == [{"id" : 1, "name" : "bob", "pid" : 1, "pname" : "p1", "emp_id" : 1},
                    {"id" : 2, "name" : "alice", "pid" : 2, "pname" : "p2", "emp_id" : 2},
                    {"id" : 3, "name" : "charlie", "pid" : 3, "pname" : "p3", "emp_id" : 3}]

    query = db.query("employees").join(projects, ("id","emp_id"), how="left_outer").sort("id")
    data = list(query.run())

    assert data == [{"id" : 1, "name" : "bob"    , "pid" : 1,    "pname" : "p1", "emp_id" : 1},
                    {"id" : 2, "name" : "alice"  , "pid" : 2,    "pname" : "p2", "emp_id" : 2},
                    {"id" : 3, "name" : "charlie", "pid" : 3,    "pname" : "p3", "emp_id" : 3},
                    {"id" : 4, "name" : "dave"   , "pid" : None, "pname" : None, "emp_id" : None}]


def test_rename_join(tmp_path, caplog) :
    db = Database.create(tmp_path / "db")
    emp_table = db.add_table("employees", [
        cspec("id", "int"), cspec("name", "str")
    ])
    proj_table = db.add_table("projects", [
        cspec("id", "int"), cspec("name", "str"), cspec("emp_id", "int")
    ])

    caplog.set_level(logging.DEBUG, logger="gertrude.table")

    emp_table.insert({"id" : 1, "name" : "bob"})
    emp_table.insert({"id" : 2, "name" : "alice"})
    emp_table.insert({"id" : 3, "name" : "charlie"})
    emp_table.insert({"id" : 4, "name" : "dave"})

    proj_table.insert({"id" : 1, "name" : "p1", "emp_id" : 1})
    proj_table.insert({"id" : 2, "name" : "p2", "emp_id" : 2})
    proj_table.insert({"id" : 3, "name" : "p3", "emp_id" : 3})

    projects = db.query("projects")

    caplog.set_level(logging.DEBUG, logger="gertrude.lib.plan")


    query = db.query("employees").join(projects, ("id","emp_id"), rename=True).sort("id_left")
    data = list(query.run())

    assert data == [{"id_left" : 1, "name_left" : "bob",     "id_right" : 1, "name_right" : "p1", "emp_id" : 1},
                    {"id_left" : 2, "name_left" : "alice",   "id_right" : 2, "name_right" : "p2", "emp_id" : 2},
                    {"id_left" : 3, "name_left" : "charlie", "id_right" : 3, "name_right" : "p3", "emp_id" : 3}]

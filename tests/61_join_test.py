import logging
from gertrude import Database, cspec

def test_join(tmp_path, caplog) :
    db = Database.create(tmp_path / "db")
    emp_table = db.add_table("employees", [
        cspec("id", "int"), cspec("name", "str")
    ])
    proj_table = db.add_table("projects", [
        cspec("pid", "int"), cspec("pname", "str"), cspec("emp_id", "int")
    ])

    caplog.set_level(logging.DEBUG, logger="gertrude.table")

    emp_table.insert({"id" : 1, "name" : "bob"})
    emp_table.insert({"id" : 2, "name" : "alice"})
    emp_table.insert({"id" : 3, "name" : "charlie"})
    emp_table.insert({"id" : 4, "name" : "dave"})

    proj_table.insert({"pid" : 1, "pname" : "p1", "emp_id" : 1})
    proj_table.insert({"pid" : 2, "pname" : "p2", "emp_id" : 2})
    proj_table.insert({"pid" : 3, "pname" : "p3", "emp_id" : 3})
    projects = db.query("projects")

    # The sort is just to make the output stable.

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
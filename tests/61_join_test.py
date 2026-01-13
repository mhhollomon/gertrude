
from gertrude import Database, cspec

def test_inner_join(tmp_path, caplog) :
    db = Database.create(tmp_path / "db")
    emp_table = db.add_table("employees", [
        cspec("id", "int"), cspec("name", "str")
    ])
    proj_table = db.add_table("projects", [
        cspec("id", "int"), cspec("name", "str"), cspec("emp_id", "int")
    ])

    emp_table.insert({"id" : 1, "name" : "bob"})
    emp_table.insert({"id" : 2, "name" : "alice"})
    emp_table.insert({"id" : 3, "name" : "charlie"})

    proj_table.insert({"id" : 1, "name" : "p1", "emp_id" : 1})
    proj_table.insert({"id" : 2, "name" : "p2", "emp_id" : 2})
    proj_table.insert({"id" : 3, "name" : "p3", "emp_id" : 3})
    projects = db.query("projects")

    data = projects.run()
    # The sort is just to make the output stable.

    query = db.query("employees").join(projects, ("id","emp_id")).sort("id")
    data = list(query.run())

    assert data == [{"id" : 1, "name" : "bob", "id" : 1, "name" : "p1", "emp_id" : 1},
                    {"id" : 2, "name" : "alice", "id" : 2, "name" : "p2", "emp_id" : 2},
                    {"id" : 3, "name" : "charlie", "id" : 3, "name" : "p3", "emp_id" : 3}]
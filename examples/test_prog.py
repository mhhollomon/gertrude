#!/usr/bin/env python

import sys
import os
script_dir = os.path.dirname(os.path.abspath(__file__))
module_dir = os.path.join(script_dir, '..')
sys.path.insert(0, module_dir)

from pathlib import Path
import shutil
from gertrude import  Database, cspec

# import logging
# logging.basicConfig(level=logging.DEBUG)

db_path = Path("./output")
if db_path.exists() :
    shutil.rmtree(db_path)

db = Database.create(Path("./output"))

table = db.add_table("test", [
    cspec("name", 'str', pk=True),
    cspec("age", 'int'),
])

print(table.get_spec())

table.insert({"name" : "bob", "age" : 12})
table.insert({"name" : "alice", "age" : 22})
table.insert({"name" : "george", "age" : 14})
table.insert({"name" : "mark", "age" : 62})
table.insert({"name" : "martha", "age" : 32})
table.insert({"name" : "eli", "age" : 20})
table.insert({"name" : "rebecca", "age" : 26})


# Test building an index after the fact.
table.add_index("age_index", "age")

table.insert({"name" : "sam", "age" : 18})
table.insert({"name" : "joe", "age" : 18})
table.insert({"name" : "fred", "age" : 18})
table.insert({"name" : "tina", "age" : 18})
table.insert({"name" : "velma", "age" : 18})
table.insert({"name" : "jackie", "age" : 18})

table.insert({"name" : "ben", "age" : 19})

# Test inserting a null into a nullable field with index
table.insert({"name" : "jim", "age" : None})
table.insert({"name" : "garfield", "age" : None})

table.insert({"name" : "kris", "age" : 55})
table.insert({"name" : "chris", "age" : 55})
table.insert({"name" : "scooby", "age" : 10})
table.insert({"name" : "bilbo", "age" : 100})
table.insert({"name" : "harry", "age" : None})


query = db.query("test").filter("age = 18").sort("name")
print(query.optplan())
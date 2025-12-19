#!/usr/bin/env python

from pathlib import Path
import shutil
import gertrude

db_path = Path("./output")
if db_path.exists() :
    shutil.rmtree(db_path)

db = gertrude.Database(Path("./output"))

table = db.create_table("test", [
    gertrude.FieldSpec("name", 'str', {'pk' :True}),
    gertrude.FieldSpec("age", 'int', {}),
])

table.insert({"name" : "bob", "age" : 12})
table.insert({"name" : "alice", "age" : 22})
try :
    table.insert({"name" : "alice", "age" : 22})
except ValueError as e :
    print(e)
table.insert({"name" : "george", "age" : 14})
table.insert({"name" : "mark", "age" : 62})
table.insert({"name" : "martha", "age" : 32})
table.insert({"name" : "eli", "age" : 20})
table.insert({"name" : "rebecca", "age" : 26})
try :
    table.insert({"name" : None, "age" : 16})
except ValueError as e :
    print(e)

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

print("Scan:")
for record in table.scan() :
    print(f"{record.name} is {record.age} years old.")


print("Cache stats:")
print(db.get_cache_stats())
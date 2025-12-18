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

# Test building an index after the fact.
table.add_index("age_index", "age")

for record in table.scan() :
    print(f"{record.name} is {record.age} years old.")


print("Cache stats:")
print(db.get_cache_stats())
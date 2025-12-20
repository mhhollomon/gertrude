#!/usr/bin/env python

import sys
import os
script_dir = os.path.dirname(os.path.abspath(__file__))
module_dir = os.path.join(script_dir, '..')
sys.path.insert(0, module_dir)

from pathlib import Path
import random
import shutil
import gertrude


db_path = Path("./output")
if db_path.exists() :
    shutil.rmtree(db_path)

db = gertrude.Database.create(Path("./output"))

table = db.create_table("test", [
    gertrude.FieldSpec("num", 'int', {}),
])

array = [i for i in range(100)]
random.shuffle(array)
table.add_index("test_index", "num")
for num in array :
    table.insert({"num" : num})

table.print_index("test_index")

print("Cache stats:")
print(db.get_cache_stats())
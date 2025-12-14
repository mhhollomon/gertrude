#!/usr/bin/env python

from pathlib import Path
import shutil
import gertrude

db_path = Path("./output")
shutil.rmtree(db_path)

db = gertrude.Database(Path("./output"))

db.create_table("test", [
    gertrude.FieldSpec("name", 'str', {'pk' :True}),
    gertrude.FieldSpec("age", 'int', {}),
])

from pathlib import Path
from typing import Iterable, NamedTuple, Any, cast
import json


FieldSpec = NamedTuple("FieldSpec", [("name", str), ("type", str), ("options", dict[str, Any])])

class Table :
    def __init__(self, 
                    parent : Any, 
                    db_path : Path, 
                    table_name : str, 
                    spec : Iterable[FieldSpec]) :
        
        from gertrude.database import Database
        assert isinstance(parent, Database)

        self.db_path = db_path
        self.spec = spec
        self.name = table_name
        self.parent = cast(Database, parent)
        self.open = True

    def _drop(self) :
        if not self.open or self.parent is None :
            return
        import shutil
        shutil.rmtree(self.db_path)
        self.open = False
        self.parent = None


    def _create(self) :
        if self.db_path.exists() :
            raise ValueError(f"Table {self.name} directory already exists.")
        
        # good to go
        self.db_path.mkdir(exist_ok=True)
        (self.db_path / "config").write_text(json.dumps(self.spec))

    def _load_def(self) :
        self.spec = json.loads((self.db_path / "config").read_text())

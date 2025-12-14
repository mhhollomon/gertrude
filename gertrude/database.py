from typing import Any, NamedTuple, Iterable
from nanoid import generate
from pathlib import Path
import msgpack
import json
import regex as re

from .table import Table, FieldSpec

from .globals import (_generate_id, CURRENT_SCHEMA_VERSION, 
                      GERTRUDE_VERSION, NAME_REGEX)


_OPTIONS = {
    "pk" : bool,
}



#################################################################
# Database class
#################################################################
class Database :
    def __init__(self, db_path : Path, comment : str = '') :
        self.db_path = db_path
        self.table_defs = {}

        need_setup = True
        if not self.db_path.exists() :
            self.db_path.mkdir()
        else :
            assert self.db_path.is_dir()
            conf_file = self.db_path / "gertrude.conf"
            if not conf_file.exists() :
                self._clear()
            else :
                config = json.loads((self.db_path / "gertrude.conf").read_text())
                assert config["schema_version"] == CURRENT_SCHEMA_VERSION    
                assert config["gertrude_version"] == GERTRUDE_VERSION
                self._load_table_defs()
                need_setup = False

        if need_setup :
            self._setup(comment)

    #################################################################
    # Internal utilities
    #################################################################
    def _setup(self, comment : str) :
            config = {
                "schema_version" : CURRENT_SCHEMA_VERSION,
                "gertrude_version" : GERTRUDE_VERSION,
                "comment" : comment,
            }
            (self.db_path / "gertrude.conf").write_text(json.dumps(config))
            (self.db_path / "tables").mkdir(exist_ok=True)
    def _clear(self) :
        self.db_path.rmdir()
        self.db_path.mkdir()

    def _load_table_defs(self) :
        for table_path in self.db_path.glob("tables/*") :
            assert table_path.is_dir()
            table = Table(self, table_path, table_path.name, [])
            table._load_def()
            self.table_defs[table_path.name] = table


    #################################################################
    # Public API
    #################################################################
    def create_table(self, name : str, spec : Iterable[FieldSpec]) -> Table :
        # Name okay?
        if not NAME_REGEX.match(name) :
            raise ValueError(f"Invalid table name {name}")
        # is it unique?
        if name in self.table_defs :
            raise ValueError(f"Table {name} already exists.")
        
        # Does the directory exist?
        table_path = self.db_path / "tables" / name

        table = Table(self, table_path, name, spec)
        table._create()
        self.table_defs[name] = table

        return table
    
    def drop_table(self, table_name : str) :
        table = self.table_defs.pop(table_name)
        table._drop()

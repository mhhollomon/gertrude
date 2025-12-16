from typing import Iterable
from nanoid import generate
from pathlib import Path
import json
import regex as re

from .table import Table, FieldSpec

from .globals import ( CURRENT_SCHEMA_VERSION,
                      GERTRUDE_VERSION, NAME_REGEX)

from .int_id import IntegerIdGenerator

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
        self.id_gen = IntegerIdGenerator(self.db_path / "int_id")

        need_setup = True
        if not self.db_path.exists() :
            self.db_path.mkdir()
        else :
            assert self.db_path.is_dir()
            conf_file = self.db_path / "gertrude.conf"
            if len(list(self.db_path.glob("*"))) > 0:
                if not conf_file.exists() :
                    raise ValueError(f"Database {self.db_path} is not initialized.")
                else :
                    config = json.loads(conf_file.read_text())
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

    def _load_table_defs(self) :
        tables = self.db_path / "tables"
        if not tables.exists() :
            raise ValueError(f"Database {self.db_path} is not initialized.")

        for table_path in tables.glob("*") :
            assert table_path.is_dir()
            table = Table(self, table_path, table_path.name, [], self.id_gen)
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

        table = Table(self, table_path, name, spec, self.id_gen)
        table._create()
        self.table_defs[name] = table

        return table

    def drop_table(self, table_name : str) :
        table = self.table_defs.pop(table_name)
        table._drop()

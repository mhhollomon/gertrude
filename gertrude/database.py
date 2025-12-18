from typing import Iterable
from pathlib import Path
import json

from .table import Table, FieldSpec

from .globals import ( CURRENT_SCHEMA_VERSION,
                      GERTRUDE_VERSION, NAME_REGEX, DBContext
                      )

from .int_id import IntegerIdGenerator
from .cache import LRUCache

_OPTIONS = {
    "pk" : bool,
    "unique" : bool,
    "nullable" : bool,
}



#################################################################
# Database class
#################################################################
class Database :
    def __init__(self, db_path : Path, *, mode : str = "rw", comment : str = '') :
        self.db_path = db_path
        self.table_defs = {}
        self.mode = mode
        self.id_gen = IntegerIdGenerator(self.db_path / "int_id")
        self.db_ctx = DBContext(self.db_path, self.mode, self.id_gen, LRUCache(100))

        need_setup = True
        if not self.db_path.exists() :
            if self.mode == "ro" :
                raise ValueError(f"Database {self.db_path} does not exist.")

            self.db_path.mkdir()
        else :
            assert self.db_path.is_dir()
            conf_file = self.db_path / "gertrude.conf"

            # if the directory is not empty the conf file better be there.
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
            (self.db_path / "tables").mkdir()

    def _load_table_defs(self) :
        tables = self.db_path / "tables"
        if not tables.exists() :
            raise ValueError(f"Database {self.db_path} is not initialized.")

        for table_path in tables.glob("*") :
            assert table_path.is_dir()
            table = Table(table_path, table_path.name, [], self.db_ctx)
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

        table = Table(table_path, name, spec, self.db_ctx)
        table._create()
        self.table_defs[name] = table

        return table

    def drop_table(self, table_name : str) :
        table = self.table_defs.pop(table_name)
        table._drop()

    def get_cache_stats(self) :
        return self.db_ctx.cache.stats
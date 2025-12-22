from typing import Iterable, Self
from pathlib import Path
import json

from .globals import ( CURRENT_SCHEMA_VERSION,
                      GERTRUDE_VERSION, NAME_REGEX, DBContext
                      )

from .query import Query
from .table import Table, FieldSpec


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
        """Do not call directly. Use `Database.create` or `Database.open` instead.
        """
        self.db_path = db_path
        self.table_defs = {}
        self.mode = mode
        self.comment = comment


    #################################################################
    # Internal utilities
    #################################################################
    def _create(self) :
        config = {
            "schema_version" : CURRENT_SCHEMA_VERSION,
            "gertrude_version" : GERTRUDE_VERSION,
            "comment" : self.comment,
        }
        (self.db_path / "gertrude.conf").write_text(json.dumps(config))
        (self.db_path / "tables").mkdir()

        self.id_gen = IntegerIdGenerator(self.db_path / "int_id")
        self.db_ctx = DBContext(self.db_path, self.mode, self.id_gen, LRUCache(100))


    def _open(self) :
        self.id_gen = IntegerIdGenerator(self.db_path / "int_id")
        self.db_ctx = DBContext(self.db_path, self.mode, self.id_gen, LRUCache(100))

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

    @classmethod
    def create(cls, db_path : Path, *, mode : str = "rw", comment : str = '') -> Self:

        if db_path.exists() :
            if len(list(db_path.glob("*"))) > 0 :
                raise ValueError(f"Database {db_path} already exists and is not empty.")
        else :
            db_path.mkdir()

        db = cls(db_path, mode = mode, comment = comment)
        db._create()

        return db

    @classmethod
    def open(cls, db_path : Path | str, *, mode : str = "rw") -> Self:
        db_path = Path(db_path)

        if not db_path.exists() :
            raise ValueError(f"Database {db_path} does not exist.")
        if not db_path.is_dir() :
            raise ValueError(f"Database {db_path} is not a directory.")

        config = json.loads((db_path / "gertrude.conf").read_text())
        assert config["schema_version"] == CURRENT_SCHEMA_VERSION
        assert config["gertrude_version"] == GERTRUDE_VERSION
        db = cls(db_path, mode = mode, comment = config["comment"])
        db._open()

        return db

    def add_table(self, name : str, spec : Iterable[FieldSpec]) -> Table :
        if self.mode == "ro" :
            raise ValueError("Database is in read-only mode.")
        
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
        if self.mode == "ro" :
            raise ValueError("Database is in read-only mode.")
        
        if table_name not in self.table_defs :
            raise ValueError(f"Table {table_name} does not exist.")

        table = self.table_defs.pop(table_name)
        table._drop()

    def add_index(self, table_name : str, index_name : str, column : str, **kwargs) :
        if self.mode == "ro" :
            raise ValueError("Database is in read-only mode.")

        table = self.table_defs[table_name]
        table.add_index(index_name, column, **kwargs)
    
    def drop_index(self, table_name : str, index_name : str) :
        if self.mode == "ro" :
            raise ValueError("Database is in read-only mode.")

        self.table_defs[table_name].drop_index(index_name)

    @property
    def cache_stats(self) :
        return self.db_ctx.cache.stats
    
    def query(self, table_name : str) :
        if table_name not in self.table_defs :
            raise ValueError(f"Table {table_name} does not exist.")
        
        return Query(self, table_name)
    
    def table_list(self) :
        return list(self.table_defs.keys())
    
    def table(self, table_name : str) :
        return self.table_defs[table_name]
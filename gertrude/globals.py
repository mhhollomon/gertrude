from dataclasses import dataclass
import regex as re
from pathlib import Path
from typing import Any, NamedTuple

GERTRUDE_VERSION = "0.0.2"
CURRENT_SCHEMA_VERSION = 1

NAME_REGEX = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

TYPES = {
    "str" : str,
    "int" : int,
    "float" : float,
    "bool" : bool,
}

class GertrudeError(RuntimeError) :
    pass

from .int_id import IntegerIdGenerator
from .cache import LRUCache

_DB_OPTIONS = set(["index_fanout"])
@dataclass
class DBOptions :
    # decent compromise between insert performance and probe performance.
    index_fanout : int = 80
    index_cache_size : int = 128

class DBContext :
    def __init__(self, db_path : Path,
                 mode : str,
                 id_gen : IntegerIdGenerator,
                 cache : LRUCache,
                 options : DBOptions) :
        self.db_path = db_path
        self.rw_mode = mode
        self.id_gen = id_gen
        self.cache = cache
        self.options = options

    def path(self) -> Path :
        return self.db_path

    def mode(self) -> str :
        return self.rw_mode

    def generate_id(self) -> int :
        return self.id_gen.gen_id()


FieldSpec = NamedTuple("FieldSpec", [("name", str), ("type", str), ("options", dict[str, Any])])

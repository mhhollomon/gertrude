from dataclasses import dataclass
from enum import Enum
import regex as re
from nanoid import generate
from pathlib import Path
from typing import Any, Callable, NamedTuple, Tuple
from abc import ABC, abstractmethod

GERTRUDE_VERSION = "0.0.1"
CURRENT_SCHEMA_VERSION = 1

HEAP_ID_ALPHABET = '123456789ABCDEFGHIJKLMNPQRSTUVWXYZ'
HEAP_ID_LENGTH = 20

NAME_REGEX = re.compile(r'^[a-zA-Z0-9_-]+$')

TYPES = {
    "str" : str,
    "int" : int,
    "float" : float,
    "bool" : bool,
}

from .int_id import IntegerIdGenerator
from .cache import LRUCache

class DBContext :
    def __init__(self, db_path : Path,
                 mode : str,
                 id_gen : IntegerIdGenerator,
                 cache : LRUCache) :
        self.db_path = db_path
        self.rw_mode = mode
        self.id_gen = id_gen
        self.cache = cache

    def path(self) -> Path :
        return self.db_path

    def mode(self) -> str :
        return self.rw_mode

    def generate_id(self) -> int :
        return self.id_gen.gen_id()

def _generate_id():
    return generate(alphabet=HEAP_ID_ALPHABET, size=HEAP_ID_LENGTH)

class STEP_TYPE(Enum) :
    READ = 0
    FILTER = 1
    SELECT = 2
    SORT = 3
    ADD_COLUMN = 4

class Step(NamedTuple) :
    type : STEP_TYPE
    data : Any
    
###################################################################
# EXPRESSION CLASSES
###################################################################
class ExprNode(ABC):
    @abstractmethod
    def calc(self, row : dict[str, Any]) :
        ...

    @abstractmethod
    def to_python(self) :
        ...


@dataclass
class Operation(ExprNode) :
    category : str
    op : Callable[[Any, Any], Any]
    left : ExprNode
    right : ExprNode

    def name(self) :
        return self.op.__name__
    
    def calc(self, row : dict[str, Any]) :
        return self.op(self.left.calc(row), self.right.calc(row))

    def to_python(self) :
        return f"({self.left.to_python()} {self.op.__name__} {self.right.to_python()})"

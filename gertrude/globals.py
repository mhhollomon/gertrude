import regex as re
from nanoid import generate
import msgpack
from pathlib import Path
from typing import Any

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

def _save_to_heap(heap : Path, value : Any) -> str :
    """Saves to the heap pointed to by the path.
    Checks for path collisions.
    Returns the hash_id.
    """
    while True :
        hash_id = _generate_id()
        proposed_path = heap / hash_id[0:2] / hash_id[2: 4] / hash_id[4:]
        if not proposed_path.exists():
            break

    proposed_path.parent.mkdir(parents=True, exist_ok=True)

    with proposed_path.open("wb") as f:
        msgpack.dump(value, f)

    return hash_id

def _delete_from_heap(heap : Path, hash_id : str) -> Any :
    """ Note that the hash_id is not validated nor are any
    empty directories removed.
    """
    heap_path = heap / hash_id[0:2] / hash_id[2: 4] / hash_id[4:]

    if not heap_path.exists():
        return None

    retval = msgpack.unpackb(heap_path.read_bytes())

    heap_path.unlink()

    return retval


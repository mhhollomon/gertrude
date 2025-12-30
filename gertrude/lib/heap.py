"""Routines for dealing with the table heap.
The current implementation assumes heap blocks are only written once.
Data changes must be implemented with a read/delete/write cycle.
"""
from typing import Any
from nanoid import generate
from pathlib import Path
import msgpack

HEAP_ID_ALPHABET = '123456789ABCDEF'
HEAP_ID_LENGTH = 20

def generate_heap_id():
    return generate(alphabet=HEAP_ID_ALPHABET, size=HEAP_ID_LENGTH)


def write(heap : Path, value : Any) -> str :
    """Saves to the heap pointed to by the path.
    Checks for path collisions.
    Returns the hash_id.
    """
    while True :
        hash_id = generate_heap_id()
        proposed_path = heap / hash_id[0:2] / hash_id[2: 4] / hash_id[4:]
        if not proposed_path.exists():
            break

    proposed_path.parent.mkdir(parents=True, exist_ok=True)

    with proposed_path.open("wb") as f:
        msgpack.dump(value, f)

    return hash_id

def read(heap : Path, hash_id : str) -> Any :
    heap_path = heap / hash_id[0:2] / hash_id[2: 4] / hash_id[4:]

    if not heap_path.exists():
        return None

    return msgpack.unpackb(heap_path.read_bytes())

def delete(heap : Path, hash_id : str) -> Any :
    """ Note that the hash_id is not validated nor are any
    empty directories removed.
    If the block exists, it will return the content.
    """
    heap_path = heap / hash_id[0:2] / hash_id[2: 4] / hash_id[4:]

    if not heap_path.exists():
        return None

    retval = msgpack.unpackb(heap_path.read_bytes())

    heap_path.unlink()

    return retval

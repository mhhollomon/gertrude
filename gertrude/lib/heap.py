"""Routines for dealing with the table heap.
The current implementation assumes heap blocks are only written once.
Data changes must be implemented with a read/delete/write cycle.
"""
from typing import Any
from nanoid import generate
from pathlib import Path
import msgpack

class HeapID() :
    __slots__ = ('id')

    def __init__(self, id : str | bytes | int) :
        if isinstance(id, str) :
            self.id = int(id, base=16)
        elif isinstance(id, bytes) :
            self.id = int.from_bytes(id, 'big')
        elif isinstance(id, int) :
            self.id = id
        else :
            raise TypeError


    def __str__(self) :
        return f"{self.id:016X}"

    def __int__(self) :
        return self.id

    def __bytes__(self) :
        return self.id.to_bytes(8, 'big')

    def __eq__(self, other) :
        return self.id == other.id

    def __hash__(self) :
        return self.id

    def __repr__(self) :
        return f"heap_id({self.id})"

    def to_path(self) -> Path :
        s = self.__str__()
        return Path(s[0:2]) / s[2:4] / s[4:]


HEAP_ID_ALPHABET = '123456789ABCDEF'
HEAP_ID_LENGTH = 16

def generate_heap_id()-> HeapID :
    return HeapID(generate(alphabet=HEAP_ID_ALPHABET, size=HEAP_ID_LENGTH))

def heap_id_to_heap_path(heap_id : int | str | bytes | HeapID) -> Path :
    if isinstance(heap_id, HeapID) :
        return heap_id.to_path()
    else :
        return HeapID(heap_id).to_path()


def write(heap : Path, value : Any) -> HeapID :
    """Saves to the heap pointed to by the path.
    Checks for path collisions.
    Returns the hash_id.
    """
    while True :
        heap_id = generate_heap_id()
        proposed_path = heap / heap_id.to_path()
        if not proposed_path.exists():
            break

    proposed_path.parent.mkdir(parents=True, exist_ok=True)

    with proposed_path.open("wb") as f:
        msgpack.dump(value, f)

    return heap_id

def read(heap : Path, hash_id : str | int | bytes | HeapID) -> Any :
    heap_path = heap / heap_id_to_heap_path(hash_id)

    if not heap_path.exists():
        return None

    return msgpack.unpackb(heap_path.read_bytes())

def delete(heap : Path, hash_id : str | int | bytes | HeapID) -> Any :
    """ Note that the hash_id is not validated nor are any
    empty directories removed.
    If the block exists, it will return the content.
    """
    heap_path = heap / heap_id_to_heap_path(hash_id)

    if not heap_path.exists():
        return None

    retval = msgpack.unpackb(heap_path.read_bytes())

    heap_path.unlink()

    return retval

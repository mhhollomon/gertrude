"""Routines for dealing with the table heap.
The current implementation assumes heap blocks are only written once.
Data changes must be implemented with a read/delete/write cycle.
"""
from typing import Any
from nanoid import generate
from pathlib import Path
from . import packer

from .types.heap_id import HeapID


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
        heap_id = HeapID.generate()
        proposed_path = heap / heap_id.to_path()
        if not proposed_path.exists():
            break

    proposed_path.parent.mkdir(parents=True, exist_ok=True)

    with proposed_path.open("wb") as f:
        packer.packf(value, f)

    return heap_id

def read(heap : Path, hash_id : str | int | bytes | HeapID) -> Any :
    heap_path = heap / heap_id_to_heap_path(hash_id)

    if not heap_path.exists():
        return None

    return packer.unpack(heap_path.read_bytes())

def delete(heap : Path, hash_id : str | int | bytes | HeapID) -> Any :
    """ Note that the hash_id is not validated nor are any
    empty directories removed.
    If the block exists, it will return the content.
    """
    heap_path = heap / heap_id_to_heap_path(hash_id)

    if not heap_path.exists():
        return None

    retval = packer.unpack(heap_path.read_bytes())

    heap_path.unlink()

    return retval

from bisect import bisect_left, insort
import json
from pathlib import Path
from typing import Any, List, NamedTuple, Tuple, cast

import msgpack

from .globals import _generate_id, TYPES

_NODE_FANOUT = 6

type DataList = List[Tuple[Any, str]]


def _reserve_file_id(path : Path) -> str :
    while True :
        new_id = _generate_id()
        proposed_path = path / new_id
        if not proposed_path.exists():
            break
    proposed_path.write_bytes(b'')
    return new_id


def _read_node(path : Path) -> DataList:
    with open(path, "rb") as f :
        return cast(DataList, msgpack.load(f))
    
def _write_node(path : Path, node : DataList) :
    with open(path, "wb") as f :
        msgpack.dump(node, f)

def _create_node(path : Path, node : DataList) -> str :
    new_id = _reserve_file_id(path)
    new_file = path / new_id
    _write_node(new_file, node)

    return new_id


class Index :
    def __init__(self, index_name : str, path : Path, column : str, coltype : str) :
        self.index_name = index_name
        self.column = column
        self.coltype = coltype
        self.path = path
        self.real_type = TYPES[coltype]
        self.record = NamedTuple(index_name, [('key', self.real_type), ('value', str)])

    def _create(self, iterator) :
        if self.path.exists() :
            raise ValueError(f"Index {self.index_name} directory already exists.")

        self.path.mkdir()

        ## Dump config info
        (self.path / "config").write_text(json.dumps([self.column, self.coltype]))

        ## Create the root node
        block_list_path = self.path / "block_list"
        first_block_id = _reserve_file_id(self.path)
        # we'll work about splitting the node later
        records = []
        for record in iterator() :
            (heap_id, data) = record
            key = getattr(data, self.column)
            records.append((key, heap_id))
        
        if len(records) == 0 :
            block_list = []
        else :
            records.sort(key=lambda x : getattr(x, self.column))
            _write_node(self.path / first_block_id, records)
            block_list = [(records[0][0], first_block_id)]

        _write_node(block_list_path, block_list)

    
    def _insert(self, obj : Any, heap_id : str) :
        block_list = _read_node(self.path / "block_list")

        key = getattr(obj, self.column)

        i = bisect_left(block_list, key)
        if i == len(block_list) :
            pass # need to add a new block or something - not sure
        else :
            leaf_id = block_list[i][1]
            leaf = _read_node(self.path / leaf_id)
            insort(leaf, (key, heap_id))
            _write_node(self.path / leaf_id, leaf)

        #_write_node(self.path / "block_list", block_list)



    def _split(self, node : DataList, orig_id : str) :
        left_data = node[:_NODE_FANOUT//2]
        left_id = _reserve_file_id(self.path)
        right_data = node[_NODE_FANOUT//2:]
        right_id = _reserve_file_id(self.path)

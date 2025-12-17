from bisect import bisect_left, insort
import json
import msgpack
from pathlib import Path
from typing import Any, List, NamedTuple, Tuple, cast

from .globals import _generate_id, TYPES, DBContext

_NODE_FANOUT = 6

# Used by the block list
type DataList = List[Tuple[Any, int]]
# use by leaf nodes
type LeafData = List[Tuple[Any, str]]

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

class Index :
    def __init__(self, index_name : str, path : Path, column : str, coltype : str, db_ctx : DBContext) :
        self.index_name = index_name
        self.column = column
        self.coltype = coltype
        self.path = path
        self.real_type = TYPES[coltype]
        self.record = NamedTuple(index_name, [('key', self.real_type), ('value', str)])
        self.db_ctx = db_ctx

    def _write_node(self, path : int | Path, node : DataList | LeafData) :
        if not isinstance(path, Path) :
            path = self.path / f"{path:03}"
        with open(path, "wb") as f :
            msgpack.dump(node, f)

    def _read_node(self, node_id : int) -> LeafData:
        path = self.path / f"{node_id:03}"
        with open(path, "rb") as f :
            return cast(LeafData, msgpack.load(f))


    def _create(self, iterator) :
        if self.path.exists() :
            raise ValueError(f"Index {self.index_name} directory already exists.")

        self.path.mkdir()

        self.id = self.db_ctx.generate_id()

        config = {
            "column" : self.column,
            "coltype" : self.coltype,
            "id" : self.id
        }

        ## Dump config info
        (self.path / "config").write_text(json.dumps(config))

        ## Create the root node
        block_list_path = self.path / "block_list"
        first_block_id = self.db_ctx.generate_id()
        # we'll work about splitting the node later
        records = []
        for record in iterator() :
            (heap_id, data) = record
            key = getattr(data, self.column)
            records.append((key, heap_id))

        records.sort(key=lambda x : x[0])

        # TODO : Split this into multiple nodes if needed.
        self._write_node(first_block_id, records)

        block_list = [(None, first_block_id)]

        self._write_node(block_list_path, block_list)


    def _insert(self, obj : Any, heap_id : str) :
        block_list = _read_node(self.path / "block_list")

        key = getattr(obj, self.column)
        print(f"Inserting {key}")

        # Find which block the key may be in.
        # The block pointed to by index n has keys that
        # greater than or equal to the key at index n.
        # So we need to find the largest key that is still less than
        # the given key.
        # index=0 is for those keys that are strictly less than
        # the first key.
        i = bisect_left(block_list, key, lo=1, key=lambda x : x[0])
        print(f"raw i = {i}")
        # if the index is 1, it is either because we need to
        # look at the block at index 1 or we need to look at
        # the block at index 0.
        if i == 1 :
            # If the block_list has only one entry, then
            # we need to look at the block at index 0.
            # If the given key is less that the key at index 1,
            # then we need to look at the block at index 0.
            if i == len(block_list) or block_list[i][0] < key :
                i = 0
        elif i == len(block_list) :
            i -= 1
        print(f"final i = {i}")
        leaf_id = block_list[i][1]

        leaf = self._read_node(leaf_id)
        insort(leaf, (key, heap_id), key=lambda x : x[0])

        if len(leaf) >= _NODE_FANOUT :
            self._split(leaf, leaf_id, block_list, i)
        else :
            self._write_node(leaf_id, leaf)



    def _split(self, node : LeafData, orig_id : int, block_list : DataList, index : int) :
        left_data = node[:_NODE_FANOUT//2]
        right_data = node[_NODE_FANOUT//2:]
        self._write_node(orig_id, left_data)
        right_id = self.db_ctx.generate_id()
        self._write_node(right_id, right_data)

        block_list.insert(index+1, (right_data[0][0], right_id))

        self._write_node(self.path / "block_list", block_list)
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
    def __init__(self, index_name : str, path : Path,
                 column : str, coltype : str, db_ctx : DBContext, *, unique : bool = False) :
        self.index_name = index_name
        self.column = column
        self.coltype = coltype
        self.path = path
        self.real_type = TYPES[coltype]
        self.record = NamedTuple(index_name, [('key', self.real_type), ('value', str)])
        self.db_ctx = db_ctx
        self.unique = unique

        # cache the block_list
        self.block_list : DataList | None = None

    def _get_block_list(self) :
        if self.block_list is None :
            self.block_list = _read_node(self.path / "block_list")
        return self.block_list

    def _write_node(self, node_id : int, node : LeafData) :
        path = self.path / f"{node_id:03}"
        with open(path, "wb") as f :
            msgpack.dump(node, f)

    def _read_node(self, node_id : int) -> LeafData:
        path = self.path / f"{node_id:03}"
        with open(path, "rb") as f :
            return cast(LeafData, msgpack.load(f))

    def _update_block_list(self, block_list : DataList) :
        self.block_list = block_list
        with open(self.path / "block_list", "wb") as f :
            msgpack.dump(block_list, f)


    def _create(self, iterator) :
        if self.path.exists() :
            raise ValueError(f"Index {self.index_name} directory already exists.")

        self.path.mkdir()

        self.id = self.db_ctx.generate_id()

        config = {
            "column" : self.column,
            "coltype" : self.coltype,
            "id" : self.id,
            "unique" : self.unique,
        }

        ## Dump config info
        (self.path / "config").write_text(json.dumps(config))

        ## Create the root node
        block_list_path = self.path / "block_list"
        first_block_id = self.db_ctx.generate_id()
        # we'll work about splitting the node later
        records = []
        keyset = set()
        for record in iterator() :
            (heap_id, data) = record
            key = getattr(data, self.column)
            records.append((key, heap_id))

            if self.unique :
                if key in keyset :
                    raise ValueError(f"Duplicate key {key} in unique index {self.index_name}")
                keyset.add(key)

        records.sort(key=lambda x : x[0])

        # TODO : Split this into multiple nodes if needed.
        self._write_node(first_block_id, records)

        block_list = [(None, first_block_id)]

        self._update_block_list(block_list)

    def _find_block(self, key : Any) -> Tuple[int, int] :
        block_list = self._get_block_list()
        print(f"Finding block for key = '{key}'")

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

        return (leaf_id, i)

    def _insert(self, obj : Any, heap_id : str) :

        key = getattr(obj, self.column)
        leaf_id, i = self._find_block(key)

        leaf = self._read_node(leaf_id)
        insort(leaf, (key, heap_id), key=lambda x : x[0])

        if len(leaf) >= _NODE_FANOUT :
            block_list = self._get_block_list()
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

        self._update_block_list(block_list)

    def test_for_insert(self, record : Any) -> Tuple[bool, str] :
        if not self.unique :
            return (True, "")

        key = getattr(record, self.column)

        leaf_id, i = self._find_block(key)
        leaf = self._read_node(leaf_id)
        i = bisect_left(leaf, key, key=lambda x : x[0])

        if i < len(leaf) and leaf[i][0] == key :
            return False, f"Duplicate key in column '{self.column}' for index {self.index_name}"

        return True, ""
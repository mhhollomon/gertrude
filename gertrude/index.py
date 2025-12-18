from bisect import bisect_left, insort
import json
import msgpack
from pathlib import Path
from typing import Any, List, NamedTuple, Tuple, cast

from .globals import _generate_id, TYPES, DBContext

_NODE_FANOUT : int = 6

# Used by the block list
type DataList = List[Tuple[Any, int]]
# use by leaf nodes
type LeafData = List[Tuple[Any, str]]


def _read_block_list(path : Path) -> DataList:
    with open(path, "rb") as f :
        return cast(DataList, msgpack.load(f))

class Index :
    def __init__(self, index_name : str, path : Path,
                 column : str, coltype : str, db_ctx : DBContext, *, 
                 unique : bool = False, nullable : bool = True) :
        self.index_name = index_name
        self.column = column
        self.coltype = coltype
        self.path = path
        self.real_type = TYPES[coltype]
        self.record = NamedTuple(index_name, [('key', self.real_type), ('value', str)])
        self.db_ctx = db_ctx
        self.unique = unique
        self.nullable = nullable

        # cache the block_list
        self.block_list : DataList | None = None

    def _get_block_list(self) :
        if self.block_list is None :
            self.block_list = _read_block_list(self.path / "block_list")
        return self.block_list

    def _write_node(self, node_id : int, node : LeafData, cache : bool = True) :
        raw_data = cast(bytes, (msgpack.dumps(node)))
        self.db_ctx.cache.put(self.id, node_id, raw_data, cache=cache)

    def _read_node(self, node_id : int) -> LeafData:
        raw_data = self.db_ctx.cache.get(self.id, node_id)
        return cast(LeafData, msgpack.loads(raw_data))

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
            "nullable" : self.nullable,
        }

        ## Dump config info
        (self.path / "config").write_text(json.dumps(config))

        ## Register with the cache
        self.db_ctx.cache.register(self.id, self.path)

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

            if not self.nullable :
                if key is None :
                    raise ValueError(f"Null key in non-nullable index {self.index_name}")

        block_list = []
        records.sort(key=lambda x : x[0])

        init_fanout = int(_NODE_FANOUT * 0.75)

        while len(records) >= init_fanout :
            new_block_id = self.db_ctx.generate_id()
            block_list.append((records[0][0], new_block_id))
            self._write_node(new_block_id, records[:init_fanout], cache=False)
            records = records[init_fanout:]

        if len(records) > 0 :
            new_block_id = self.db_ctx.generate_id()
            block_list.append((records[0][0], new_block_id))
            self._write_node(new_block_id, records, cache=False)

        if len(block_list) > 0 :
            block_list[0] = (None, block_list[0][1])
        else :
            # create an empty block
            first_block_id = self.db_ctx.generate_id()
            self._write_node(first_block_id, [], cache=False)
            block_list.append((None, first_block_id))

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
            if i == len(block_list) or block_list[i][0] > key :
                i = 0
        elif i == len(block_list) or block_list[i][0] > key :
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

    def _pick_split_point(self, node : LeafData) :
        # Calculate where to split the block.
        # Prefer to split it down the middle, but if
        # there are multiple entries with the same key,
        # we need to make sure all the entries with the same key
        # are in the same block.
        split_point = _NODE_FANOUT//2
        split_key = node[split_point][0]
        left_offset = 0
        while True:
            if split_point - left_offset < 0 :
                break
            if node[split_point - left_offset][0] < split_key :
                left_offset -= 1
                break
            left_offset += 1

        right_offset = 0
        while True:
            if split_point + right_offset >= len(node) :
                break
            if node[split_point + right_offset][0] > split_key :
                break
            right_offset += 1
        
        print(f"left_offset = {left_offset}, right_offset = {right_offset}")
        if left_offset < right_offset :
            new_split_point = split_point - left_offset
            if new_split_point <= 0 :
                if split_point + right_offset >= len(node) :
                    # The block is full of the same key.
                    # So split down the middle.
                    new_split_point = split_point
                else :
                    new_split_point = split_point + right_offset
        else :
            new_split_point = split_point + right_offset
            if new_split_point >= len(node) :
                if split_point - left_offset < 0 :
                    # The block is full of the same key.
                    # So split down the middle.
                    new_split_point = split_point
                else :
                    new_split_point = split_point - left_offset

        return new_split_point

    def _split(self, node : LeafData, orig_id : int, block_list : DataList, index : int) :

        print(f"--- splitting {orig_id} at block_list index {index}")
            
        split_point = self._pick_split_point(node)
        print(f"split_point = {split_point}")

        left_data = node[:split_point]
        right_data = node[split_point:]
        print(f"left_data = {left_data}")
        print(f"right_data = {right_data}")

        self._write_node(orig_id, left_data)
        right_id = self.db_ctx.generate_id()
        self._write_node(right_id, right_data)

        block_list.insert(index+1, (right_data[0][0], right_id))

        self._update_block_list(block_list)

    def test_for_insert(self, record : Any) -> Tuple[bool, str] :
        if not self.nullable :
            key = getattr(record, self.column)
            if key is None :
                return False, f"Null key in non-nullable index {self.index_name}"

        if not self.unique :
            return (True, "")
        
        # check for duplicate key
        key = getattr(record, self.column)

        leaf_id, i = self._find_block(key)
        leaf = self._read_node(leaf_id)
        i = bisect_left(leaf, key, key=lambda x : x[0])

        if i < len(leaf) and leaf[i][0] == key :
            return False, f"Duplicate key in column '{self.column}' for index {self.index_name}"

        return True, ""
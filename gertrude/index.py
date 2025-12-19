from bisect import bisect_left, insort
from dataclasses import dataclass, asdict
import json
import msgpack
from pathlib import Path
from typing import Any, List, NamedTuple, Tuple, cast

from .globals import TYPES, DBContext

_NODE_FANOUT : int = 6

type KeyTuple = Tuple[bool, Any]

# Used by the block list
type DataList = List[Tuple[KeyTuple, int]]
# use by leaf nodes
type LeafData = List[Tuple[KeyTuple, str]]

@dataclass
class LeafNode :
    k : str
    d : LeafData

_NODE_TYPE_LEAF = 'L'

@dataclass
class InternalNode :
    k : str
    d : DataList

_NODE_TYPE_INTERNAL = 'I'

def _read_block_list(path : Path) -> DataList:
    with open(path, "rb") as f :
        return cast(DataList, msgpack.load(f))
    
def _make_leaf(d : LeafData) :
    return LeafNode(_NODE_TYPE_LEAF, d)

def _make_internal(d : DataList) :
    return InternalNode(_NODE_TYPE_INTERNAL, d)

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

        # We'll fix this later
        self.id : int= 0



    def _write_node(self, node_id : int, node : LeafNode | InternalNode, cache : bool = True) :
        raw_data = cast(bytes, (msgpack.dumps(asdict(node))))
        self.db_ctx.cache.put(self.id, node_id, raw_data, cache=cache)

    def _read_node(self, node_id : int) -> LeafNode | InternalNode:
        raw_data = self.db_ctx.cache.get(self.id, node_id)
        data = cast (dict, msgpack.loads(raw_data))
        if raw_data[0] == _NODE_TYPE_LEAF :
            return LeafNode(**data)
        else :
            return InternalNode(**data)


    def _read_root(self) -> InternalNode :
        return cast(InternalNode, self._read_node(0))
    
    def _gen_key_tuple(self, key : Any) -> KeyTuple :
        if key is None :
            return (True, None)
        else :
            return (False, key)
        
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

            if self.unique :
                if key in keyset :
                    raise ValueError(f"Duplicate key {key} in unique index {self.index_name}")
                keyset.add(key)

            if not self.nullable :
                if key is None :
                    raise ValueError(f"Null key in non-nullable index {self.index_name}")

            records.append((self._gen_key_tuple(key), heap_id))

        root = []
        records.sort(key=lambda x : x[0])

        init_fanout = int(_NODE_FANOUT * 0.75)

        while len(records) >= init_fanout :
            new_block_id = self.db_ctx.generate_id()
            root.append((records[0][0], new_block_id))
            new_node = LeafNode(_NODE_TYPE_LEAF, records[:init_fanout])
            self._write_node(new_block_id, new_node, cache=False)
            records = records[init_fanout:]

        if len(records) > 0 :
            new_block_id = self.db_ctx.generate_id()
            root.append((records[0][0], new_block_id))
            new_node = LeafNode(_NODE_TYPE_LEAF, records)

            self._write_node(new_block_id, new_node, cache=False)

        if len(root) > 0 :
            root[0] = (None, root[0][1])
        else :
            # create an empty block
            first_block_id = self.db_ctx.generate_id()
            new_node = LeafNode(_NODE_TYPE_LEAF, [])
            self._write_node(first_block_id, new_node, cache=False)
            root.append((None, first_block_id))

        self._write_node(0, _make_internal(root))

    def _find_block(self, key : Any) -> Tuple[int, int] :
        root = self._read_root()
        print(f"Finding block for key = '{key}'")

        # Find which block the key may be in.
        # The block pointed to by index n has keys that
        # greater than or equal to the key at index n.
        # So we need to find the largest key that is still less than
        # the given key.
        # index=0 is for those keys that are strictly less than
        # the first key.
        i = bisect_left(root.d, key, lo=1, key=lambda x : tuple(x[0]))
        print(f"raw i = {i}")
        # if the index is 1, it is either because we need to
        # look at the block at index 1 or we need to look at
        # the block at index 0.
        if i == 1 :
            # If the block_list has only one entry, then
            # we need to look at the block at index 0.
            # If the given key is less that the key at index 1,
            # then we need to look at the block at index 0.
            if i == len(root.d) or tuple(root.d[i][0]) > key :
                i = 0
        elif i == len(root.d) or tuple(root.d[i][0]) > key :
            i -= 1
        print(f"final i = {i}")
        leaf_id = root.d[i][1]

        return (leaf_id, i)

    def _insert(self, obj : Any, heap_id : str) :

        key = self._gen_key_tuple(getattr(obj, self.column))
        leaf_id, i = self._find_block(key)

        leaf = self._read_node(leaf_id)
        if leaf.k == _NODE_TYPE_LEAF :
            leaf = cast(LeafNode, leaf)
        else :
            raise ValueError(f"Invalid node type {leaf.k}")
        
        insort(leaf.d, (key, heap_id), key=lambda x : tuple(x[0]))

        if len(leaf.d) >= _NODE_FANOUT :
            root = self._read_root()
            self._split(leaf, leaf_id, root, i)
        else :
            self._write_node(leaf_id, leaf)

    def _pick_split_point(self, node : LeafNode) :
        # Calculate where to split the block.
        # Prefer to split it down the middle, but if
        # there are multiple entries with the same key,
        # we need to make sure all the entries with the same key
        # are in the same block.
        split_point = _NODE_FANOUT//2
        split_key = tuple(node.d[split_point][0])
        left_offset = 0
        while True:
            if split_point - left_offset < 0 :
                break
            if tuple(node.d[split_point - left_offset][0]) < split_key :
                left_offset -= 1
                break
            left_offset += 1

        right_offset = 0
        while True:
            if split_point + right_offset >= len(node.d) :
                break
            if tuple(node.d[split_point + right_offset][0]) > split_key :
                break
            right_offset += 1
        
        print(f"left_offset = {left_offset}, right_offset = {right_offset}")
        if left_offset < right_offset :
            new_split_point = split_point - left_offset
            if new_split_point <= 0 :
                if split_point + right_offset >= len(node.d) :
                    # The block is full of the same key.
                    # So split down the middle.
                    new_split_point = split_point
                else :
                    new_split_point = split_point + right_offset
        else :
            new_split_point = split_point + right_offset
            if new_split_point >= len(node.d) :
                if split_point - left_offset < 0 :
                    # The block is full of the same key.
                    # So split down the middle.
                    new_split_point = split_point
                else :
                    new_split_point = split_point - left_offset

        return new_split_point

    def _split(self, node : LeafNode, orig_id : int, root : InternalNode, index : int) :

        print(f"--- splitting {orig_id} at root index {index}")
            
        split_point = self._pick_split_point(node)
        print(f"split_point = {split_point}")

        left_data = node.d[:split_point]
        right_data = node.d[split_point:]
        print(f"left_data = {left_data}")
        print(f"right_data = {right_data}")

        self._write_node(orig_id, _make_leaf(left_data))
        right_id = self.db_ctx.generate_id()
        self._write_node(right_id, _make_leaf(right_data))

        root.d.insert(index+1, (right_data[0][0], right_id))

        self._write_node(0, root)

    def test_for_insert(self, record : Any) -> Tuple[bool, str] :
        if not self.nullable :
            key = getattr(record, self.column)
            if key is None :
                return False, f"Null key in non-nullable index {self.index_name}"

        if not self.unique :
            return (True, "")
        
        # check for duplicate key
        key = self._gen_key_tuple(getattr(record, self.column))

        leaf_id, i = self._find_block(key)
        leaf = self._read_node(leaf_id)
        i = bisect_left(leaf.d, key, key=lambda x : tuple(x[0]))

        if i < len(leaf.d) and leaf.d[i][0] == key :
            return False, f"Duplicate key in column '{self.column}' for index {self.index_name}"

        return True, ""
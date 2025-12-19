from bisect import bisect_left, insort
from dataclasses import dataclass, asdict
import json
import msgpack
from pathlib import Path
from typing import Any, List, NamedTuple, Optional, Tuple, cast

from .globals import TYPES, DBContext

_NODE_FANOUT : int = 6

type KeyTuple = Tuple[bool, Any]

# Used by the block list
type DataList = List[Tuple[KeyTuple, int]]
# use by leaf nodes
type LeafData = List[Tuple[KeyTuple, str]]

# List of tuple (block_id, index)
# zeroth entry will always be (0, _INVALID_INDEX)
# Middle entries will always be (internal_block_id, index_into_parent)
# The last entry will always be (leaf_block_id, index_into_parent)
type TreePath = List[Tuple[int, int]]

_INVALID_INDEX = -10

@dataclass
class LeafNode :
    k : str        # node type
    n : int        # node id
    d : LeafData

_NODE_TYPE_LEAF = 'L'

@dataclass
class InternalNode :
    k : str       # node type
    n : int       # node id
    d : DataList

_NODE_TYPE_INTERNAL = 'I'

def _make_leaf(node_id : int, d : LeafData) :
    return LeafNode(_NODE_TYPE_LEAF, node_id, d)

def _make_internal(node_id : int, d : DataList) :
    return InternalNode(_NODE_TYPE_INTERNAL, node_id, d)

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
        # see _create or _load
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
            "name" : self.index_name,
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
            new_node = _make_leaf(new_block_id, records[:init_fanout])
            self._write_node(new_block_id, new_node, cache=False)
            records = records[init_fanout:]

        if len(records) > 0 :
            new_block_id = self.db_ctx.generate_id()
            root.append((records[0][0], new_block_id))
            new_node = _make_leaf(new_block_id, records)
            self._write_node(new_block_id, new_node, cache=False)

        if len(root) > 0 :
            root[0] = (self._gen_key_tuple(None), root[0][1])
        else :
            # create an empty block
            first_block_id = self.db_ctx.generate_id()
            new_node = _make_leaf(first_block_id, [])
            self._write_node(first_block_id, new_node, cache=False)
            root.append((self._gen_key_tuple(None), first_block_id))

        self._write_node(0, _make_internal(0, root))
        #self.print_tree()

    @classmethod
    def _load(cls, path : Path, db_ctx : DBContext) :
        config = json.loads((path / "config").read_text())

        index = Index(config["name"], path, config["column"], config["coltype"],
                      db_ctx, unique=config["unique"], nullable=config["nullable"])

        index.id = config["id"]
        db_ctx.cache.register(index.id, path)
        return index
        
    def _find_key_in_leaf(self, key : KeyTuple, leaf : LeafNode) -> Tuple[bool, int] :
        """Check if a key is in a leaf node. If so, return the index.
        """

        i = bisect_left(leaf.d, key, key=lambda x : tuple(x[0]))
        print(f"_find_key_in_leaf: i = {i}")
        if i < len(leaf.d) and tuple(leaf.d[i][0]) == key :
            return True, i
        else :
            return False, -1
    
    def _find_block(self, key : KeyTuple, parent : Optional[InternalNode] = None) -> TreePath :
        """Returns the path to the leaf node where the key might be stored.
        Each entry in the list is a tuple (block_id, index).
        """

        retval : TreePath = []
        if parent is None :
            parent = self._read_root()
            retval += [(0, _INVALID_INDEX)]
        print(f"Finding block for key = '{key}'")

        # Find which block the key may be in.
        # The block pointed to by index n has keys that
        # greater than or equal to the key at index n.
        # So we need to find the largest key that is still less than
        # the given key.
        # index=0 is for those keys that are strictly less than
        # the first key.
        i = bisect_left(parent.d, key, lo=1, key=lambda x : tuple(x[0]))
        print(f"raw i = {i}")
        # if the index is 1, it is either because we need to
        # look at the block at index 1 or we need to look at
        # the block at index 0.
        if i == 1 :
            # If the block_list has only one entry, then
            # we need to look at the block at index 0.
            # If the given key is less that the key at index 1,
            # then we need to look at the block at index 0.
            if i == len(parent.d) or tuple(parent.d[i][0]) > key :
                i = 0
        elif i == len(parent.d) or tuple(parent.d[i][0]) > key :
            i -= 1
        print(f"final i = {i}")
        leaf_id = parent.d[i][1]

        retval += [(leaf_id, i)]


        maybe_leaf = self._read_node(leaf_id)
        if maybe_leaf.k == _NODE_TYPE_INTERNAL :
            maybe_leaf = cast(InternalNode, maybe_leaf)
            return retval + self._find_block(key, maybe_leaf)

        print(f"_find_block returning {retval}")
        return retval

    def _insert(self, obj : Any, heap_id : str) :
        key : KeyTuple= self._gen_key_tuple(getattr(obj, self.column))
        print(f"--- Inserting {key} into index {self.index_name}")

        # In theory, this is not needed since check_for_insert
        # should have been called. But its cheap, so why not.
        if not self.nullable and key[0] :
            raise ValueError(f"Null key in non-nullable index {self.index_name}")
        
        tree_path = self._find_block(key)
        
        leaf_id, parent_index = tree_path[-1]
        parent_id, parent_parent_index = tree_path[-2]

        leaf = self._read_node(leaf_id)
        if leaf.k == _NODE_TYPE_LEAF :
            leaf = cast(LeafNode, leaf)
        else :
            raise ValueError(f"Invalid node type {leaf.k} for leaf node {leaf_id}")

        insort(leaf.d, (key, heap_id), key=lambda x : tuple(x[0]))

        if len(leaf.d) >= _NODE_FANOUT :
            self._split_leaf(leaf, parent_index, tree_path[:-1])
        else :
            self._write_node(leaf_id, leaf)

        #self.print_tree()

    def _pick_split_point(self, node : LeafNode | InternalNode) -> int :
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

    def _split_leaf(self, node : LeafNode,  parent_index : int, tree_path : TreePath) :

        parent_id, parent_parent_index = tree_path[-1]
        parent = self._read_node(parent_id)
        if parent.k == _NODE_TYPE_INTERNAL :
            parent = cast(InternalNode, parent)
        else :
            raise ValueError(f"Invalid node type {parent.k} for internal node {parent_id}")

        print(f"--- splitting {node.n} at parent {parent.n}, index {parent_index}")

        split_point = self._pick_split_point(node)
        print(f"split_point = {split_point}")

        left_data = node.d[:split_point]
        right_data = node.d[split_point:]
        print(f"left_data = {left_data}")
        print(f"right_data = {right_data}")

        self._write_node(node.n, _make_leaf(node.n, left_data))
        right_id = self.db_ctx.generate_id()
        self._write_node(right_id, _make_leaf(right_id, right_data))

        parent.d.insert(parent_index+1, (right_data[0][0], right_id))

        if len(parent.d) >= _NODE_FANOUT :
            self._split_internal(parent, parent_parent_index, tree_path[:-1])
        else :
            self._write_node(parent.n, parent)

        #self.print_tree()

    def _split_internal(self, node : InternalNode, parent_index : int, tree_path : TreePath) :

        splitting_root : bool = (node.n == 0)
        if (parent_index == _INVALID_INDEX) :
            parent_id = 0
            parent_parent_index = _INVALID_INDEX
        else :
            parent_id, parent_parent_index = tree_path[-1]
        parent = self._read_node(parent_id)
        if parent.k == _NODE_TYPE_INTERNAL :
            parent = cast(InternalNode, parent)
        else :
            raise ValueError(f"Invalid node type {parent.k} for internal node {parent_id}")

        print(f"--- splitting {node.n} at parent {parent.n}, index {parent_index}")

        split_point = self._pick_split_point(node)
        print(f"split_point = {split_point}")

        left_data = node.d[:split_point]
        right_data = node.d[split_point:]
        print(f"left_data = {left_data}")
        print(f"right_data = {right_data}")

        if splitting_root :
            left_id = self.db_ctx.generate_id()
            right_id = self.db_ctx.generate_id()
            new_root = _make_internal(0, [])
            new_root.d.append((self._gen_key_tuple(None), left_id))
            new_root.d.append((right_data[0][0], right_id))
            right_data[0] = (self._gen_key_tuple(None), right_data[0][1])
            self._write_node(left_id, _make_internal(left_id, left_data))
            self._write_node(right_id, _make_internal(right_id, right_data))
            self._write_node(0, new_root)

        else :

            right_id = self.db_ctx.generate_id()
            parent.d.insert(parent_index+1, (right_data[0][0], right_id))
            right_data[0] = (self._gen_key_tuple(None), right_data[0][1])
            self._write_node(node.n, _make_internal(node.n, left_data))
            self._write_node(right_id, _make_internal(right_id, right_data))

            if len(parent.d) >= _NODE_FANOUT :
                self._split_internal(parent, parent_parent_index, tree_path[:-1])
            else :
                self._write_node(parent.n, parent)

    def test_for_insert(self, record : Any) -> Tuple[bool, str] :
        """Method to check if the record meets the index constraints.
        This must be called before _insert() on the record.
        """
        raw_key = getattr(record, self.column)
        print(f"---- Testing key {raw_key} for index {self.index_name}")

        if not self.nullable :
            if raw_key is None :
                print(f"--- Null key in non-nullable index {self.index_name}")
                return False, f"Null key in non-nullable index {self.index_name}"

        if not self.unique :
            print(f"--- Non-unique index {self.index_name}")
            return (True, "")

        # check for duplicate key
        key = self._gen_key_tuple(raw_key)

        leaf_id, i = self._find_block(key)[-1]
        print(f"--- leaf_id = {leaf_id}, i = {i}")
        leaf = self._read_node(leaf_id)

        test = self._find_key_in_leaf(key, cast( LeafNode, leaf))
        print(f"--- test = {test}")

        if test[0] :
            print(f"--- Duplicate key '{raw_key}' in unique index {self.index_name}")
            return False, f"Duplicate key '{raw_key}' in unique index {self.index_name}"

        print("--- OK")
        return True, ""
    

    def print_tree(self) :
        print(f"=== {self.index_name} Tree:")
        self._print_tree(0, '')
        print("=== End of tree")

    def _print_tree(self, node_id : int, prefix : str) :
        node = self._read_node(node_id)
        print(f"{prefix}{node.n} {node.k} :")
        prefix = prefix + '  '

        if node.k == _NODE_TYPE_INTERNAL :
            node = cast(InternalNode, node)
            for n in node.d :
                print(f"{prefix}{n[0]} -> {n[1]}")
                self._print_tree(n[1], prefix + ' ')
        else :
            node = cast(LeafNode, node)
            for n in node.d :
                print(f"{prefix}{n[0]} -> {n[1]}")
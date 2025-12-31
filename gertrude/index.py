from bisect import bisect_left, insort, bisect_right
from dataclasses import dataclass, asdict
from enum import Enum
import json
import msgpack
from pathlib import Path
from typing import Any, List, NamedTuple, Optional, Tuple, cast
import operator as pyops

from .globals import TYPES, DBContext, _Row

import logging
logger = logging.getLogger(__name__)

_NODE_FANOUT : int = 6

type KeyTuple = Tuple[bool, Any]

# Used by the block list
type DataList = List[Tuple[KeyTuple, int]]
# use by leaf nodes
type LeafData = List[Tuple[KeyTuple, str]]

class tpi(NamedTuple) :
    block_id : int
    index : int
# List of tuple (block_id, index)
# zeroth entry will always be (0, _INVALID_INDEX)
# Middle entries will always be (internal_block_id, index_into_parent)
# The last entry will always be (leaf_block_id, index_into_parent)
type TreePath = List[tpi]

_INVALID_INDEX = -10

class KeyBound(Enum) :
    NONE = 0
    LOWER = 1
    UPPER = 2

OPERATOR_MAP = {
    'gt' : 'gt',
    'ge' : 'ge',
    'lt' : 'lt',
    'le' : 'le',
    'eq' : 'eq',
    'eq_' : 'eq',
    '='  : 'eq',
    '>=' : 'ge',
    '<=' : 'le',
    '>'  : 'gt',
    '<'  : 'lt',
}

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
        self._column = column
        self.coltype = coltype
        self.path = path
        self.real_type = TYPES[coltype]
        self.record = NamedTuple(index_name, [('key', self.real_type), ('value', str)])
        self.db_ctx = db_ctx
        self.unique = unique
        self.nullable = nullable

        self.closed = False

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
            "column" : self._column,
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
            key = data[self._column]

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
        logger.debug(f"_find_key_in_leaf: i = {i}")
        if i < len(leaf.d) and tuple(leaf.d[i][0]) == key :
            return True, i
        else :
            return False, -1

    #################################################################
    # FIND_BLOCK2
    #################################################################
    def _find_block2(self, key : KeyTuple,
                     parent : Optional[InternalNode] = None,
                     lower_bound : bool = True) -> TreePath :
        bisect_func = bisect_left if lower_bound else bisect_right
        retval : TreePath = []
        if parent is None :
            parent = self._read_root()
        logger.debug(f"_find_block2: Finding pointer in block {parent.n} for key = '{key}'")
        logger.debug(f"_find_block2: lower_bound = {lower_bound}")
        i = bisect_func(parent.d, key, lo=1, key=lambda x : tuple(x[0]))
        logger.debug(f"_find_block2: raw i = {i}")
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
        logger.debug(f"_find_block2: final i = {i}")
        next_block_id = parent.d[i][1]
        retval += [tpi(parent.n, i)]
        next_node = self._read_node(next_block_id)
        if next_node.k == _NODE_TYPE_INTERNAL :
            next_node = cast(InternalNode, next_node)
            logger.debug(f"_find_block2: calling _find_block2 recursively")
            retval = retval + self._find_block2(key, parent=next_node, lower_bound=lower_bound)
        else :
            next_node = cast(LeafNode, next_node)
            logger.debug(f"_find_block2: in leaf node {next_block_id}")
            i = bisect_func(next_node.d, key, key=lambda x : tuple(x[0]))
            logger.debug(f"_find_block2: leaf i = {i}")
            check_index = i if lower_bound else i-1
            if i >= len(next_node.d) or tuple(next_node.d[check_index][0]) != key :
                i = _INVALID_INDEX
            retval += [tpi(next_block_id, i)]

        logger.debug(f"_find_block2: returning {retval}")
        return retval

    def _find_block(self, key : KeyTuple, parent : Optional[InternalNode] = None) -> TreePath :
        """Returns the path to the leaf node where the key might be stored.
        Each entry in the list is a tuple (block_id, index).
        """

        retval : TreePath = []
        if parent is None :
            parent = self._read_root()
            retval += [tpi(0, _INVALID_INDEX)]
        logger.debug(f"Finding block for key = '{key}'")

        # Find which block the key may be in.
        # The block pointed to by index n has keys that
        # greater than or equal to the key at index n.
        # So we need to find the largest key that is still less than
        # the given key.
        # index=0 is for those keys that are strictly less than
        # the first key.
        i = bisect_left(parent.d, key, lo=1, key=lambda x : tuple(x[0]))
        logger.debug(f"raw i = {i}")
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
        logger.debug(f"final i = {i}")
        leaf_id = parent.d[i][1]

        retval += [tpi(leaf_id, i)]


        maybe_leaf = self._read_node(leaf_id)
        if maybe_leaf.k == _NODE_TYPE_INTERNAL :
            maybe_leaf = cast(InternalNode, maybe_leaf)
            return retval + self._find_block(key, maybe_leaf)

        logger.debug(f"_find_block returning {retval}")
        return retval


    def _pick_split_point(self, node : LeafNode | InternalNode) -> int :
        # Calculate where to split the block.
        # Prefer to split it down the middle, but if
        # there are multiple entries with the same key,
        # we need to make sure all the entries with the same key
        # are in the same block.
        split_point = _NODE_FANOUT//2
        split_key = tuple(node.d[split_point][0])

        # How many places to the left do we need to move
        # to get to the first entry with a different key.
        left_offset = 0
        while True:
            if split_point - left_offset < 0 :
                break
            if tuple(node.d[split_point - left_offset][0]) < split_key :
                left_offset -= 1
                break
            left_offset += 1

        # How many places to the right do we need to move
        # to get to the first entry with a different key.
        right_offset = 0
        while True:
            if split_point + right_offset >= len(node.d) :
                break
            if tuple(node.d[split_point + right_offset][0]) > split_key :
                break
            right_offset += 1

        logger.debug(f"left_offset = {left_offset}, right_offset = {right_offset}")

        # Which way is shorter?
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

        logger.debug(f"--- splitting {node.n} at parent {parent.n}, index {parent_index}")

        split_point = self._pick_split_point(node)
        logger.debug(f"split_point = {split_point}")

        left_data = node.d[:split_point]
        right_data = node.d[split_point:]
        logger.debug(f"left_data = {left_data}")
        logger.debug(f"right_data = {right_data}")

        self._write_node(node.n, _make_leaf(node.n, left_data))
        right_id = self.db_ctx.generate_id()
        self._write_node(right_id, _make_leaf(right_id, right_data))

        parent.d.insert(parent_index+1, (right_data[0][0], right_id))

        if len(parent.d) >= _NODE_FANOUT :
            self._split_internal(parent, parent_parent_index, tree_path[:-1])
        else :
            self._write_node(parent.n, parent)

    def _split_internal(self, node : InternalNode, parent_index : int, tree_path : TreePath) :
        """Split an internal node.
        recursively splits parents if necessary.
        """
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

        logger.debug(f"--- splitting {node.n} at parent {parent.n}, index {parent_index}")

        split_point = self._pick_split_point(node)
        logger.debug(f"split_point = {split_point}")

        left_data = node.d[:split_point]
        right_data = node.d[split_point:]
        logger.debug(f"left_data = {left_data}")
        logger.debug(f"right_data = {right_data}")

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

    def _print_tree(self, node_id : int, prefix : str) :
        node = self._read_node(node_id)
        print(f"{prefix}{node.n} {node.k} ({len(node.d)}):")
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


    class IndexIterator :
        def __init__(self, index : 'Index', key : Any = None, op : str | None = None) :
            # This assumes that parameter sanitizing has already been checked.
            self.index = index
            self.key = key
            self.bound_key = key
            self.op = op

            #List of tuples of block_id and current index
            self.scan_path : TreePath = []

            if op in [None, 'le', 'lt'] :
                logger.debug(f"__init__ : scan_path_for_start")
                self.scan_path_for_start()
                self.pyop = getattr(pyops, op) if op is not None else None

            elif op in ['ge', 'gt', 'eq'] :
                logger.debug(f"__init__ : scan_path_for_key")
                self.scan_path_for_key(lower_bound=(op != 'gt'))
                self.pyop = pyops.eq if op == 'eq' else None
            else :
                raise RuntimeError(f"Not sure what to do with operator {op}")

            logger.debug(f"__init__ : starting scan_path = {self.scan_path}")
            logger.debug(f"__init__ : pyops = {self.pyop.__name__ if self.pyop is not None else None}")

        def scan_path_for_start(self) :
            node = self.index._read_root()
            while node.k == _NODE_TYPE_INTERNAL :
                node = cast(InternalNode, node)
                self.scan_path.append(tpi(node.n, 0))
                node = self.index._read_node(node.d[0][1])
            # append the leaf
            self.scan_path.append(tpi(node.n, 0))

        def scan_path_for_key(self, lower_bound : bool = True) :
            self.bound_key = None
            self.scan_path = self.index._find_block2((False, self.key), lower_bound=lower_bound)


        def __iter__(self) :
            return self

        def __next__(self) -> str :
            '''Returns the row heap id.
            Assumes the key has already been skipped if it is not to be included.
            '''
            logger.debug(f"__next__: scan_path = {self.scan_path}")
            if len(self.scan_path) == 0 :
                logger.debug(f"__next__: scan_path is empty - Stopping")
                raise StopIteration
            item = self.scan_path.pop()
            node = self.index._read_node(item[0])
            if node.k == _NODE_TYPE_LEAF :
                node = cast(LeafNode, node)
                if item[1] >= len(node.d) :
                    return self.__next__()
                else :
                    if self.pyop is not None and not self.pyop(tuple(node.d[item[1]][0]),(False, self.key)) :
                        raise StopIteration
                    self.scan_path.append(tpi(node.n, item[1]+1))
                    return node.d[item[1]][1]
            else :
                node = cast(InternalNode, node)
                if item[1] >= len(node.d) - 1:
                    return self.__next__()
                else :
                    current_index = item[1]+1
                    self.scan_path.append(tpi(node.n, current_index))
                    node = self.index._read_node(node.d[current_index][1])
                    while node.k == _NODE_TYPE_INTERNAL :
                        node = cast(InternalNode, node)
                        self.scan_path.append(tpi(node.n, 0))
                        node = self.index._read_node(node.d[0][1])
                    # append the leaf
                    node = cast(LeafNode, node)
                    self.scan_path.append(tpi(node.n, 0))
                    if self.pyop is not None and not self.pyop(tuple(node.d[item[1]][0]),(False, self.key)) :
                        raise StopIteration

                    return node.d[0][1]

    #################################################################
    # Public API
    #################################################################

    @property
    def column(self) :
        return self._column

    def test_for_insert(self, record : _Row) -> Tuple[bool, str] :
        """Method to check if the record meets the index constraints.
        This must be called before insert() on the record.
        """
        if self.db_ctx.mode == "ro" :
            raise ValueError("Database is in read-only mode.")
        if self.closed :
            raise ValueError(f"Index {self.index_name} is closed.")

        raw_key = record[self._column]
        logger.debug(f"---- Testing key {raw_key} for index {self.index_name}")

        if not self.nullable :
            if raw_key is None :
                logger.debug(f"--- Null key in non-nullable index {self.index_name}")
                return False, f"Null key in non-nullable index {self.index_name}"

        if not self.unique :
            logger.debug(f"--- Non-unique index {self.index_name}")
            return (True, "")

        # check for duplicate key
        key = self._gen_key_tuple(raw_key)

        leaf_id, i = self._find_block(key)[-1]
        logger.debug(f"--- leaf_id = {leaf_id}, i = {i}")
        leaf = self._read_node(leaf_id)

        test = self._find_key_in_leaf(key, cast( LeafNode, leaf))
        logger.debug(f"--- test = {test}")

        if test[0] :
            logger.debug(f"--- Duplicate key '{raw_key}' in unique index {self.index_name}")
            return False, f"Duplicate key '{raw_key}' in unique index {self.index_name}"

        logger.debug("--- OK")
        return True, ""

    def insert(self, obj : _Row, heap_id : str) :
        """Insert object into index.
        test_for_insert() must be called first, otherwise constraints may be violated.
        """
        if self.db_ctx.mode == "ro" :
            raise ValueError("Database is in read-only mode.")
        if self.closed :
            raise ValueError(f"Index {self.index_name} is closed.")

        key : KeyTuple= self._gen_key_tuple(obj[self._column])

        # In theory, this is not needed since check_for_insert
        # should have been called. But its cheap, so why not.
        if not self.nullable and key[0] :
            raise ValueError(f"Null key in non-nullable index {self.index_name}")

        logger.debug(f"--- Inserting {key} into index {self.index_name}")

        tree_path = self._find_block(key)

        leaf_id, parent_index = tree_path[-1]

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

    def print_tree(self) :
        """Output a representation of the index B+-Tree onto stdout.
        """
        if self.closed :
            raise ValueError(f"Index {self.index_name} is closed.")

        print(f"=== {self.index_name} Tree:")
        self._print_tree(0, '')
        print("=== End of tree")

    def scan(self, key : Any = None, op : str | None = None) :
        if self.closed :
            raise ValueError(f"Index {self.index_name} is closed.")

        if op is not None and op not in OPERATOR_MAP :
            raise ValueError(f"Invalid operator {op}")

        mapped_op = OPERATOR_MAP[op] if op is not None else None

        if key is None and op is not None :
            raise ValueError("Cannot specify operator without key.")

        logger.debug(f"--- Scanning index {self.index_name}, key = {key}, op = {op}, mapped_op = {mapped_op}")

        for record in Index.IndexIterator(self, key, mapped_op) :
            yield record

    def close(self) :
        if self.closed :
            return
        # Let the table take care of deleting storage.

        self.db_ctx.cache.unregister(self.id)
        self.closed = True

    def delete(self, row : dict[Any, str]) :
        if self.db_ctx.mode == "ro" :
            raise ValueError("Database is in read-only mode.")
        if self.closed :
            raise ValueError(f"Index {self.index_name} is closed.")

        key = self._gen_key_tuple(row[self._column])
        tree_path = self._find_block2(key)

        leaf_id, leaf_index = tree_path[-1]

        if leaf_index == _INVALID_INDEX :
            raise ValueError(f"Key {key} not found in index {self.index_name}")

        leaf = self._read_node(leaf_id)
        if leaf.k == _NODE_TYPE_LEAF :
            leaf = cast(LeafNode, leaf)
        else :
            raise ValueError(f"Invalid node type {leaf.k} for leaf node {leaf_id}")
        del leaf.d[leaf_index]
        self._write_node(leaf_id, leaf)




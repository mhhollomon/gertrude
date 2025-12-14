from bisect import insort
import json
from pathlib import Path
from typing import Any, List, NamedTuple, Tuple, cast

import msgpack

from .globals import _generate_id, TYPES

_NODE_FANOUT = 6

class TreeNode(NamedTuple) :
    kind : int
    prev : str
    next : str
    data : List[Tuple[Any, str]]

_NODE_LEAF = 1
_NODE_INTERNAL = 2

class ScanPointers(NamedTuple) :
    start : str
    end : str


def _read_node(path : Path) -> TreeNode:
    with open(path, "rb") as f :
        return TreeNode(*(cast(list,msgpack.load(f))))
    
def _write_node(path : Path, node : TreeNode) :
    with open(path, "wb") as f :
        msgpack.dump(node, f)

def _create_node(path : Path, kind : int, prev : str, next : str, data : List[Tuple]) -> str :
    while True :
        new_id = _generate_id()
        proposed_path = path / new_id
        if not proposed_path.exists():
            break

    node = TreeNode(kind, prev, next, data)
    _write_node(proposed_path, node)

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
        root = self.path / "root"
        # we'll work about splitting the node later
        records = []
        for record in iterator() :
            (heap_id, data) = record
            key = getattr(data, self.column)
            records.append((key, heap_id))

        records.sort(key=lambda x : getattr(x, self.column))

        node = TreeNode(_NODE_LEAF, '', '', records) # 1 = leaf
        _write_node(root, node)

        scanptrs = ScanPointers('root', 'root')
        (self.path / "scan").write_text(json.dumps(scanptrs))
    
    def _insert(self, obj : Any, heap_id : str) :
        node = _read_node(self.path / "root")

        key = getattr(obj, self.column)
        insort(node.data, (key, heap_id), key=lambda x : x[0])

        if len(node.data) > _NODE_FANOUT :
            self._split(node, 'root')

        _write_node(self.path / "root", node)

    def _reserve_file_id(self) -> str :
        while True :
            new_id = _generate_id()
            proposed_path = self.path / new_id
            if not proposed_path.exists():
                break
        proposed_path.write_bytes(b'')
        return new_id

    def _update_scan_pointers(self, orig_id : str, new_start : str, new_end : str) :   
        need_to_write = False

        scanptrs = ScanPointers(**json.loads((self.path / "scan").read_text()))
        if scanptrs.start == orig_id :
            scanptrs = ScanPointers(new_start, scanptrs.end)
            need_to_write = True
        if scanptrs.end == orig_id :
            scanptrs = ScanPointers(scanptrs.start, new_end)
            need_to_write = True

        if need_to_write :
            (self.path / "scan").write_text(json.dumps(scanptrs))

    def _split(self, node : TreeNode, orig_id : str) :
        left_data = node.data[:_NODE_FANOUT//2]
        left_id = self._reserve_file_id()
        right_data = node.data[_NODE_FANOUT//2:]
        right_id = self._reserve_file_id()

        if (node.kind == _NODE_LEAF) :
            left_node = TreeNode(_NODE_LEAF, node.prev, right_id, left_data)
            right_node = TreeNode(_NODE_LEAF, right_id, node.next, right_data)

            self._update_scan_pointers(orig_id, left_id, right_id)
        else :
            left_node = TreeNode(_NODE_INTERNAL, '', '', left_data)
            right_node = TreeNode(_NODE_INTERNAL, '', '', right_data)

from dataclasses import dataclass
from typing import Any, List, Tuple


type KeyTuple = Tuple[bool, Any]

# Used by the block list
type DataList = List[Tuple[KeyTuple, int]]
# use by leaf nodes
type LeafData = List[Tuple[KeyTuple, int]]

@dataclass
class IndexNode :
    k : str        # node type
    n : int        # node id

@dataclass
class LeafNode(IndexNode) :
    d : LeafData

INDEX_NODE_TYPE_LEAF = 'L'

@dataclass
class InternalNode(IndexNode) :
    d : DataList

INDEX_NODE_TYPE_INTERNAL = 'I'

def make_leaf(node_id : int, d : LeafData) :
    return LeafNode(INDEX_NODE_TYPE_LEAF, node_id, d)

def make_internal(node_id : int, d : DataList) :
    return InternalNode(INDEX_NODE_TYPE_INTERNAL, node_id, d)

from dataclasses import dataclass
from typing import List, NamedTuple

from .value import Value

# Yes, these look similar. But I think it will make
# the code easier to reason about if we are clear on
# the difference between a heap_id (lives in the table data)
# and a node_id (lives in the index data).
@dataclass(frozen=True)
class LeafItem :
    key : Value
    heap_id : int

@dataclass(frozen=True)
class InternalItem :
    key : Value
    node_id : int

# Used by the InternalNode
type InternalData = List[InternalItem]

# use by leaf nodes
type LeafData = List[LeafItem]

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
    d : InternalData

INDEX_NODE_TYPE_INTERNAL = 'I'

def make_leaf(node_id : int, d : LeafData) :
    return LeafNode(INDEX_NODE_TYPE_LEAF, node_id, d)

def make_internal(node_id : int, d : InternalData) :
    return InternalNode(INDEX_NODE_TYPE_INTERNAL, node_id, d)

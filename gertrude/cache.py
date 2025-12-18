from dataclasses import dataclass
from pathlib import Path
from typing import OrderedDict, Tuple

type CacheKey = Tuple[int, int]

@dataclass
class CacheStats:
    hits : int
    misses : int
    evictions : int
    blocks : int

class LRUCache :
    """
    A simple LRU cache implementation.
    Use OrderedDict to keep track of the most recently used items.
    """
    def __init__(self, max_size : int) :
        self.max_size = max_size
        self.cache : OrderedDict[CacheKey, bytes] = OrderedDict()
        self.paths : dict[int, Path] = {}
        self.stats = CacheStats(0, 0, 0, 0)

    def register(self, key, path : Path) :
        self.paths[key] = path

    def get(self, index : int, block_id : int) -> bytes:
        if index not in self.paths :
            raise Exception(f"Index {index} not registered")

        if (index, block_id) in self.cache :
            self.stats.hits += 1
            self.cache.move_to_end((index, block_id))
            return self.cache[(index, block_id)]

        self.stats.misses += 1
        with open(self.paths[index] / f"{block_id:03}", "rb") as f :
            data = f.read()
            self.cache[(index, block_id)] = data
            if len(self.cache) > self.max_size :
                self.stats.evictions += 1
                self.cache.popitem(last=False)
            self.stats.blocks = len(self.cache)
            return data

    def lookup(self, index : int, block_id : int) -> bytes | None :
        if (index, block_id) in self.cache :
            self.stats.hits += 1
            self.cache.move_to_end((index, block_id))
            return self.cache[(index, block_id)]

        return None

    def put(self, index : int, block_id : int, data : bytes, cache : bool = True) :
        if index not in self.paths :
            raise Exception(f"Index {index} not registered")

        if cache :
            if (index, block_id) in self.cache :
                self.stats.hits += 1
                self.cache.move_to_end((index, block_id))

            if cache :
                self.cache[(index, block_id)] = data
                if len(self.cache) > self.max_size :
                    self.stats.evictions += 1
                    self.cache.popitem(last=False)
        elif (index, block_id) in self.cache :
            del self.cache[(index, block_id)]

        self.stats.blocks = len(self.cache)

        with open(self.paths[index] / f"{block_id:03}", "wb") as f :
            f.write(data)

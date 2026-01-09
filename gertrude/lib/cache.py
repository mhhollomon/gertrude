from dataclasses import dataclass
from pathlib import Path
from typing import OrderedDict, Tuple, cast
from .types.value import Value

import msgpack

type CacheKey = Tuple[int, int]

def custom_pack(obj) :
    if isinstance(obj, Value) :
        return msgpack.ExtType(1, obj.raw)
    return obj

def ext_hook(code, data) :
    if code == 1 :
        return Value.from_raw(data)
    return msgpack.ExtType(code, data)

@dataclass
class CacheStats:
    hits : int = 0
    misses : int = 0
    evictions : int = 0
    blocks : int = 0
    size : int = 0
    gets : int = 0
    puts : int = 0
    indexes : int = 0

class LRUCache :
    """
    A simple LRU cache implementation.
    Use OrderedDict to keep track of the most recently used items.
    """
    def __init__(self, max_size : int) :
        self.max_size = max_size
        self.cache : OrderedDict[CacheKey, dict] = OrderedDict()
        self.paths : dict[int, Path] = {}
        self._stats = CacheStats(size = max_size)

    def register(self, key, path : Path) :
        self.paths[key] = path

    def unregister(self, key) :
        del self.paths[key]

        dead = [k for k in self.cache if k[0] == key]
        for k in dead :
            del self.cache[k]

    @property
    def stats(self) :
        self._stats.blocks = len(self.cache)
        self._stats.indexes = len(self.paths)
        # return a copy.
        return CacheStats(**self._stats.__dict__)

    def get(self, index : int, block_id : int) -> dict:
        if index not in self.paths :
            raise Exception(f"Index {index} not registered")

        self._stats.gets += 1

        if (index, block_id) in self.cache :
            self._stats.hits += 1
            self.cache.move_to_end((index, block_id))
            return self.cache[(index, block_id)]

        self._stats.misses += 1
        with open(self.paths[index] / f"{block_id:03}", "rb") as f :
            data = msgpack.unpackb(f.read(), ext_hook=ext_hook)
            self.cache[(index, block_id)] = data
            if len(self.cache) > self.max_size :
                self._stats.evictions += 1
                self.cache.popitem(last=False)
            return data

    def lookup(self, index : int, block_id : int) -> dict | None :
        if index not in self.paths :
            raise Exception(f"Index {index} not registered")

        self._stats.gets += 1

        if (index, block_id) in self.cache :
            self._stats.hits += 1
            self.cache.move_to_end((index, block_id))
            return self.cache[(index, block_id)]

        self._stats.misses += 1
        return None

    def put(self, index : int, block_id : int, data : dict, cache : bool = True) :
        if index not in self.paths :
            raise Exception(f"Index {index} not registered")

        self._stats.puts += 1

        if cache :
            if (index, block_id) in self.cache :
                self._stats.hits += 1
                self.cache.move_to_end((index, block_id))

            self.cache[(index, block_id)] = data
            if len(self.cache) > self.max_size :
                self._stats.evictions += 1
                self.cache.popitem(last=False)

        elif (index, block_id) in self.cache :
            del self.cache[(index, block_id)]

        with open(self.paths[index] / f"{block_id:03}", "wb") as f :
            f.write(cast(bytes, msgpack.dumps(data, default=custom_pack, use_bin_type=True)))

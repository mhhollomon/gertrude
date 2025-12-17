import msgpack
from pathlib import Path

class IntegerIdGenerator:
    SaveInterval : int = 10
    def __init__(self, cache_path : Path) :
        self.id = 0
        self.cache_path = cache_path
        self.on_first = True
        if self.cache_path.exists() :
            self.id = (msgpack.unpackb(self.cache_path.read_bytes()))['id']
            self.id += 2*self.SaveInterval
        self.count = 0

    def gen_id(self) -> int :
        self.count += 1
        self.id += 1
        if self.count == self.SaveInterval or self.on_first :
            self.count = 0
            with self.cache_path.open('wb') as f :
                msgpack.dump({'id' : self.id}, f)
            self.on_first = False
        return self.id

    def close(self) :
        with self.cache_path.open('wb') as f :
            msgpack.dump({'id' : self.id}, f)
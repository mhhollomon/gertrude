from pathlib import Path

from nanoid import generate

HEAP_ID_ALPHABET = '123456789ABCDEF'
HEAP_ID_LENGTH = 16

class HeapID() :
    __slots__ = ('id')

    def __init__(self, id : str | bytes | int) :
        if isinstance(id, str) :
            self.id = int(id, base=16)
        elif isinstance(id, bytes) :
            self.id = int.from_bytes(id, 'big')
        elif isinstance(id, int) :
            self.id = id
        else :
            raise TypeError


    def __str__(self) :
        return f"{self.id:016X}"

    def __int__(self) :
        return self.id

    def __bytes__(self) :
        return self.id.to_bytes(8, 'big')

    def __eq__(self, other) :
        return self.id == other.id

    def __hash__(self) :
        return self.id

    def __repr__(self) :
        return f"heap_id({self.id})"

    def to_path(self) -> Path :
        s = self.__str__()
        return Path(s[0:2]) / s[2:4] / s[4:]

    @classmethod
    def from_path(cls, path : Path) :
        return cls(path.parent.parent.name + path.parent.name + path.name)

    @classmethod
    def generate(cls) :
        return cls(generate(alphabet=HEAP_ID_ALPHABET, size=HEAP_ID_LENGTH))

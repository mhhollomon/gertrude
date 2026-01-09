from typing import Any, Self, Type
import struct

VALUE_INT_TYPE = 1
VALUE_STR_TYPE = 2
VALUE_FLOAT_TYPE = 3
VALUE_BOOL_TYPE = 4

TYPE_MAP = {
    int : VALUE_INT_TYPE,
    str : VALUE_STR_TYPE,
    float : VALUE_FLOAT_TYPE,
    bool : VALUE_BOOL_TYPE,

    'int' : VALUE_INT_TYPE,
    'str' : VALUE_STR_TYPE,
    'float' : VALUE_FLOAT_TYPE,
    'bool' : VALUE_BOOL_TYPE
}

TYPE_NAME = [
    "INVALID",
    "int",
    "str",
    "float",
    "bool"
]

_HEADER_FLAG = 0b11000000
_TYPE_MASK   = 0b00111100
#_UNUSED_MASK = 0b00000010 # Future use (maybe)
_NULL_MASK   = 0b00000001 # actually "NOT null"

def type_const(type : Type | str) -> int :
    return TYPE_MAP[type]


class Value:
    __slots__ = ('raw_', 'value_')

    def __init__(self, value_type : int | str | Type, value : Any):
        if isinstance(value_type, (str, Type)) :
            type = TYPE_MAP.get(value_type, -1)
        else :
            type = value_type

        if not self._valid_type(type) :
            raise ValueError(f"Invalid value type {value_type}")

        header_byte = _HEADER_FLAG | (type << 2) | int(value is not None)

        self.raw_ = header_byte.to_bytes(1, "big") + self._encode_value(type, value)
        self.value_ = value


    def _valid_type(self, type : int) -> bool:
        return type <=4 and type >= 1

    def _encode_value(self, type : int, value : Any) -> bytes :
        if value is None :
            return b""
        if type == VALUE_INT_TYPE :
            return struct.pack(">q",int(value))
        elif type == VALUE_STR_TYPE :
            v = str(value).encode("utf-8")
            if len(v) < 128 :
                header = len(v).to_bytes(1, "big")
            elif len(v) < 32767 :
                header = len(v).to_bytes(2, "big")
                header = (header[0] | 0b10000000).to_bytes(1, "big") + header[1:]
            else :
                raise ValueError("String too long")
            return header + v
        elif type == VALUE_FLOAT_TYPE :
            return struct.pack(">d", float(value))
        elif type == VALUE_BOOL_TYPE :
            return struct.pack(">?", bool(value))
        else :
            raise ValueError(f"Invalid value type {type}")

    def _decode_value(self) -> Any :
        if self.type == VALUE_INT_TYPE :
            return struct.unpack(">q", self.raw_[1:])[0]
        elif self.type == VALUE_STR_TYPE :
            if self.raw_[1] & 0b10000000 :
                return self.raw_[3:].decode("utf-8")
            else :
                return self.raw_[2:].decode("utf-8")
        elif self.type == VALUE_FLOAT_TYPE :
            return struct.unpack(">d", self.raw_[1:])[0]
        elif self.type == VALUE_BOOL_TYPE :
            return struct.unpack(">?", self.raw_[1:])[0]
        else :
            raise ValueError(f"Invalid value type {self.type}")

    ##############################################################
    # DUNDER METHODS
    ##############################################################

    #
    # Conversion
    #
    def __bytes__(self) :
        return self.raw_

    def __str__(self) :
        return self.as_str()

    def __int__(self) :
        return self.as_int()

    def __float__(self) :
        return self.as_float()

    def __bool__(self) :
        return self.as_bool()

    def __repr__(self) :
        return f"Value(type={self.type}, value={self.value})"

    #
    # COMPARISON
    #
    def __lt__(self, other) :
        retval = False
        if isinstance(other, Value) :
            if self.type != other.type :
                raise ValueError("Cannot compare Values of different types")
            if self.type == VALUE_STR_TYPE :
                # Only need to check this in strings because for
                # the other types we can just compare the complete raw.
                if self.is_null :
                    return other.is_null
                str_bytes = self.raw_[3:] if self.raw_[1] & 0b10000000 else self.raw_[2:]
                other_str_bytes = other.raw_[3:] if other.raw_[1] & 0b10000000 else other.raw_[2:]
                retval = str_bytes < other_str_bytes
            else :
                retval = self.raw_ < other.raw_
        elif isinstance(other, bytes) :
            retval = self.raw_ < other
        return retval


    def __eq__(self, other) :
        if isinstance(other, Value) :
            return self.raw_ == other.raw_
        elif isinstance(other, bytes) :
            return self.raw_ == other
        else :
            return False

    def __gt__(self, other) :
        retval = False
        if isinstance(other, Value) :
            if self.type != other.type :
                raise ValueError("Cannot compare Values of different types")
            if self.type == VALUE_STR_TYPE :
                # Only need to check this in strings because for
                # the other types we can just compare the complete raw.
                if self.is_null :
                    return other.is_null
                str_bytes = self.raw_[3:] if self.raw_[1] & 0b10000000 else self.raw_[2:]
                other_str_bytes = other.raw_[3:] if other.raw_[1] & 0b10000000 else other.raw_[2:]
                retval = str_bytes > other_str_bytes
            else :
                retval = self.raw_ > other.raw_
        elif isinstance(other, bytes) :
            retval = self.raw_ > other
        return retval

    def __le__(self, other) :
        return self < other or self == other

    def __ge__(self, other) :
        return self > other or self == other

    def __ne__(self, other) :
        return not self == other

    #
    # MATH
    #
    def __add__(self, other) :
        if self.is_null or other.is_null :
            return Value(self.type, None)
        sum = self.value + other.value
        return Value(type(sum), sum)

    def __sub__(self, other) :
        if self.is_null or other.is_null :
            return Value(self.type, None)
        diff = self.value - other.value
        return Value(type(diff), diff)

    def __mul__(self, other) :
        if self.is_null or other.is_null :
            return Value(self.type, None)
        product = self.value * other.value
        return Value(type(product), product)

    def __truediv__(self, other) :
        if self.is_null or other.is_null :
            return Value(self.type, None)
        quotient = self.value / other.value
        return Value(type(quotient), quotient)

    def __mod__(self, other) :
        if self.is_null or other.is_null :
            return Value(self.type, None)
        mod = self.value % other.value
        return Value(type(mod), mod)

    def __not__(self) :
        if self.is_null :
            return Value(self.type, None)
        return Value(self.type, not self.value)

    ##############################################################
    # PUBLIC API
    ##############################################################

    @classmethod
    def from_raw(cls, raw : bytes) -> Self:
        obj = cls(VALUE_INT_TYPE, None)
        obj.raw_ = raw
        return obj

    def clone(self) -> Self:
        return self.from_raw(self.raw_)

    @property
    def type(self) -> int:
        return (self.raw_[0] & _TYPE_MASK) >> 2

    @property
    def type_name(self) -> str :
        return TYPE_NAME[self.type]

    @property
    def is_null(self) -> bool:
        return not bool(self.raw_[0] & _NULL_MASK)

    @property
    def value(self) -> Any :
        if self.is_null :
            return None
        if self.value_ is None :
            self.value_ = self._decode_value()
        return self.value_

    @property
    def raw(self) -> bytes:
        return self.raw_

    def as_int(self) -> int :
        return int(self.value)

    def as_str(self) -> str :
        return str(self.value)

    def as_float(self) -> float :
        return float(self.value)

    def as_bool(self) -> bool :
        return bool(self.value)


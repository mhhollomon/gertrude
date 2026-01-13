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

#
# HEADER Bits
#
# Marker bit (not really needed)
_HEADER_FLAG  = 0b10000000
# Is the value encoded or just the header itself.
_ENCODED_MASK = 0b01000000
# The value type
_TYPE_MASK    = 0b00111000
_TYPE_SHIFT   = 3
# The null indicator - actually "NOT null"
_NULL_MASK    = 0b00000100
_NULL_SHIFT   = 2
# Reserved
#_UNUSED_MASK = 0b00000011

def type_const(type : Type | str) -> int :
    return TYPE_MAP[type]


class Value:
    """Class to store a possibly None (Null) value of any type.

    This is designed to be an immutable object.

    The first byte contains both a type indicator and a null indicator.
    The remaining bytes contain the actual value (if any).

    Thus, a null value only has a single byte - the header byte.

    *Note* that string values do not encode the length of the string.
    This class is never written "by itself" to a file.
    Instead msgpack is used to actually serialize it.

    Not having the length of the string makes the comparision of values do-able
    without having to decode the value.
    """

    # Both slots are lazy.
    # If the object is created through the init function, then value_ will
    # be filled, but only the header byte will be encoded into raw_.
    # If the object is created through the from_raw() function, then value_
    # will not be filled, but raw_ will be.
    #
    __slots__ = ('raw_', 'value_')

    def __init__(self, value_type : int | str | Type, value : Any):
        if isinstance(value_type, (str, Type)) :
            type = TYPE_MAP.get(value_type, -1)
        else :
            type = value_type

        if not self._valid_type(type) :
            raise ValueError(f"Invalid value type {value_type}")

        # Just encode the header for now
        self.raw_ = (_HEADER_FLAG | (type << _TYPE_SHIFT) | (int(value is not None) << _NULL_SHIFT)).to_bytes(1, "big")
        self.value_ = value

    def _valid_type(self, type : int) -> bool:
        return type <=4 and type >= 1

    @property
    def is_encoded(self) -> bool:
        return bool(self.raw_[0] & _ENCODED_MASK)

    def _encode_value(self, type : int, value : Any) -> bytes :
        if value is None :
            return b""
        if type == VALUE_INT_TYPE :
            return struct.pack(">q",int(value))
        elif type == VALUE_STR_TYPE :
            return str(value).encode("utf-8")
        elif type == VALUE_FLOAT_TYPE :
            return struct.pack(">d", float(value))
        elif type == VALUE_BOOL_TYPE :
            return struct.pack(">?", bool(value))
        else :
            raise ValueError(f"Invalid value type {type}")

    def _decode_value(self) -> Any :
        if not self.is_encoded :
            raise ValueError("Value is not encoded")
        if self.type == VALUE_INT_TYPE :
            return struct.unpack(">q", self.raw_[1:])[0]
        elif self.type == VALUE_STR_TYPE :
            return self.raw_[1:].decode("utf-8")
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
        return self.raw

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

    def __hash__(self) :
        return hash(self.raw)

    #
    # COMPARISON
    #
    def __lt__(self, other) :
        retval = False
        if isinstance(other, Value) :
            if self.type != other.type :
                raise ValueError("Cannot compare Values of different types")
            retval = self.raw < other.raw
        elif isinstance(other, bytes) :
            retval = self.raw < other
        return retval


    def __eq__(self, other) :
        if isinstance(other, Value) :
            return self.raw == other.raw
        elif isinstance(other, bytes) :
            return self.raw == other
        else :
            return False

    def __gt__(self, other) :
        retval = False
        if isinstance(other, Value) :
            if self.type != other.type :
                raise ValueError("Cannot compare Values of different types")
            retval = self.raw > other.raw
        elif isinstance(other, bytes) :
            retval = self.raw > other
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
        return self.from_raw(self.raw)

    @property
    def type(self) -> int:
        return (self.raw_[0] & _TYPE_MASK) >> _TYPE_SHIFT

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
        if not self.is_encoded :
            self.raw_ = (self.raw_[0] | _ENCODED_MASK).to_bytes(1, "big")
            self.raw_ += self._encode_value(self.type, self.value)
        return self.raw_

    def as_int(self) -> int :
        return int(self.value)

    def as_str(self) -> str :
        return str(self.value)

    def as_float(self) -> float :
        return float(self.value)

    def as_bool(self) -> bool :
        return bool(self.value)


##################################################
## Helpers
##################################################
def valueTrue() -> Value :
    return Value(VALUE_BOOL_TYPE, True)

def valueFalse() -> Value :
    return Value(VALUE_BOOL_TYPE, False)

def valueNull() -> Value :
    return Value(VALUE_INT_TYPE, None)

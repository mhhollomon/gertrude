from gertrude.lib.value import *
import pytest


def test_int_value() :
    v = Value(VALUE_INT_TYPE, 10000034)
    assert v.type == VALUE_INT_TYPE
    assert v.value == 10000034
    assert v.is_null == False

    v = Value(VALUE_INT_TYPE, None)
    assert v.type == VALUE_INT_TYPE
    assert v.value == None
    assert v.is_null == True

    with pytest.raises(ValueError) :
        Value(5, 1)

def test_str_value() :
    v = Value(VALUE_STR_TYPE, "hello")
    assert v.type == VALUE_STR_TYPE
    assert v.value == "hello"
    assert v.is_null == False

    v = Value(VALUE_STR_TYPE, None)
    assert v.type == VALUE_STR_TYPE
    assert v.value == None
    assert v.is_null == True

    long_str = "hello" * 1000
    v = Value(VALUE_STR_TYPE, long_str)
    assert v.type == VALUE_STR_TYPE
    assert v.value == long_str
    assert v.is_null == False


def test_bool_value() :
    v = Value(VALUE_BOOL_TYPE, True)
    assert v.type == VALUE_BOOL_TYPE
    assert v.value == True
    assert v.is_null == False

    v = Value(VALUE_BOOL_TYPE, None)
    assert v.type == VALUE_BOOL_TYPE
    assert v.value == None
    assert v.is_null == True

def test_float_value() :
    v = Value(VALUE_FLOAT_TYPE, 12.34)
    assert v.type == VALUE_FLOAT_TYPE
    assert v.value == 12.34
    assert v.is_null == False

    v = Value(VALUE_FLOAT_TYPE, None)
    assert v.type == VALUE_FLOAT_TYPE
    assert v.value == None
    assert v.is_null == True

def test_from_raw() :
    v = Value.from_raw(b"\xc4")
    assert v.type == VALUE_INT_TYPE
    assert v.value == None
    assert v.is_null == True

    temp = Value(VALUE_FLOAT_TYPE, 12.34)

    v = Value.from_raw(temp.raw_)
    assert v.type == VALUE_FLOAT_TYPE
    assert v.value == 12.34
    assert v.is_null == False
from .types.value import Value
import msgpack

def _custom_pack(obj) :
    if isinstance(obj, Value) :
        return msgpack.ExtType(1, obj.raw)
    return obj

def _ext_hook(code, data) :
    if code == 1 :
        return Value.from_raw(data)
    return msgpack.ExtType(code, data)


def pack(data) :
    return msgpack.packb(data, default=_custom_pack, use_bin_type=True)

def packf(data, file) :
    msgpack.dump(data, file, default=_custom_pack, use_bin_type=True)

def unpack(data) :
    return msgpack.unpackb(data, ext_hook=_ext_hook)

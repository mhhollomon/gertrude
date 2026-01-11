from typing import cast
from .types.value import Value
from .types.index import InternalItem, LeafItem
import msgpack
import logging
logger = logging.getLogger(__name__)

def _custom_pack(obj) :
    logger.debug(f"custom_pack {type(obj)}")
    if isinstance(obj, Value) :
        return msgpack.ExtType(1, obj.raw)
    elif isinstance(obj, LeafItem) :
        return msgpack.ExtType(2, pack([obj.key, obj.heap_id]))
    elif isinstance(obj, InternalItem) :
        return msgpack.ExtType(3, pack([obj.key, obj.node_id]))
    return obj

def _ext_hook(code, data) :
    logger.debug(f"ext_hook {code} {data}")
    if code == 1 :
        return Value.from_raw(data)
    elif code == 2 :
        return LeafItem(*unpack(data))
    elif code == 3 :
        return InternalItem(*unpack(data))
    return msgpack.ExtType(code, data)


def pack(data) -> bytes :
    return cast(bytes, msgpack.packb(data, default=_custom_pack, use_bin_type=True))

def packf(data, file) :
    msgpack.dump(data, file, default=_custom_pack, use_bin_type=True)

def unpack(data) :
    return msgpack.unpackb(data, ext_hook=_ext_hook)

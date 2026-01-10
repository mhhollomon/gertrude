from unittest.mock import Mock, patch
from gertrude.lib.types.heap_id import HeapID, HEAP_ID_LENGTH
from gertrude.lib.heap import (
    write,
    delete,
    )

import msgpack


def test_id() :
    assert HEAP_ID_LENGTH >= 16
    assert len(str(HeapID.generate())) == HEAP_ID_LENGTH

def test_save_to_heap(tmp_path) :
    db_path = tmp_path / "db"
    db_path.mkdir()
    heap_id = write(db_path, {"key" : "value"})

    created_file = db_path / heap_id.to_path()
    assert created_file.exists()
    data = msgpack.unpackb(created_file.read_bytes())
    assert data == {"key": "value"}

def test_collision(tmp_path) :
    db_path = tmp_path / "db"
    db_path.mkdir()

    retvals = (HeapID("F12391AB4DCD93AC"), HeapID("F12391AB4DCD93AC"), HeapID("9992DFBCABD12345"))
    import gertrude.lib.types.heap_id
    with patch("gertrude.lib.types.heap_id.HeapID.generate") as gen_id_mock :
        gen_id_mock.side_effect = retvals
        first = write(db_path, {"key" : "value"})
        second = write(db_path, {"key" : "value"})
        assert str(first) == "F12391AB4DCD93AC"
        assert str(second) == "9992DFBCABD12345"

        assert gen_id_mock.call_count == 3

def test_delete(tmp_path) :
    db_path = tmp_path / "db"
    db_path.mkdir()

    heap_id = write(db_path, {"key" : "value"})
    data = delete(db_path, heap_id)

    assert data == {"key" : "value"}

    deleted_file = db_path / heap_id.to_path()
    assert not deleted_file.exists()

    assert delete(db_path, heap_id) is None
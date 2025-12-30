from unittest.mock import Mock, patch
from gertrude.lib.heap import (
    write,
    delete,
    HEAP_ID_LENGTH, generate_heap_id
    )

import msgpack


def test_id() :
    assert HEAP_ID_LENGTH >= 20
    assert len(generate_heap_id()) == HEAP_ID_LENGTH

def test_save_to_heap(tmp_path) :
    db_path = tmp_path / "db"
    db_path.mkdir()
    heap_id = write(db_path, {"key" : "value"})

    created_file = db_path / heap_id[0:2] / heap_id[2:4] / heap_id[4:]
    assert created_file.exists()
    data = msgpack.unpackb(created_file.read_bytes())
    assert data == {"key": "value"}

def test_collision(tmp_path) :
    db_path = tmp_path / "db"
    db_path.mkdir()

    retvals = ("M3IJW1290DEV2APF", "M3IJW1290DEV2APF", "9JI7BB6HW6253D12")
    gen_id_mock = Mock(side_effect=retvals)
    with patch("gertrude.lib.heap._generate_heap_id", gen_id_mock) :
        first = write(db_path, {"key" : "value"})
        second = write(db_path, {"key" : "value"})
        assert first == "M3IJW1290DEV2APF"
        assert second == "9JI7BB6HW6253D12"

    assert gen_id_mock.call_count == 3

def test_delete(tmp_path) :
    db_path = tmp_path / "db"
    db_path.mkdir()

    heap_id = write(db_path, {"key" : "value"})
    data = delete(db_path, heap_id)

    assert data == {"key" : "value"}

    deleted_file = db_path / heap_id[0:2] / heap_id[2:4] / heap_id[4:]
    assert not deleted_file.exists()

    assert delete(db_path, heap_id) is None
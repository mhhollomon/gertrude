from typing import Any
from nanoid import generate
from pathlib import Path
import json
import regex as re

GERTRUDE_VERSION = "0.0.1"
CURRENT_SCHEMA_VERSION = 1

HEAP_ID_ALPHABET = '123456789ABCDEFGHIJKLMNPQRSTUVWXYZ'
HEAP_ID_LENGTH = 20

NAME_REGEX = re.compile(r'^[a-zA-Z0-9_-]+$')

def _generate_id():
    return generate(alphabet=HEAP_ID_ALPHABET, size=HEAP_ID_LENGTH)

def _save_to_heap(heap : Path, value : Any) -> str :
    """Saves to the heap pointed to by the path.
    Checks for path collisions.
    Returns the hash_id.
    """
    while True :
        hash_id = _generate_id()
        proposed_path = heap / hash_id[0:2] / hash_id[2: 4] / hash_id[4:]
        if not proposed_path.exists():
            break

    proposed_path.parent.mkdir(parents=True, exist_ok=True)
    proposed_path.write_text(json.dumps(value))
    
    return hash_id

def _delete_from_heap(heap : Path, hash_id : str) -> Any :
    """ Note that the hash_id is not validated nor are any 
    empty directories removed.
    """
    heap_path = heap / hash_id[0:2] / hash_id[2: 4] / hash_id[4:]

    if not heap_path.exists():
        return None

    retval = json.loads(heap_path.read_text())    
    heap_path.unlink()

    return retval

class Database :
    def __init__(self, db_path : Path, comment : str = '') :
        self.db_path = db_path

        need_setup = True
        if not self.db_path.exists() :
            self.db_path.mkdir()
        else :
            assert self.db_path.is_dir()
            conf_file = self.db_path / "gertrude.conf"
            if not conf_file.exists() :
                self._clear()
            else :
                config = json.loads((self.db_path / "gertrude.conf").read_text())
                assert config["schema_version"] == CURRENT_SCHEMA_VERSION    
                assert config["gertrude_version"] == GERTRUDE_VERSION
                need_setup = False

        if need_setup :
            self._setup(comment)

    #################################################################
    # Internal utilities
    #################################################################
    def _setup(self, comment : str) :
            config = {
                "schema_version" : CURRENT_SCHEMA_VERSION,
                "gertrude_version" : GERTRUDE_VERSION,
                "comment" : comment,
            }
            (self.db_path / "gertrude.conf").write_text(json.dumps(config))
            (self.db_path / "tables").mkdir(exist_ok=True)
    def _clear(self) :
        self.db_path.rmdir()
        self.db_path.mkdir()
import regex as re
from nanoid import generate

GERTRUDE_VERSION = "0.0.1"
CURRENT_SCHEMA_VERSION = 1

HEAP_ID_ALPHABET = '123456789ABCDEFGHIJKLMNPQRSTUVWXYZ'
HEAP_ID_LENGTH = 20

NAME_REGEX = re.compile(r'^[a-zA-Z0-9_-]+$')

def _generate_id():
    return generate(alphabet=HEAP_ID_ALPHABET, size=HEAP_ID_LENGTH)

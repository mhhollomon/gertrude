"""This is Gertrude.

Gertrude is a python database using the file system as storage.
"""

from .globals import GERTRUDE_VERSION
__version__ = GERTRUDE_VERSION

from .database import Database
from .table import FieldSpec, cspec
from .util import asc, desc

__all__ = ["Database", "FieldSpec", "cspec", "asc", "desc"]
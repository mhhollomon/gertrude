"""This is Gertrude.

Gertrude is a python database using the file system as storage.
"""

from .globals import GERTRUDE_VERSION
__version__ = GERTRUDE_VERSION

from .database import Database
from .table import FieldSpec, cspec

__all__ = ["Database", "FieldSpec", "cspec"]
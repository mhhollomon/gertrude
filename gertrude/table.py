
from pathlib import Path
from typing import Dict, Iterable, NamedTuple, Any, cast
import json
import msgpack

from gertrude.globals import NAME_REGEX


FieldSpec = NamedTuple("FieldSpec", [("name", str), ("type", str), ("options", dict[str, Any])])

_TYPES = {
    "str" : str,
    "int" : int,
    "float" : float,
    "bool" : bool,
}


class Table :
    def __init__(self,
                    parent : Any,
                    db_path : Path,
                    table_name : str,
                    spec : Iterable[FieldSpec]) :

        from gertrude.database import Database
        assert isinstance(parent, Database)

        self.db_path = db_path
        self.index : Dict[str, Table._index] = {}
        self.spec = spec
        self.name = table_name
        self.parent = cast(Database, parent)
        self.open = True

    def _drop(self) :
        if not self.open or self.parent is None :
            return
        import shutil
        shutil.rmtree(self.db_path)
        self.open = False
        self.parent = None

    def _generate_tuple_type(self) :
        tuple_types = []
        for s in self.spec :
            if s.type not in _TYPES :
                raise ValueError(f"Invalid type {s.type} for field {s.name}")
            real_type = _TYPES[s.type]
            tuple_types.append((s.name, real_type))

        return NamedTuple(self.name, tuple_types)

    def _create_pk(self) :
        pk = [x for x in self.spec if x.options.get("pk", False)]
        if len(pk) > 1 :
            raise ValueError(f"Table {self.name} has multiple primary keys.")
        elif len(pk) == 1 :
            pk = pk[0]
        else :
            return

        self.add_index("pk_" + pk.name, pk.name)

    def _create(self) :
        if self.db_path.exists() :
            raise ValueError(f"Table {self.name} directory already exists.")

        # good to go
        self.db_path.mkdir(exist_ok=True)
        (self.db_path / "config").write_text(json.dumps(self.spec))
        (self.db_path / "data").mkdir()
        (self.db_path / "index").mkdir()

        self.record = self._generate_tuple_type()
        self._create_pk()

    def _load_def(self) :
        self.spec = json.loads((self.db_path / "config").read_text())

        self.record = self._generate_tuple_type()

    #################################################################
    # _INDEX CLASS
    #################################################################
    class _index :
        def __init__(self, index_name : str, path : Path, column : str, coltype : str) :
            self.index_name = index_name
            self.column = column
            self.coltype = coltype
            self.path = path
            self.real_type = _TYPES[coltype]
            self.record = NamedTuple(index_name, [('key', self.real_type), ('value', str)])

        def _create(self) :
            if self.path.exists() :
                raise ValueError(f"Index {self.index_name} directory already exists.")

            self.path.mkdir()
            root = self.path / "root"
            data = (1, ()) # 1 = leaf
            with open(root, "wb") as f :
                msgpack.dump(data, f)

    #################################################################
    # Public API
    #################################################################
    def add_index(self, index_name : str, column : str) :
        if not NAME_REGEX.match(index_name) :
            raise ValueError(f"Invalid index name {index_name} for table {self.name}")

        col = [x for x in self.spec if x.name == column]
        if len(col) != 1 :
            raise ValueError(f"Invalid column name {column} for table {self.name}")

        new_index = Table._index(index_name, self.db_path / "index" / index_name, column, col[0].type)
        self.index[index_name] = new_index

        new_index._create()

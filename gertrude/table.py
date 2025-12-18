
from pathlib import Path
from typing import Dict, Iterable, NamedTuple, Any, cast
import json
import msgpack

from .globals import NAME_REGEX, DBContext, _save_to_heap, TYPES

from .index import Index


FieldSpec = NamedTuple("FieldSpec", [("name", str), ("type", str), ("options", dict[str, Any])])

OPT_DEFAULT = {
    "pk" : False,
    "unique" : False,
    "nullable" : True,
}


class Table :
    def __init__(self,
                db_path : Path,
                table_name : str,
                spec : Iterable[FieldSpec],
                db_ctx : DBContext) :

        self.db_path = db_path
        self.index : Dict[str, Index] = {}
        self.orig_spec = spec
        self.name = table_name
        self.db_ctx = db_ctx
        self.open = True

        self.spec = self._reform_spec()

    def _drop(self) :
        if not self.open or self.parent is None :
            return
        import shutil
        shutil.rmtree(self.db_path)
        self.open = False
        self.parent = None

    def _reform_spec(self) :
        x = [FieldSpec(s.name, s.type, {**OPT_DEFAULT, **s.options}) for s in self.orig_spec]

        nameset = set()
        for s in x :
            if s.name in nameset :
                raise ValueError(f"Duplicate field name {s.name} in table {self.name}")
            nameset.add(s.name)

            opts = s.options
            if opts["pk"] :
                opts["unique"] = True
                opts["nullable"] = False

        return x

    def _generate_tuple_type(self) :
        tuple_types = []
        for s in self.spec :
            if s.type not in TYPES :
                raise ValueError(f"Invalid type {s.type} for field {s.name}")
            real_type = TYPES[s.type]
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

        self.add_index("pk_" + pk.name, pk.name, unique=True, nullable=False)

    def _create(self) :
        if self.db_path.exists() :
            raise ValueError(f"Table {self.name} directory already exists.")

        # good to go
        self.id = self.db_ctx.generate_id()
        config = {
            "spec" : self.spec,
            "id" : self.id
        }

        self.db_path.mkdir(exist_ok=True)
        (self.db_path / "config").write_text(json.dumps(config))
        (self.db_path / "data").mkdir()
        (self.db_path / "index").mkdir()

        self.record = self._generate_tuple_type()
        self._create_pk()

    def _load_def(self) :
        config = json.loads((self.db_path / "config").read_text())
        self.id = config["id"]
        self.spec = config["spec"]

        self.record = self._generate_tuple_type()

        # TODO : load indexes

    def _data_iter(self) :
        if not self.open :
            raise ValueError(f"Table {self.name} is closed.")

        for entry in (self.db_path / "data").rglob('*') :
            if entry.is_file() :
                heap_id = entry.name
                heap_id = entry.parent.name + heap_id
                heap_id = entry.parent.parent.name + heap_id

                data = msgpack.unpackb(entry.read_bytes())
                record = self.record(*data)
                yield (heap_id,record)


    #################################################################
    # Public API
    #################################################################
    def add_index(self, index_name : str, column : str, **kwargs) :
        if not NAME_REGEX.match(index_name) :
            raise ValueError(f"Invalid index name {index_name} for table {self.name}")

        if index_name in self.index :
            raise ValueError(f"Index {index_name} already exists for table {self.name}")

        col = [x for x in self.spec if x.name == column]
        if len(col) != 1 :
            raise ValueError(f"Invalid column name {column} for table {self.name}")

        new_index = Index(index_name,
                          self.db_path / "index" / index_name,
                          column, col[0].type, self.db_ctx, **kwargs)
        self.index[index_name] = new_index

        new_index._create(self._data_iter)


    def get_spec(self) :
        return self.spec

    def insert(self, record : Any) :
        if not self.open :
            raise ValueError(f"Table {self.name} is closed.")

        if not isinstance(record, self.record) :
            if isinstance(record, dict) :
                record_object = self.record(**record)
            else :
                record_object = self.record(*record)
        print(f"--- record_object = {record_object}")

        for index in self.index.values() :
            success, msg = index.test_for_insert(record_object)
            if not success :
                raise ValueError(f"Failed to insert record: {msg}")

        heap_id = _save_to_heap(self.db_path / "data", record_object)

        for index in self.index.values() :
            index._insert(record_object, heap_id)

        return heap_id

    def scan(self) :
        for record in self._data_iter() :
            yield record[1]
from .lib.types.value import Value
from pathlib import Path
from typing import Dict, Iterable, Any, Callable
import json
import shutil
import logging

from .lib import heap
from .lib.types.heap_id import HeapID

from .globals import (
    NAME_REGEX, DBContext,
    TYPES,
    FieldSpec
    )

from .index import Index


OPT_DEFAULT = {
    "pk" : False,
    "unique" : False,
    "nullable" : True,
    "default" : None
}

def cspec(name : str, type : str, **kwargs) :
    return FieldSpec(name, type, kwargs)



logger = logging.getLogger(__name__)

class Table :
    def __init__(self,
                db_path : Path,
                table_name : str,
                spec : Iterable[FieldSpec],
                db_ctx : DBContext) :

        self.db_path = db_path
        self.indexes : Dict[str, Index] = {}
        self.orig_spec = spec
        self.name = table_name
        self.db_ctx = db_ctx
        self.open = True

        self.spec : tuple[FieldSpec, ...] = self._reform_spec()
        self.spec_map = {s.name : s for s in self.spec}

    def _drop(self) :
        if not self.open :
            return

        for i in self.indexes.values() :
            i.close()

        import shutil
        shutil.rmtree(self.db_path)
        self.open = False

    def _reform_spec(self) -> tuple[FieldSpec, ...] :
        x = [FieldSpec(s.name, s.type, {**OPT_DEFAULT, **s.options}) for s in self.orig_spec]

        nameset = set()
        for s in x :
            if s.name in nameset :
                raise ValueError(f"Duplicate field name {s.name} in table {self.name}")
            if s.type not in TYPES :
                raise ValueError(f"Invalid type {s.type} for field {s.name}")
            nameset.add(s.name)

            opts = s.options
            if opts["pk"] :
                opts["unique"] = True
                opts["nullable"] = False

        return tuple(x)

    def _create_auto_indexes(self) :
        pk = [x for x in self.spec if x.options.get("pk", False)]
        if len(pk) > 1 :
            raise ValueError(f"Table {self.name} has multiple primary keys.")
        elif len(pk) == 1 :
            pk = pk[0]
        else :
            return

        self.add_index("pk_" + pk.name, pk.name, unique=True, nullable=False)

        unique = [x for x in self.spec if x.options.get("unique", False) and not x.options.get("pk", False)]
        for u in unique :
            self.add_index("unq_" + u.name, u.name, unique=True, nullable=False)

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
        self._create_auto_indexes()

    def _load_def(self) :
        config = json.loads((self.db_path / "config").read_text())
        self.id = config["id"]
        self.spec = config["spec"]
        index_path = self.db_path / "index"
        for index in index_path.glob("*") :
            Index._load(index, self.db_ctx)

    def _row_from_storage(self, in_data) :
        """This assumes the data is in the same order as the spec and that it really
        does come from storage (values are Values)."""
        return dict(zip([x.name for x in self.spec], in_data, strict=True))

    def _row_from_user_tuple(self, in_tuple) -> dict :
        return {x.name : Value(x.type, TYPES[x.type](y) if y is not None else None) for x, y in zip(self.spec, in_tuple)}

    def _row_to_storage(self, in_dict) -> list :
        return [in_dict[x.name] for x in self.spec]

    def _row_from_dict(self, in_dict) :
        need = set([x.name for x in self.spec])
        have = set(in_dict.keys())
        if len(need - have) > 0 :
            missing = set()
            for f in need - have :
                spec = self.spec_map[f]
                if spec.options["default"] is not None :
                    default = spec.options["default"]
                    realtype = TYPES[spec.type]
                    if isinstance(default, Callable) :
                        default = realtype(default())
                    else :
                        default = realtype(default)
                    in_dict[f] = default
                elif spec.options["nullable"] :
                    in_dict[f] = None
                else :
                    missing.add(f)
            if len(missing) > 0 :
                raise ValueError(f"Missing fields: {missing} - not nullable, but no default value defined.")
        elif len(have - need) > 0 :
            raise ValueError(f"Unknown fields: {have - need}")

        return {x.name : Value(x.type, TYPES[x.type](in_dict[x.name]) if in_dict[x.name] is not None else None) for x in self.spec}

    def _data_iter(self) -> Iterable[tuple[int, dict[str, Any]]] :
        if not self.open :
            raise ValueError(f"Table {self.name} is closed.")

        for entry in (self.db_path / "data").rglob('*') :
            if entry.is_file() :
                heap_id = HeapID.from_path(entry)

                data = heap.read(self.db_path / "data", heap_id)
                record = self._row_from_storage(data)
                yield (int(heap_id),record)


    def _unwrap(self, data : dict[str, Value] ) -> dict[str, Any] :
        return {x : y.value for x,y in data.items()}

    #################################################################
    # Public API
    #################################################################
    def add_index(self, index_name : str, column : str, **kwargs) -> Index:
        if self.db_ctx.mode == "ro" :
            raise ValueError("Database is in read-only mode.")

        if not self.open :
            raise ValueError(f"Table {self.name} is closed.")

        if not NAME_REGEX.match(index_name) :
            raise ValueError(f"Invalid index name {index_name} for table {self.name}")

        if index_name in self.indexes :
            raise ValueError(f"Index {index_name} already exists for table {self.name}")

        col = [x for x in self.spec if x.name == column]
        if len(col) != 1 :
            raise ValueError(f"Invalid column name {column} for table {self.name}")

        new_index = Index(index_name,
                          self.db_path / "index" / index_name,
                          column, col[0].type, self.db_ctx, **kwargs)
        self.indexes[index_name] = new_index

        new_index._create(self._data_iter)

        return new_index

    def drop_index(self, index_name : str) :
        if self.db_ctx.mode == "ro" :
            raise ValueError("Database is in read-only mode.")

        if not self.open :
            raise ValueError(f"Table {self.name} is closed.")

        if index_name not in self.indexes :
            raise ValueError(f"Index {index_name} does not exist for table {self.name}")

        index_path = self.db_path / "index" / index_name
        self.indexes[index_name].close()
        del self.indexes[index_name]

        shutil.rmtree(index_path)

    def get_spec(self) :
        return self.spec

    def spec_for_column(self, column : str) -> FieldSpec | None :
        col = [x for x in self.spec if x.name == column]
        if len(col) != 1 :
            return None
        return col[0]

    def find_index_for_column(self, column : str) -> str | None:
        index = [k for k, x in self.indexes.items() if x.column == column]
        if len(index) != 1 :
            return None
        return index[0]

    def insert(self, *args, **kwargs) :
        if self.db_ctx.mode == "ro" :
            raise ValueError("Database is in read-only mode.")

        if not self.open :
            raise ValueError(f"Table {self.name} is closed.")

        if len(args) == 1 :
            record = args[0]
            if isinstance(record, dict) :
                record_object = self._row_from_dict(record)
            else :
                record_object = self._row_from_storage(record)
        elif len(args) > 1 :
            raise ValueError(f"Invalid number of arguments for insert(): {len(args)}")
        else :
            record_object = self._row_from_dict(**kwargs)

        logger.debug(f"--- record_object = {record_object}")

        for index in self.indexes.values() :
            success, msg = index.test_for_insert(record_object)
            if not success :
                raise ValueError(f"Failed to insert record: {msg}")

        heap_id = heap.write(self.db_path / "data", self._row_to_storage(record_object))

        for index in self.indexes.values() :
            index.insert(record_object, int(heap_id))

        return heap_id

    def scan(self, unwrap : bool = True) -> Iterable[dict[str, Any]]:
        for record in self._data_iter() :
            if unwrap :
                yield self._unwrap(record[1])
            else :
                yield record[1]

    def index_scan(self, name : str, key : Any = None, op : str | None = None, unwrap : bool = True) -> Iterable[dict[str, Any]]:
        if name not in self.indexes :
            raise ValueError(f"Index {name} does not exist for table {self.name}")
        if not self.open :
            raise ValueError(f"Table {self.name} is closed.")

        for block in self.indexes[name].scan(key, op) :
            row = self._row_from_storage(heap.read(self.db_path / "data", block))
            if unwrap :
                yield self._unwrap(row)
            else :
                yield row




    def print_index(self, name : str) :
        self.indexes[name].print_tree()

    def index_list(self) :
        return list(self.indexes.keys())

    def delete(self, row : dict[str, Any]) -> bool :
        if self.db_ctx.mode == "ro" :
            raise ValueError("Database is in read-only mode.")

        if not self.open :
            raise ValueError(f"Table {self.name} is closed.")

        victim = self._row_from_dict(row)

        for block_id, record in self._data_iter() :
            if record == victim :
                logger.debug(f"Deleting record{record}")
                heap.delete(self.db_path / "data", block_id)
                for index in self.indexes.values() :
                    index.delete(row)
                return True

        return False

    def delete_from_query(self, query : Any) -> int :
        if self.db_ctx.mode == "ro" :
            raise ValueError("Database is in read-only mode.")

        if not self.open :
            raise ValueError(f"Table {self.name} is closed.")

        from .query import Query

        if not isinstance(query, Query) :
            raise ValueError(f"Invalid query type {type(query)}")

        count = 0
        for row in query.run() :
            if self.delete(row) :
                count += 1
        return count

    def index(self, index_name : str) -> Index :
        return self.indexes[index_name]
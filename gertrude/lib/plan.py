from dataclasses import dataclass
from enum import Enum
from itertools import islice
import itertools
from types import GeneratorType
from typing import Any, Iterable, List, Tuple, cast, override

from gertrude.lib.types.colref import ColRef
from gertrude.util import SortSpec
from . import expr_nodes as node
from .types.value import Value, valueNull

import logging
logger = logging.getLogger(__name__)


class OpType(Enum) :
    read = "read"
    scan = "scan"
    filter = "filter"
    sort = "sort"
    distinct = "distinct"
    project = "project"
    limit = "limit"
    join = "join"
    rename = "rename"

@dataclass
class QueryOp :
    op : OpType

    def run(self, data : Iterable[dict[str, Value]]) -> Iterable[dict[str, Value]]:
        raise NotImplementedError("Subclasses must implement run()")

    def columns(self, in_cols : set[ColRef]) -> set[ColRef] :
        raise NotImplementedError("Subclasses must implement columns()")

type QueryPlan = List[QueryOp]

class ReadOp(QueryOp) :
    def __init__(self, table_name : str) :
        super().__init__(OpType.read)

        self.table_name_ = table_name

    @property
    def table_name(self) -> str :
        return self.table_name_

    def __repr__(self) :
        return f"ReadOp('{self.table_name}')"

    @override
    def run(self, _) -> Iterable[dict[str, Value]] :
        raise NotImplementedError("ReadOp is preplanner and should not be in a planned query.")

class ScanOp(QueryOp) :
    def __init__(self, scan : Iterable[dict[str, Value]], description : str = "") :
        super().__init__(OpType.scan)

        self.scan_ = scan
        self.description_ = description

    @property
    def scan(self) :
        return self.scan_

    @property
    def description(self) :
        return self.description_

    def __str__(self) :
        if self.description == "" :
            return super().__str__()
        return f"ScanOp({self.description})"

    @override
    def run(self, _ : list[dict]) -> Iterable[dict[str, Value]] :
        logger.debug(f"Scanning {self.description}")
        return self.scan

class FilterOp(QueryOp) :
    def __init__(self, exprs : List[node.ExprNode]) :
        super().__init__(OpType.filter)

        self.exprs_ = exprs

    @property
    def exprs(self) -> list[node.ExprNode] :
        return self.exprs_

    def __str__(self) :
        return f"FilterOp({self.exprs})"

    @override
    def run(self, data : Iterable[dict[str, Value]]) -> Iterable[dict[str, Value]] :
        logger.debug(f"Filtering by {self.exprs}")
        retval : list[dict] = []
        for row in data :
            if all(f.calc(row) for f in self.exprs) :
                retval.append(row)
        return retval

    @override
    def columns(self, in_cols : set[ColRef]) -> set[ColRef] :
        return in_cols


class SortOp(QueryOp) :
    def __init__(self, spec : List[SortSpec]) :
        super().__init__(OpType.sort)

        self.spec_ = spec

    def __str__(self) :
        return f"SortOp({self.spec})"

    @property
    def spec(self) -> list[SortSpec]:
        return self.spec_

    @override
    def run(self, data : Iterable[dict[str, Value]]) -> Iterable[dict[str, Any]] :
        logger.debug(f"Sorting by {self.spec}")
        # Sort must have a list to work with.
        if isinstance(data, GeneratorType) :
            retval = [ x for x in data ]
        else :
            retval = cast(list[dict[str, Value]], data)
        logger.debug(f"row count in = {len(retval)}")
        # python sort is stable, so we sort with minor keys first
        # and then the relative ordering is maintained as we sort
        # by the major keys.
        for s in reversed(self.spec) :
            retval.sort(reverse=(s.order == "desc"),
                        key=lambda row : s.expr.calc(row))
        logger.debug(f"row count out = {len(retval)}")
        return retval

    @override
    def columns(self, in_cols : set[ColRef]) -> set[ColRef] :
        return in_cols

class DistinctOp(QueryOp) :
    def __init__(self, keys : List[str]) :
        super().__init__(OpType.distinct)

        self.keys_ = keys

    @property
    def keys(self) -> list[str] :
        return self.keys_

    @override
    def run(self, data : Iterable[dict[str, Value]]) -> Iterable[dict[str, Value]] :
        logger.debug(f"Distinct by {self.keys}")
        seen : set[tuple] = set()
        retval : list[dict] = []
        keys : list[str] = self.keys
        for row in data :
            if len(keys) == 0 :
                keys = list(row.keys())
                logger.debug(f"getting keys from row = {keys}")
            key = tuple(row[c] for c in keys)
            if key not in seen :
                seen.add(key)
                retval.append(row)
        return retval

    @override
    def columns(self, in_cols : set[ColRef]) -> set[ColRef] :
        return in_cols

class ProjectOp(QueryOp) :
    def __init__(self, retain : bool, columns : List[tuple[str, node.ExprNode]]) :
        super().__init__(OpType.project)

        self.retain = retain
        self.column_list = columns

    def __str__(self) :
        return f"Project({self.retain}, {self.columns})"

    @override
    def run(self, data : Iterable[dict[str, Value]]) -> Iterable[dict[str, Value]] :
        logger.debug(f"Projecting (retain = {self.retain}) {self.column_list}")
        if self.retain :
            retval = [ {**x, **{ c : e.calc(x) for c,e in self.column_list }} for x in data ]
        else :
            retval = [ { c : e.calc(x) for c,e in self.column_list } for x in data ]
        return retval

    def columns(self, in_cols : set[ColRef]) -> set[ColRef] :
        new_cols = set([ ColRef(c) for c,e in self.column_list ])
        if self.retain :
            return in_cols | new_cols
        else :
            return new_cols

class RenameOp(QueryOp) :
    def __init__(self, columns : List[tuple[str, str]]) :
        super().__init__(OpType.rename)

        self.column_map = {c : e for c,e in columns}

    def __str__(self) :
        return f"Rename({self.column_map})"

    @override
    def run(self, data : Iterable[dict[str, Value]]) -> Iterable[dict[str, Value]] :
        logger.debug(f"Renaming {self.column_map}")
        retval = [ { self.column_map.get(c,c) :  x[c] for c in x } for x in data ]
        return retval

    def columns(self, in_cols : set[ColRef]) -> set[ColRef] :
        colref_map = { ColRef(c) : ColRef(e) for c,e in self.column_map.items() }
        logger.debug(f"-- Rename : colref_map = {colref_map}")

        retval : set[ColRef] = set()
        for c in in_cols :
            logger.debug(f"-- Rename : Matching {c}")
            matches = [ x for x in colref_map if x.matchedBy(c) ]
            if len(matches) > 1 :
                raise ValueError(f"Column {c} matched more than one column [{', '.join(map(str, matches))}] in rename")
            elif len(matches) == 0 :
                logger.debug(f"-- Rename : No match")
                retval.add(c)
            else :
                logger.debug(f"-- Rename : Found one match {matches[0]}")
                retval.add(colref_map[matches[0]])
        return retval

class LimitOp(QueryOp) :
    def __init__(self, limit : int) :
        super().__init__(OpType.limit)

        self.limit_ = limit

    def __str__(self) :
        return f"Limit({self.limit})"

    @property
    def limit(self) -> int :
        return self.limit_

    @override
    def run(self, data : Iterable[dict[str, Value]] ) -> list[dict[str, Value]] :
        return list(islice(data, self.limit))

    def columns(self, in_cols : set[ColRef]) -> set[ColRef] :
        return in_cols

class JoinOp(QueryOp) :
    def __init__(self, right : Any, on : str | Tuple[str, str], how : str = "inner", rename : bool | tuple[str, str] = False) :
        super().__init__(OpType.join)
        from gertrude.query import Query

        if not isinstance(right, Query) :
            raise ValueError(f"right must be a Query, got {type(right)}")

        if how not in ("inner", "left_outer") :
            raise ValueError("how must be one of inner or left_outer")

        self.right_ = right
        self.how_ = how
        self.on_ = on
        if isinstance(rename, tuple) :
            self.rename_ = True
            self.left_rename_, self.right_rename_ = rename
        elif rename :
            self.rename_ = True
            self.left_rename_, self.right_rename_ = ('_left', '_right')
        else :
            self.rename_ = False

    def __str__(self) :
        return f"Join({self.right_})"

    def _compute_key_maps(self, left_cols : set[str], right_cols : set[str]) -> tuple[dict[str, str], dict[str, str]] :
        if self.rename_ :
            same_keys = left_cols & right_cols
            if len(same_keys) > 0 :
                return { k : k+self.left_rename_ for k in same_keys }, { k : k+self.right_rename_ for k in same_keys }
            else :
                return {}, {}
        else :
            return {}, {}

    @override
    def run(self, data : Iterable[dict[str, Value]] ) -> Iterable[dict[str, Value]] :
        if isinstance(self.on_, str) :
            left_col = right_col = self.on_
        elif isinstance(self.on_, tuple) :
            left_col, right_col = self.on_
        else :
            raise ValueError(f"'on' must be a string or tuple, got {type(self.on_)}")

        left_row = next(iter(data))
        right_row = next(iter(self.right_.run(values=True)))

        empty_row : dict[str, Value] = { k : valueNull() for k in right_row.keys() }
        logger.debug(f"empty_row = {empty_row}")

        left_keys = set(left_row.keys())
        right_keys = set(right_row.keys())
        left_key_map, right_key_map = self._compute_key_maps(left_keys, right_keys)

        hash_map : dict[Value, list[dict[str, Value]]] = {}
        for lrow in self.right_.run(values=True) :

            logger.debug(f"-- right row = {lrow}")

            key = lrow[right_col]
            if key in hash_map :
                hash_map[key].append(lrow)
            else :
                hash_map[key] = [lrow]

        logger.debug(f"hash_map count = {len(hash_map)}")
        logger.debug(f"hash_map = {hash_map}")

        retval : list[dict[str, Value]] = []
        for lrow in itertools.chain([left_row], data) :
            key = lrow[left_col]
            if self.how_ == "inner" :
                if key in hash_map :
                    retval.extend([{**{left_key_map.get(k,k) : v for k,v in lrow.items()},
                                    **{right_key_map.get(k,k) : v for k,v in x.items()}} for x in hash_map[key]])
            elif self.how_ == "left_outer" :
                if key in hash_map :
                    retval.extend([{**{left_key_map.get(k,k) : v for k,v in lrow.items()},
                                    **{right_key_map.get(k,k) : v for k,v in x.items()}} for x in hash_map[key]])
                else :
                    retval.append({**{left_key_map.get(k,k) : v for k,v in lrow.items()},
                                   **{right_key_map.get(k,k) : v for k,v in empty_row.items()}})
            else :
                raise ValueError(f"how must be one of inner or left_outer, got {self.how_}")
        return retval

    def columns(self, left_cols : set[ColRef]) -> set[ColRef] :
        """This isn't quite right as it totally ignore aliasing.
        But join will use alias rather than renaming once aliasing is further along.
        """
        right_cols = self.right_.columns()

        if not self.rename_ :
            return left_cols | right_cols

        retval : set[ColRef] = set()
        left_key_map, right_key_map = self._compute_key_maps(set(x.name for x in left_cols), set(x.name for x in right_cols))

        for c in left_cols :
            retval.add(ColRef(left_key_map.get(c.name, c.name)))
        for c in right_cols :
            retval.add(ColRef(right_key_map.get(c.name, c.name)))

        return retval
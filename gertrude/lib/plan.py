from dataclasses import dataclass
from enum import Enum
from types import GeneratorType
from typing import Any, Generator, List, cast, override

from gertrude.globals import _Row
from gertrude.util import SortSpec
from . import expr_nodes as node

import logging
logger = logging.getLogger(__name__)


type RowGenerator = Generator[_Row, Any, None]

class OpType(Enum) :
    read = "read"
    scan = "scan"
    filter = "filter"
    select = "select"
    add_column = "add_column"
    sort = "sort"
    distinct = "distinct"
    to_dict = "to_dict"

@dataclass
class QueryOp :
    op : OpType
    data : Any

    def __str__(self) :
        return f"{self.op.value}({self.data})"

    def run(self, data : list[dict] | RowGenerator) -> RowGenerator | list[dict] :
        raise NotImplementedError("Subclasses must implement run()")

type QueryPlan = List[QueryOp]

class ReadOp(QueryOp) :
    def __init__(self, table_name : str) :
        super().__init__(OpType.read, table_name)

    @property
    def table_name(self) -> str :
        return self.data

    def __repr__(self) :
        return f"ReadOp('{self.data}')"

    @override
    def run(self, _) :
        raise NotImplementedError("ReadOp is preplanner and should not be in a planned query.")

class ScanOp(QueryOp) :
    def __init__(self, data : RowGenerator, description : str = "") :
        super().__init__(OpType.scan, data)
        self.description_ = description

    @property
    def description(self) :
        return self.description_

    def __str__(self) :
        if self.description == "" :
            return super().__str__()
        return f"{self.op.value}({self.description})"

    @override
    def run(self, _ : list[dict] | RowGenerator) -> RowGenerator | list[dict] :
        logger.debug(f"Scanning {self.description}")
        gen = cast(RowGenerator, self.data)
        return gen

class FilterOp(QueryOp) :
    def __init__(self, data : List[node.ExprNode]) :
        super().__init__(OpType.filter, data)

    @property
    def exprs(self) -> list[node.ExprNode] :
        return cast(list[node.ExprNode], self.data)

    @override
    def run(self, data : list[dict] | RowGenerator) -> list[dict] :
        logger.debug(f"Filtering by {self.exprs}")
        retval : list[dict] = []
        for row in data :
            if isinstance(row, _Row) :
                row = row._asdict()
            if all(f.calc(row) for f in self.exprs) :
                retval.append(row)
        return retval


class SortOp(QueryOp) :
    def __init__(self, data : List[SortSpec]) :
        super().__init__(OpType.sort, data)

    @property
    def spec(self) -> list[SortSpec]:
        return cast(list[SortSpec], self.data)
    @override
    def run(self, data : list[dict] | RowGenerator) -> list[dict] :
        logger.debug(f"Sorting by {self.spec}")
        # Sort must have a list to work with.
        if isinstance(data, GeneratorType) :
            retval = [ x._asdict() if isinstance(x, _Row) else x for x in data ]
        else :
            retval = cast(list[dict[str, Any]], data)
        logger.debug(f"row count in = {len(retval)}")
        # python sort is stable, so we sort with minor keys first
        # and then the relative ordering is maintained as we sort
        # by the major keys.
        for s in reversed(self.spec) :
            retval.sort(reverse=(s.order == "desc"),
                        key=lambda row : s.expr.calc(row))
        logger.debug(f"row count out = {len(retval)}")
        return retval


class DistinctOp(QueryOp) :
    def __init__(self, data : List[str]) :
        super().__init__(OpType.distinct, data)

    @property
    def keys(self) -> list[str] :
        return cast(list[str], self.data)

    @override
    def run(self, data : list[dict] | RowGenerator) -> list[dict] :
        logger.debug(f"Distinct by {self.keys}")
        seen : set[tuple] = set()
        retval : list[dict] = []
        keys : list[str] = self.keys
        for row in data :
            if isinstance(row, _Row) :
                row = row._asdict()
            if len(keys) == 0 :
                keys = list(row.keys())
                logger.debug(f"getting keys from row = {keys}")
            key = tuple(row[c] for c in keys)
            if key not in seen :
                seen.add(key)
                retval.append(row)
        return retval


class ToDictOp(QueryOp) :
    def __init__(self, _ : Any) :
        super().__init__(OpType.to_dict, None)

    @override
    def run(self, data : list[dict] | RowGenerator) -> list[dict] :
        if isinstance(data, GeneratorType) :
            retval = [ x._asdict() if isinstance(x, _Row) else x for x in data ]
        else :
            retval = cast(list[dict[str, Any]], data)
        return retval

from dataclasses import dataclass
from enum import Enum
from typing import Any, Generator, List

from gertrude.globals import _Row


class OpType(Enum) :
    read = "read"
    scan = "scan"
    filter = "filter"
    select = "select"
    add_column = "add_column"
    sort = "sort"
    distinct = "distinct"

@dataclass
class QueryOp :
    op : OpType
    data : Any

    def __str__(self) :
        return f"{self.op.value}({self.data})"

type QueryPlan = List[QueryOp]
type RowGenerator = Generator[_Row, Any, None]

@dataclass
class ReadOp(QueryOp) :
    def __init__(self, table_name : str) :
        super().__init__(OpType.read, table_name)

    @property
    def table_name(self) -> str :
        return self.data

    def __repr__(self) :
        return f"ReadOp('{self.data}')"

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

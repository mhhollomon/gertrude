from dataclasses import dataclass
from enum import Enum
from typing import Any, Generator, List, cast

from gertrude.index import KeyBound
from gertrude.table import Table
from .database import Database

from .globals import _Row, Step, STEP_TYPE
from .lib import expr_nodes as node

import logging
logger = logging.getLogger(__name__)

class OpType(Enum) :
    scan = "scan"
    filter = "filter"
    select = "select"
    add_column = "add_column"
    sort = "sort"

@dataclass
class QueryOp :
    op : OpType
    data : Any

type QueryPlan = List[QueryOp]
type RowGenerator = Generator[_Row, Any, None]

class QueryRunner :
    def __init__(self, db : Database, steps : list[Step]) :
        self.db = db
        self.steps = steps

        self.ops = {
            OpType.scan : self.scan,
            OpType.filter : self.filter,
            OpType.select : self.select,
            OpType.add_column : self.add_column,
            OpType.sort : self.sort
        }

    def scan(self, scan : QueryOp, _ : list[dict] | RowGenerator) -> RowGenerator :
        return cast(RowGenerator, scan.data)

    def filter(self, filter : QueryOp, data : list[dict] | RowGenerator) -> list[dict]  :
        logger.debug(f"Filtering by {filter.data}")
        data = [ x._asdict() if isinstance(x, _Row) else x for x in data if all(f.calc(x) for f in filter.data) ]
        return data

    def select(self, select : QueryOp, data : list[dict] | RowGenerator) -> list[dict] :
        logger.debug(f"Selecting {select.data}")
        columns = select.data
        data = [ { c : e.calc(x) for c,e in columns } for x in data ]
        return data

    def add_column(self, add_column : QueryOp, data : list[dict] | RowGenerator) -> list[dict] :
        logger.debug(f"Adding column {add_column.data}")
        columns = add_column.data
        data = [ {**(x._asdict() if isinstance(x, _Row) else x), **{ c : e.calc(x) for c,e in columns }} for x in data ]
        return data

    def sort(self, sort : QueryOp, data : list[dict] | RowGenerator) -> list[dict] :
        logger.debug(f"Sorting by {sort.data}")
        retval = sorted(data, key=lambda row : tuple(tuple(row[c] for c in sort.data)))
        if isinstance(retval[0], _Row) :
            retval = [ x._asdict() if isinstance(x, _Row) else x for x in retval ]
        return retval

    def _test_filter_for_index(self, filter : Step, table : Table) -> RowGenerator | None :
        if filter.type != STEP_TYPE.FILTER :
            # really this is just to get the type system to hush.
            return None
        expr = filter.data[0]
        logger.debug(f"isinstance(expr, node.Operation) = {isinstance(expr, node.Operation)}")
        logger.debug(f"expr.name() = '{expr.name()}'")
        if isinstance(expr, node.Operation) and expr.name() in ['eq','gt', 'ge', 'lt', 'le'] \
            and isinstance(expr.left, node.ColumnName) and isinstance(expr.right, node.Literal) \
            and table.spec_for_column(expr.left.name) is not None \
            and table.find_index_for_column(expr.left.name) is not None :

            key = expr.right.calc({})
            index_name = table.find_index_for_column(expr.left.name)
            logger.debug(f"Using index '{index_name}' on column {expr.left.name} for key = {key} with operator {expr.name()}")
            return table.index_scan(index_name, key, op=expr.name()) # type: ignore
        else :
             return None

    def plan(self) -> QueryPlan:
        logger.debug(f"Planning query with steps {self.steps}")

        plan : QueryPlan = []


        step_index = 0
        logger.debug(f"step_index = {step_index}")
        step = self.steps[step_index]

        if step.type != STEP_TYPE.READ :
            raise ValueError("First step must be read")

        table_name = step.data
        table = self.db.table(table_name=table_name)

        if table is None :
            raise ValueError(f"Table {table_name} does not exist.")

        if len(self.steps) > 1 :
            step_index += 1
            scan = self._test_filter_for_index(self.steps[step_index], table)
            if scan is None :
                step_index -= 1
                logger.debug(f"Using table scan to read table {table_name}")
                scan = table.scan()
        else :
            logger.debug(f"Using table scan to read table {table_name}")
            scan = table.scan()

        plan.append(QueryOp(OpType.scan, scan))

        for s in self.steps[step_index + 1:] :
            if s.type == STEP_TYPE.FILTER :
                plan.append(QueryOp(OpType.filter, s.data))
            elif s.type == STEP_TYPE.SELECT :
                plan.append(QueryOp(OpType.select, s.data))
            elif s.type == STEP_TYPE.ADD_COLUMN :
                plan.append(QueryOp(OpType.add_column, s.data))
            elif s.type == STEP_TYPE.SORT :
                plan.append(QueryOp(OpType.sort, s.data))

        return plan

    def run(self) :
        logger.debug(f"Running query with steps {self.steps}")

        plan = self.plan()

        data = []
        for op in plan :
            data = self.ops[op.op](op, data)

        return data

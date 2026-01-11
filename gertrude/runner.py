from typing import Any, Iterable, cast

from .lib.plan import OpType, QueryOp, QueryPlan, ScanOp, FilterOp, ReadOp, UnwrapOp
from .table import Table

from .globals import GertrudeError
from .lib import expr_nodes as node

import logging
logger = logging.getLogger(__name__)

class QueryRunner :
    def __init__(self, db : Any, steps : QueryPlan) :
        self.db = db
        self.steps = steps

    def _test_filter_for_index(self, filter : QueryOp, table : Table) -> tuple[Iterable[dict[str, Any]], str] | None :
        if filter.op != OpType.filter :
            # really this is just to get the type system to hush.
            return None
        filter = cast(FilterOp, filter)
        expr = filter.exprs[0]
        logger.debug(f"isinstance(expr, node.Operation) = {isinstance(expr, node.Operation)}")
        logger.debug(f"expr.name = '{expr.name}'")
        if isinstance(expr, node.Operation) and expr.name in ['eq','gt', 'ge', 'lt', 'le'] \
            and isinstance(expr.left, node.ColumnName) and isinstance(expr.right, node.Literal) \
            and table.spec_for_column(expr.left.name) is not None \
            and table.find_index_for_column(expr.left.name) is not None :

            key = expr.right.calc({})
            index_name = table.find_index_for_column(expr.left.name)
            logger.debug(f"Using index '{index_name}' on column {expr.left.name} for key = {key} with operator {expr.name}")
            scan = table.index_scan(index_name, key, op=expr.name) # type: ignore
            description = f"Using index '{index_name}' on column {expr.left.name} for key = {key} with operator {expr.name}"
            return scan, description
        else :
             return None

    def plan(self) -> QueryPlan:
        from .database import Database

        db = cast(Database, self.db)

        logger.debug(f"Planning query with steps {self.steps}")

        new_plan : QueryPlan = []


        step_index = 0
        step = self.steps[step_index]

        if step.op != OpType.read :
            raise ValueError("First step must be read")


        table_name = cast(ReadOp, step).table_name
        table = db.table(table_name=table_name)

        if table is None :
            raise ValueError(f"Table {table_name} does not exist.")

        if len(self.steps) > 1 :
            step_index += 1
            scan_return = self._test_filter_for_index(self.steps[step_index], table)
            if scan_return is None :
                step_index -= 1
                logger.debug(f"Using table scan to read table {table_name}")
                new_plan.append(ScanOp(table.scan(), f"table scan of {table_name}"))
            else :
                scan, description = scan_return
                new_plan.append(ScanOp(scan, description))
        else :
            logger.debug(f"Using table scan to read table {table_name}")
            new_plan.append(ScanOp(table.scan(), f"table scan of {table_name}"))

        if step_index < len(self.steps)-1 :
            new_plan.extend(self.steps[step_index+1:])

        if len(new_plan) == 1:
            new_plan.append(UnwrapOp())

        return new_plan

    def run(self) -> list[dict[str, Any]] :
        logger.debug(f"Running query with steps {self.steps}")

        plan = self.plan()

        data = []
        for op in plan :
            data = op.run(data)

        if not isinstance(data, list) :
            raise GertrudeError(f"Expected list of rows, got {type(data)}")

        return data

    def show_plan(self) -> list[str] :
        retval : list[str] = [str(op) for op in self.plan()]
        return retval

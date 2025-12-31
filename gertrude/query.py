from typing import Any, List, Self, Tuple, cast

from .expression import expr_parse

from .lib.expr_nodes import ExprNode
from .lib import plan
from .runner import QueryRunner


class Query:
    def __init__(self, parent : Any, table : str) :
        self.parent = parent
        self.steps : plan.QueryPlan = [plan.ReadOp(table)]

    def _generate_select_step(self, expressions : Tuple[Tuple[str, str] | str], optype : plan.OpType) :
        new_exprs = []
        for e in expressions :
            if isinstance(e, str) :
                new_exprs.append((e, expr_parse(e)))
            elif isinstance(e, tuple) :
                if len(e) != 2 :
                    raise ValueError(f"Invalid expression {e}")
                name, expr = e
                new_exprs.append((name, expr_parse(expr)))
        self.steps.append(plan.QueryOp(optype, new_exprs))

    def filter(self, *conditions : str) -> Self :
        expr : List[ExprNode] = []
        for c in conditions :
            e = expr_parse(c)
            expr.append(e)
        self.steps.append(plan.QueryOp(plan.OpType.filter, expr))
        return self

    def select(self, *expressions : Tuple[str, str] |str) -> Self :
        self._generate_select_step(cast(Tuple[Tuple[str, str] | str], expressions), plan.OpType.select)
        return self

    def add_column(self, name : str, expr : str) -> Self :
        self._generate_select_step(((name, expr),), plan.OpType.add_column)
        return self

    def sort(self, *columns : str) -> Self :
        self.steps.append(plan.QueryOp(plan.OpType.sort, columns))
        return self

    def distinct(self, *columns : str) -> Self :
        self.steps.append(plan.QueryOp(plan.OpType.distinct, columns))
        return self

    def _create_runner(self) -> QueryRunner :
        # This needs to be scoped due to circular dependencies

        from .database import Database

        if isinstance(self.parent, Database) :
            return QueryRunner(self.parent, self.steps)
        else :
            raise ValueError(f"Invalid parent type {type(self.parent)}")

    def run(self) -> list[dict[str, Any]] :
        return self._create_runner().run()

    def show_plan(self) -> list[str] :
        return self._create_runner().show_plan()
from typing import Any, List, Self, Set, Tuple, cast


from .expression import expr_parse

from .lib.expr_nodes import ExprNode
from .lib import plan
from .runner import QueryRunner
from .util import SortSpec, asc
from .lib.types.colref import ColRef


class Query:
    def __init__(self, parent : Any, table : str) :
        self.parent = parent
        self.steps : plan.QueryPlan = [plan.ReadOp(table)]
        self.runner_ : QueryRunner | None = None

    def _generate_select_step(self, expressions : Tuple[Tuple[str, str] | str, ...], retain : bool) :
        new_exprs = []
        for e in expressions :
            if isinstance(e, str) :
                new_exprs.append((e, expr_parse(e)))
            elif isinstance(e, tuple) :
                if len(e) != 2 :
                    raise ValueError(f"Invalid expression {e}")
                name, expr = e
                new_exprs.append((name, expr_parse(expr)))
        self.steps.append(plan.ProjectOp(retain, new_exprs))

    def filter(self, *conditions : str) -> Self :
        expr : List[ExprNode] = []
        for c in conditions :
            e = expr_parse(c)
            expr.append(e)
        self.steps.append(plan.FilterOp(expr))
        return self

    def select(self, *expressions : Tuple[str, str] |str) -> Self :
        self._generate_select_step(expressions, False)
        return self

    def add_column(self, name : str, expr : str) -> Self :
        self._generate_select_step(((name, expr),), True)
        return self

    def add_columns(self, *expressions : Tuple[str, str] ) -> Self :
        self._generate_select_step(expressions, True)
        return self

    def rename_columns(self, *columns : Tuple[str, str]) -> Self :
        self.steps.append(plan.RenameOp(list(columns)))
        return self

    def sort(self, *columns : str | SortSpec) -> Self :
        spec = [ c if isinstance(c, SortSpec) else asc(c) for c in columns ]
        self.steps.append(plan.SortOp(spec))
        return self

    def distinct(self, *columns : str) -> Self :
        self.steps.append(plan.DistinctOp(list(columns)))
        return self

    def limit(self, limit : int) -> Self :
        self.steps.append(plan.LimitOp(limit))
        return self

    def join(self, right : 'Query', on : str | Tuple[str, str], how : str = "inner", rename : bool | tuple[str, str] = False) -> Self :

        self.steps.append(plan.JoinOp(right, on, how, rename))
        return self

    def _create_runner(self) -> QueryRunner :
        if self.runner_ is None :
            from .database import Database

            if isinstance(self.parent, Database) :
                self.runner_ = QueryRunner(self.parent, self.steps)
            else :
                raise ValueError(f"Invalid parent type {type(self.parent)}")

        return self.runner_

    def run(self, values:bool = False) -> list[dict[str, Any]] :
        return self._create_runner().run(values)

    def show_plan(self) -> list[str] :
        return self._create_runner().show_plan()

    def columns(self) -> Set[ColRef] :
        return self._create_runner().columns()

    def optplan(self) -> str :
        return self._create_runner().optplan()
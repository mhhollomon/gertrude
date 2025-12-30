from typing import Any, List, Self, Tuple, cast

from .expression import expr_parse
from .globals import Step, STEP_TYPE

from .lib.expr_nodes import ExprNode


class Query:
    def __init__(self, parent : Any, table : str) :
        self.parent = parent
        self.steps : List[Step] = [Step(STEP_TYPE.READ, table)]

    def _generate_select_step(self, expressions : Tuple[Tuple[str, str] | str], step_type : STEP_TYPE) :
        new_exprs = []
        for e in expressions :
            if isinstance(e, str) :
                new_exprs.append((e, expr_parse(e)))
            elif isinstance(e, tuple) :
                if len(e) != 2 :
                    raise ValueError(f"Invalid expression {e}")
                name, expr = e
                new_exprs.append((name, expr_parse(expr)))
        self.steps.append(Step(step_type, new_exprs))

    def filter(self, *conditions : str) -> Self :
        expr : List[ExprNode] = []
        for c in conditions :
            e = expr_parse(c)
            expr.append(e)
        self.steps.append(Step(STEP_TYPE.FILTER, expr))
        return self

    def select(self, *expressions : Tuple[str, str] |str) -> Self :
        self._generate_select_step(cast(Tuple[Tuple[str, str] | str], expressions), STEP_TYPE.SELECT)
        return self

    def add_column(self, name : str, expr : str) -> Self :
        self._generate_select_step(((name, expr),), STEP_TYPE.ADD_COLUMN)
        return self

    def sort(self, *columns : str) -> Self :
        self.steps.append(Step(STEP_TYPE.SORT, columns))
        return self

    def run(self) :
        # These need to be scoped due to circular dependencies

        from .database import Database
        from .runner import QueryRunner

        if isinstance(self.parent, Database) :
            return QueryRunner(self.parent, self.steps).run()
        else :
            raise ValueError(f"Invalid parent type {type(self.parent)}")

from typing import Any, List, Self, Tuple, cast

from .expression import expr_parse
from .globals import ExprNode

type Step = Tuple[int, Any]

_STAGE_READ = 0
_STAGE_FILTER = 1
_STAGE_SELECT = 2
_STAGE_SORT = 3
_STAGE_ADD_COLUMN = 4

class Query:
    def __init__(self, parent : Any, table : str) :
        self.parent = parent
        self.steps : List[Step] = [(_STAGE_READ, table)]

    def _gnerate_select_step(self, expressions : Tuple[Tuple[str, str] | str], stage_type : int) :
        new_exprs = []
        for e in expressions :
            if isinstance(e, str) :
                new_exprs.append((e, expr_parse(e)))
            elif isinstance(e, tuple) :
                if len(e) != 2 :
                    raise ValueError(f"Invalid expression {e}")
                name, expr = e
                new_exprs.append((name, expr_parse(expr)))
        self.steps.append((stage_type, new_exprs))

    def filter(self, *conditions : tuple) -> Self :
        self.steps.append((_STAGE_FILTER, conditions))
        return self

    def select(self, *expressions : Tuple[str, str] |str) -> Self :
        self._gnerate_select_step(cast(Tuple[Tuple[str, str] | str], expressions), _STAGE_SELECT)
        return self

    def add_column(self, name : str, expr : str) -> Self :
        self._gnerate_select_step(((name, expr),), _STAGE_ADD_COLUMN)
        return self

    def sort(self, *columns : str) -> Self :
        self.steps.append((_STAGE_SORT, columns))
        return self

    def run(self) :
        # These need to be scoped due to circular dependencies

        from .database import Database
        from .runner import QueryRunner

        if isinstance(self.parent, Database) :
            return QueryRunner(self.parent, self.steps).run()
        else :
            raise ValueError(f"Invalid parent type {type(self.parent)}")

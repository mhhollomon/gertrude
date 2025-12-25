from typing import Any, List, Self, Tuple

from .expression import expr_parse

type Step = Tuple[int, Any]

_STAGE_READ = 0
_STAGE_FILTER = 1
_STAGE_SELECT = 2
_STAGE_SORT = 3

class Query:
    def __init__(self, parent : Any, table : str) :
        self.parent = parent
        self.steps : List[Step] = [(_STAGE_READ, table)]

    def filter(self, *conditions : tuple) -> Self :
        self.steps.append((_STAGE_FILTER, conditions))
        return self

    def select(self, *expressions : str) -> Self :
        expr = [expr_parse(e) for e in expressions]
        self.steps.append((_STAGE_SELECT, expr))
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

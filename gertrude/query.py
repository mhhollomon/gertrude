from typing import Any, List, Self, Tuple

type Step = Tuple[int, Any]

_STAGE_READ = 0
_STAGE_FILTER = 1
_STAGE_SELECT = 2
_STAGE_SORT = 3

class Query:
    def __init__(self, parent : Any, table : str) :
        self.parent = parent
        self.steps : List[Step] = [(_STAGE_READ, table)]
        self.stage = _STAGE_READ

    def filter(self, *conditions : tuple) -> Self :
        if self.stage > _STAGE_FILTER :
            raise ValueError("Filter can not be called after select.")

        self.stage = _STAGE_FILTER
        self.steps.append((_STAGE_FILTER, conditions))
        return self

    def select(self, *columns : str) -> Self :
        if self.stage >= _STAGE_SELECT :
            raise ValueError("Select can only be called once.")

        self.stage = _STAGE_SELECT
        self.steps.append((_STAGE_SELECT, columns))
        return self

    def sort(self, *columns : str) -> Self :
        if self.stage >= _STAGE_SORT :
            raise ValueError("Sort can only be called once.")
        self.stage = _STAGE_SORT
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

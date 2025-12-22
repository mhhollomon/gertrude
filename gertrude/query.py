from typing import Any

class Query:
    def __init__(self, parent : Any, table : str) :
        self.parent = parent
        self.steps = [('read', table)]

    def run(self) :
        # These need to be scoped due to circular dependencies

        from .database import Database
        from .runner import QueryRunner

        if isinstance(self.parent, Database) :
            return QueryRunner(self.parent, self.steps).run()
        else :
            raise ValueError(f"Invalid parent type {type(self.parent)}")
        
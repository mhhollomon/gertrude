from dataclasses import asdict
from typing import Any, cast
from .database import Database
from .query import Step, _STAGE_READ, _STAGE_FILTER, _STAGE_SELECT, _STAGE_SORT, _STAGE_ADD_COLUMN

import logging

logger = logging.getLogger(__name__)

class QueryRunner :
    def __init__(self, db : Database, steps : list[Step]) :
        self.db = db
        self.steps = steps

    def run(self) :
        logger.debug(f"Running query with steps {self.steps}")

        step_index = 0
        logger.debug(f"step_index = {step_index}")

        if self.steps[step_index][0] != _STAGE_READ :
            raise ValueError("First step must be read")

        table_name = self.steps[step_index][1]
        table = self.db.table(table_name=table_name)
        data = [ cast(dict, x._asdict()) for x in table.scan() ]
        data = cast(list[dict[str, Any]], data)

        for i in range(step_index + 1, len(self.steps)) :
            if self.steps[i][0] == _STAGE_FILTER :
                logger.debug(f"Filtering by {self.steps[i][1]}")
                data = [ x for x in data if all(f in x.items() for f in self.steps[i][1]) ]
            elif self.steps[i][0] == _STAGE_SELECT :
                logger.debug(f"Selecting {self.steps[i][1]}")
                columns = self.steps[i][1]
                data = [ { c : e.calc(x) for c,e in columns } for x in data ]
            elif self.steps[i][0] == _STAGE_ADD_COLUMN :
                logger.debug(f"Adding columns {self.steps[i][1]}")
                columns = self.steps[i][1]
                data = [ {**x, **{ c : e.calc(x) for c,e in columns }} for x in data ]
            elif self.steps[i][0] == _STAGE_SORT :
                logger.debug(f"Sorting by {self.steps[i][1]}")
                columns = self.steps[i][1]
                data = sorted(data, key=lambda x : tuple(x[c] for c in columns))
            else :
                raise ValueError(f"Invalid step {self.steps[i][0]}")


        return data


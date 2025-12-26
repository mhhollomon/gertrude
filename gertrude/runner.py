from typing import Any, cast
from .database import Database

from .globals import Step, STEP_TYPE

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
        step = self.steps[step_index]

        if step.type != STEP_TYPE.READ :
            raise ValueError("First step must be read")

        table_name = self.steps[step_index][1]
        table = self.db.table(table_name=table_name)
        data = [ cast(dict, x._asdict()) for x in table.scan() ]
        data = cast(list[dict[str, Any]], data)

        for i in range(step_index + 1, len(self.steps)) :
            step = self.steps[i]
            if step.type == STEP_TYPE.FILTER :
                logger.debug(f"Filtering by {step.data}")
                data = [ x for x in data if all(f.calc(x) for f in step.data) ]
            elif step.type == STEP_TYPE.SELECT :
                logger.debug(f"Selecting {step.data}")
                columns = step[1]
                data = [ { c : e.calc(x) for c,e in columns } for x in data ]
            elif step.type == STEP_TYPE.ADD_COLUMN :
                logger.debug(f"Adding column {step.data}")
                columns = step[1]
                data = [ {**x, **{ c : e.calc(x) for c,e in columns }} for x in data ]
            elif step.type == STEP_TYPE.SORT :
                logger.debug(f"Sorting by {step.data}")
                columns = step[1]
                data = sorted(data, key=lambda x : tuple(x[c] for c in columns))
            else :
                raise ValueError(f"Invalid step type {step.type}")


        return data


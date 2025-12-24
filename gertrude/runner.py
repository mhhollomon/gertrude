from dataclasses import asdict
from typing import cast
from .database import Database
from .query import Step, _STAGE_READ, _STAGE_FILTER, _STAGE_SELECT, _STAGE_SORT

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

        step_index += 1
        logger.debug(f"step_index = {step_index}")
        if step_index >= len(self.steps) :
            return data

        while self.steps[step_index][0] == _STAGE_FILTER :
            filters = self.steps[step_index][1]
            data = [ x for x in data if all(f in x.items() for f in filters) ]
            step_index += 1
            logger.debug(f"step_index = {step_index}")
            if step_index >= len(self.steps) :
                return data

        if self.steps[step_index][0] == _STAGE_SELECT :
            columns = self.steps[step_index][1]
            data = [ { c : x[c] for c in columns } for x in data ]
            step_index += 1
            logger.debug(f"step_index = {step_index}")

        if step_index >= len(self.steps) :
            logger.debug(f"Returning data after select {data}")
            return data

        while step_index < len(self.steps) and self.steps[step_index][0] == _STAGE_SORT :
            logger.debug(f"Sorting by {self.steps[step_index][1]}")
            columns = self.steps[step_index][1]
            data = sorted(data, key=lambda x : tuple(x[c] for c in columns))
            step_index += 1
            logger.debug(f"step_index = {step_index}")

        return data


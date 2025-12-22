from dataclasses import asdict
from .database import Database

import logging

logger = logging.getLogger(__name__)

class QueryRunner :
    def __init__(self, db : Database, steps : list) :
        self.db = db
        self.steps = steps

    def run(self) :
        logger.debug(f"Running query with steps {self.steps}")

        if self.steps[0][0] != 'read' :
            raise ValueError("First step must be read")
        
        table_name = self.steps[0][1]
        table = self.db.table_defs[table_name]
        return [ x._asdict() for x in table.scan() ]
        
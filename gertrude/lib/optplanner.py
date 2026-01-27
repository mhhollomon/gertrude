
from typing import Any, cast

from .plan import OpType, QueryPlan, ReadOp
from .types.code_block import code_block

def plan(db : Any, steps : QueryPlan) -> code_block:

    retval = code_block(0, 'def my_function() :')

    for step in steps :
        if step.op == OpType.read :
            table_name = cast(ReadOp, step).table_name
            with retval.more_indent() as block :
                block.add(f"table = db.table('{table_name}')")
                block.add("for row in table.scan() :")
                with block.more_indent() as block3 :
                    block3.add("yield row")
                    block3.add("return")


    return retval
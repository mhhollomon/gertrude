
from typing import Any, cast

from .plan import FilterOp, OpType, QueryPlan, ReadOp
from .types.code_block import code_block

def filter_optimize(steps : QueryPlan) -> QueryPlan :
    retval : QueryPlan = []
    delayed_filter : FilterOp | None = None
    for step in steps :
        if step.op == OpType.filter :
            if delayed_filter is None :
                delayed_filter = cast(FilterOp, step)
            else :
                delayed_filter.exprs.extend(cast(FilterOp, step).exprs)
        elif delayed_filter is not None :
            retval.append(delayed_filter)
            delayed_filter = None
            retval.append(step)
        else :
            retval.append(step)
    return retval

def get_step_code(step_num : int, steps : QueryPlan) -> tuple[int, code_block] :
    retval = code_block(0)
    while step_num < len(steps) :
        step = steps[step_num]
        if step.op == OpType.filter :
            filter_step = cast(FilterOp, step)
            expr_list = []
            for expr in filter_step.exprs :
                expr_list.append(expr.to_python())
            retval.add(f"if ({' and '.join(expr_list)}) :")
            with retval.more_indent() as block :
                block.add("retval.append(row)")

        elif step.op == OpType.sort :
            break
        else :
            raise ValueError(f"Unknown step type {step.op}")

        step_num += 1

    return step_num, retval
def plan(db : Any, steps : QueryPlan) -> code_block:

    steps = filter_optimize(steps)

    retval = code_block(0, 'def my_function() :')
    step = steps[0]
    if step.op == OpType.read :
        table_name = cast(ReadOp, step).table_name
        with retval.more_indent() as block :
            block.add(f"table = db.table('{table_name}')")
            block.add("retval : dict[str, Value] = {}")
            block.add("for row in table.scan() :")
            last_step, step_code = get_step_code(1, steps)
            step_code.add_indent()
            block.add(step_code)
    else :
        raise ValueError(f"Expecting a Read step, got {step.op} instead.")


    return retval
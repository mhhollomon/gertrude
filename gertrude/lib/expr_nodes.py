from abc import ABC, abstractmethod
from dataclasses import dataclass
from .types.value import Value, valueTrue, valueFalse, valueNull
from typing import Any, Callable, List

import logging
logger = logging.getLogger(__name__)


class ExprNode(ABC):
    @abstractmethod
    def calc(self, row : dict[str, Value]) -> Value:
        ...

    @abstractmethod
    def to_python(self) -> str :
        ...

    @property
    @abstractmethod
    def name(self) -> str :
        ...


@dataclass
class Operation(ExprNode) :
    category : str
    op : Callable[[Any, Any], Any]
    left : ExprNode
    right : ExprNode

    @property
    def name(self) :
        return self.op.__name__

    def calc(self, row : dict[str, Value]) -> Value :
        left = self.left.calc(row)
        if left.is_null :
            return valueNull()
        right = self.right.calc(row)
        if right.is_null :
            return valueNull()
        return self.op(left, right)

    def to_python(self) :
        return f"({self.left.to_python()} {self.op.__name__} {self.right.to_python()})"

    def __repr__(self) :
        return f"Operation({self.category}, <{self.op.__name__}>, left={self.left}, right={self.right})"

@dataclass
class ColumnName(ExprNode) :
    name_ : str

    def calc(self, row : dict[str, Value]) -> Value :
        try :
            return row[self.name_]
        except KeyError :
            raise KeyError(f"Column {self.name_} not found in row")

    def to_python(self) :
        return f"row['{self.name_}']"

    @property
    def name(self) :
        return self.name_

class Literal(ExprNode) :

    def __init__(self, value : Any, vtype : str) -> None:
        self.value  = Value(vtype, value)

    def calc(self, row : dict[str, Value]) -> Value :
        return self.value

    def to_python(self) :
        return repr(self.value)

    @property
    def name(self) :
        return self.value.type_name

    def __repr__(self) :
        return f"Literal({self.value})"

@dataclass
class MonoOperation(ExprNode) :
    op : Any
    arg : ExprNode

    def calc(self, row : dict[str, Value]) -> Value :
        value = self.arg.calc(row)
        logger.debug(f"MonoOperation {self.op.__name__} arg = {value}")
        retval = self.op(self.arg.calc(row))
        if isinstance(retval, bool) :
            return Value(bool, retval)
        else :
            return retval

    def to_python(self) :
        return f"{self.op.__name__}({self.arg.to_python()})"

    @property
    def name(self) :
        return self.op.__name__

    def __repr__(self) :
        return f"MonoOperation({self.op.__name__}, {self.arg})"

class NVLOp(ExprNode) :
    args : List[ExprNode]

    def __init__(self, *args) :
        self.args = [*args]

    def calc(self, row : dict[str, Value]) -> Value :
        for arg in self.args :
            value = arg.calc(row)
            if not value.is_null :
                return value
        return valueNull()

    def to_python(self) :
        return self.args[0].to_python()

    @property
    def name(self) :
        return "nvl"

class INStmt(ExprNode) :

    def __init__(self, left : ExprNode, right : tuple[ExprNode]) :
        self.left = left
        self.right = right

    def calc(self, row : dict[str, Value]) -> Value :
        test_value : Value = self.left.calc(row)
        for x in self.right :
            if x.calc(row) == test_value :
                return valueTrue()
        return valueFalse()

    def to_python(self) :
        return f"{self.left.to_python()} in something"

    @property
    def name(self) :
        return "in"

    def __repr__(self) :
        return f"INStmt({self.left}, [{self.right}])"

@dataclass
class CaseLeg(ExprNode) :
    condition : ExprNode
    result : ExprNode

    def calc(self, row : dict[str, Value]) -> Value :
        return self.result.calc(row)

    def matches(self, row : dict[str, Value]) -> bool :
        return bool(self.condition.calc(row))

    def to_python(self) :
        return f"if {self.condition.to_python()} : {self.result.to_python()}"

    @property
    def name(self) :
        return "when"

@dataclass
class CaseStmt(ExprNode) :
    legs : list[CaseLeg]
    default : ExprNode

    def calc(self, row : dict[str, Value]) -> Value :
        for leg in self.legs :
            if leg.matches(row) :
                return leg.calc(row)
        return self.default.calc(row)

    def to_python(self) :
        return f"if {self.legs[0].to_python()} : {self.legs[0].result.to_python()}"

    @property
    def name(self) :
        return "case"

@dataclass
class Between(ExprNode) :
    arg : ExprNode
    lower : ExprNode
    upper : ExprNode

    @property
    def name(self) :
        return "between"

    def calc(self, row : dict[str, Value]) -> Value :
        value = self.arg.calc(row)
        return Value(bool, (value >= self.lower.calc(row) and value <= self.upper.calc(row)))

    def to_python(self) :
        return f"v = {self.arg.to_python()}; v >={self.lower.to_python()} and v <={self.upper.to_python()}"
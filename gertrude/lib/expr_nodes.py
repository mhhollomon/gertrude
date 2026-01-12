from abc import ABC, abstractmethod
from dataclasses import dataclass
from .types.value import Value
from typing import Any, Callable, List


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
        return self.op(self.left.calc(row), self.right.calc(row))

    def to_python(self) :
        return f"({self.left.to_python()} {self.op.__name__} {self.right.to_python()})"

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

@dataclass
class MonoOperation(ExprNode) :
    op : Any
    arg : ExprNode

    def calc(self, row : dict[str, Value]) -> Value :
        return self.op(self.arg.calc(row))

    def to_python(self) :
        return f"{self.op.__name__}({self.arg.to_python()})"

    @property
    def name(self) :
        return self.op.__name__

class NVLOp(ExprNode) :
    args : List[ExprNode]

    def __init__(self, *args) :
        self.args = [*args]

    def calc(self, row : dict[str, Value]) -> Value :
        for arg in self.args :
            value = arg.calc(row)
            if not value.is_null :
                return value
        return Value(int, None)

    def to_python(self) :
        return self.args[0].to_python()

    @property
    def name(self) :
        return "nvl"

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
        return value >= self.lower.calc(row) and value <= self.upper.calc(row)

    def to_python(self) :
        # TODO - fix this
        return f"{self.arg.to_python()} in range({self.lower.to_python()}, {self.upper.to_python()}"
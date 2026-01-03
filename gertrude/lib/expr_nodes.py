from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable


class ExprNode(ABC):
    @abstractmethod
    def calc(self, row : dict[str, Any]) -> Any:
        ...

    @abstractmethod
    def to_python(self) :
        ...

    @property
    @abstractmethod
    def name(self) :
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

    def calc(self, row : dict[str, Any]) -> Any :
        return self.op(self.left.calc(row), self.right.calc(row))

    def to_python(self) :
        return f"({self.left.to_python()} {self.op.__name__} {self.right.to_python()})"

@dataclass
class ColumnName(ExprNode) :
    name_ : str

    def calc(self, row : dict[str, Any]) -> Any :
        try :
            return row[self.name_]
        except KeyError :
            raise KeyError(f"Column {self.name_} not found in row")

    def to_python(self) :
        return f"row['{self.name_}']"

    @property
    def name(self) :
        return self.name_

@dataclass
class Literal(ExprNode) :
    value : Any
    vtype : str

    def calc(self, row : dict[str, Any]) -> Any :
        return self.value

    def to_python(self) :
        return repr(self.value)

    @property
    def name(self) :
        return self.vtype

@dataclass
class MonoOperation(ExprNode) :
    op : Any
    arg : ExprNode

    def calc(self, row : dict[str, Any]) -> Any :
        return self.op(self.arg.calc(row))

    def to_python(self) :
        return f"{self.op.__name__}({self.arg.to_python()})"

    @property
    def name(self) :
        return self.op.__name__

@dataclass
class CaseLeg(ExprNode) :
    condition : ExprNode
    result : ExprNode

    def calc(self, row : dict[str, Any]) -> Any :
        return self.result.calc(row)

    def matches(self, row : dict[str, Any]) -> bool :
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

    def calc(self, row : dict[str, Any]) -> Any :
        for leg in self.legs :
            if leg.matches(row) :
                return leg.calc(row)
        return self.default.calc(row)

    def to_python(self) :
        return f"if {self.legs[0].to_python()} : {self.legs[0].result.to_python()}"

    @property
    def name(self) :
        return "case"
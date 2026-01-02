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


@dataclass
class Operation(ExprNode) :
    category : str
    op : Callable[[Any, Any], Any]
    left : ExprNode
    right : ExprNode

    def name(self) :
        return self.op.__name__

    def calc(self, row : dict[str, Any]) -> Any :
        return self.op(self.left.calc(row), self.right.calc(row))

    def to_python(self) :
        return f"({self.left.to_python()} {self.op.__name__} {self.right.to_python()})"

@dataclass
class ColumnName(ExprNode) :
    name : str

    def calc(self, row : dict[str, Any]) -> Any :
        try :
            return row[self.name]
        except KeyError :
            raise KeyError(f"Column {self.name} not found in row")

    def to_python(self) :
        return f"row['{self.name}']"

@dataclass
class Literal(ExprNode) :
    value : Any
    vtype : str

    def calc(self, row : dict[str, Any]) -> Any :
        return self.value

    def to_python(self) :
        return repr(self.value)

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

    def name(self) :
        return self.op.__name__
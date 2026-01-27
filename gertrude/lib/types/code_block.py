from contextlib import contextmanager
from typing import Any


class code_block :
    def __init__(self, indent : int = 0, code : 'str | code_block | None' = None) -> None:
        self.indent = indent
        self.code : list[code_block|str] = []
        if code is not None :
            self.code.append(code)

    def add(self, code : 'str | code_block') :
        self.code.append(code)

    def add_indent(self, indent_increment : int = 1) :
        self.indent += indent_increment

    @contextmanager
    def more_indent(self, indent_increment : int = 1) :
        new_block = code_block(indent=self.indent+indent_increment)
        yield new_block
        self.add(new_block)

    def print(self, file: Any, prefix : str = "", indent_str : str = '  ', level : int = 0) :
        for c in self.code :
            if isinstance(c, str) :
                print(prefix + indent_str * (self.indent + level)+ c, file=file)
            elif isinstance(c, code_block) :
                c.print(file, prefix, indent_str, level + self.indent)
            else :
                raise ValueError(f"Invalid code type {type(c)}")

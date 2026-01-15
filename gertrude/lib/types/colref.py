
class ColRef :
    __slots__ = ('name_', 'alias_')

    def __init__(self, name : str, alias : str = "") :
        self.name_ = name
        self.alias_ = alias

    def __hash__(self) -> int:
        return hash((self.name_, self.alias_))

    def matchedBy(self, other : 'ColRef') -> bool :
        if other.alias_ == "" :
            return self.name_ == other.name_

        return self.alias_ == other.alias_ and self.name_ == other.name_

    def matches(self, other : 'ColRef') -> bool :
        return self.matchedBy(other)

    def alias(self) -> str :
        return self.alias_

    def name(self) -> str :
        return self.name_

    def full_name(self) -> str :
        if self.alias_ == "" :
            return self.name_

        return f"{self.alias_}.{self.name_}"
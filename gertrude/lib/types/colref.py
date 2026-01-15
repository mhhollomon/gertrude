import logging
logger = logging.getLogger(__name__)

class ColRef :
    __slots__ = ('name_', 'alias_')

    def __init__(self, name : str, alias : str = "") :
        self.name_ = name
        self.alias_ = alias

    def __hash__(self) -> int:
        return hash((self.name_, self.alias_))

    def __eq__(self, other) :
        return self.full_name == other.full_name

    def __repr__(self) :
        return f"ColRef({self.name_}, {self.alias_})"

    def matchedBy(self, other : 'ColRef') -> bool :
        logger.debug(f"Matching {self} with {other}")
        if other.alias_ == "" :
            return self.name_ == other.name_

        return self.alias_ == other.alias_ and self.name_ == other.name_

    def matches(self, other : 'ColRef') -> bool :
        return other.matchedBy(self)

    @property
    def alias(self) -> str :
        return self.alias_

    @property
    def name(self) -> str :
        return self.name_

    @property
    def full_name(self) -> str :
        if self.alias_ == "" :
            return self.name_

        return f"{self.alias_}.{self.name_}"

    def __str__(self) :
        return self.full_name
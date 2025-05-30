from typing import ClassVar

class CustomType:
    __gel_type_name__: ClassVar[str]

class DateDuration(CustomType):
    pass

class RelativeDuration(CustomType):
    pass

class ConfigMemory(CustomType):
    pass

import pydantic


class A(pydantic.BaseModel):
    a: int


# reveal_type(A.a)
type(A.a)

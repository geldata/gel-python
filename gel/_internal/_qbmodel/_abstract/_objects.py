# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Base object types used to implement class-based query builders"""

from __future__ import annotations
from typing import TYPE_CHECKING, Any, ClassVar
from typing_extensions import Self

import weakref

from gel._internal import _qb
from gel._internal._utils import Unspecified

from ._base import GelType, GelTypeMeta

if TYPE_CHECKING:
    import uuid


class GelModelMeta(GelTypeMeta):
    __gel_class_registry__: ClassVar[
        weakref.WeakValueDictionary[uuid.UUID, type[Any]]
    ] = weakref.WeakValueDictionary()

    def __new__(  # noqa: PYI034
        mcls,
        name: str,
        bases: tuple[type[Any], ...],
        namespace: dict[str, Any],
        *,
        __gel_type_id__: uuid.UUID | None = None,
        **kwargs: Any,
    ) -> GelModelMeta:
        cls = super().__new__(mcls, name, bases, namespace, **kwargs)
        if __gel_type_id__ is not None:
            mcls.__gel_class_registry__[__gel_type_id__] = cls
        return cls

    @classmethod
    def get_class_by_id(cls, tid: uuid.UUID) -> type[GelModel]:
        try:
            return cls.__gel_class_registry__[tid]
        except KeyError:
            raise LookupError(
                f"cannot find GelModel for object type id {tid}"
            ) from None

    @classmethod
    def register_class(cls, tid: uuid.UUID, type_: type[GelModel]) -> None:
        cls.__gel_class_registry__[tid] = cls


class GelModel(
    GelType,
    metaclass=GelModelMeta,
):
    if TYPE_CHECKING:

        @classmethod
        def select(cls, /, **kwargs: bool | type[GelType]) -> type[Self]: ...
    else:

        @_qb.exprmethod
        @classmethod
        def select(
            cls,
            /,
            *elements: _qb.PathAlias,
            __operand__: _qb.ExprAlias | None = None,
            **kwargs: bool | type[GelType],
        ) -> type[Self]:
            shape = {}

            for elem in elements:
                path = _qb.edgeql_qb_expr(elem)
                if not isinstance(path, _qb.Path):
                    raise TypeError(f"{elem} is not a valid path expression")

                shape[path.name] = path

            for ptrname, kwarg in kwargs.items():
                if isinstance(kwarg, bool):
                    ptr = getattr(cls, ptrname, Unspecified)
                    if ptr is Unspecified and isinstance(kwarg, bool):
                        sn = cls.__gel_reflection__.name.as_schema_name()
                        msg = f"{ptrname} is not a valid {sn} property"
                        raise AttributeError(msg)
                    if not isinstance(ptr, _qb.PathAlias):
                        raise AssertionError(
                            f"expected {cls.__name__}.{ptrname} "
                            f"to be a PathAlias"
                        )

                    if kwarg:
                        ptr.__gel_metadata__.source = _qb.PathPrefix()
                        shape[ptrname] = ptr
                    else:
                        shape.pop(ptrname, None)
                else:
                    shape[ptrname] = kwarg

            operand = cls if __operand__ is None else __operand__
            expr = _qb.Shape(
                type_=cls.__gel_reflection__.name,
                expr=operand,
                elements=shape,
            )

            return _qb.AnnotatedExpr(  # type: ignore [return-value]
                cls,
                expr,
            )

    if TYPE_CHECKING:

        @classmethod
        def filter(
            cls,
            /,
            *exprs: Any,
            **properties: Any,
        ) -> type[Self]: ...
    else:

        @_qb.exprmethod
        @classmethod
        def filter(
            cls,
            /,
            *exprs: Any,
            __operand__: _qb.ExprAlias | None = None,
            **properties: Any,
        ) -> type[Self]:
            all_exprs = list(exprs)

            for propname, value in properties.items():
                prop = getattr(cls, propname, Unspecified)
                if prop is Unspecified:
                    sn = cls.__gel_reflection__.name.as_schema_name()
                    msg = f"{propname} is not a valid {sn} property"
                    raise AttributeError(msg)
                if not isinstance(prop, _qb.PathAlias):
                    raise AssertionError(
                        f"expected {cls.__name__}.{propname} to be a PathAlias"
                    )
                prop.__gel_metadata__.source = _qb.PathPrefix()
                prop_comp = prop == value
                if not isinstance(prop_comp, _qb.ExprAlias):
                    raise AssertionError(
                        f"comparing {prop} to {value} did not produce "
                        "a Gel expression type"
                    )
                all_exprs.append(prop_comp)

            operand = cls if __operand__ is None else __operand__
            if isinstance(operand, _qb.Filter):
                expr = _qb.Filter(
                    expr=operand.expr, filters=operand.filters + all_exprs
                )
            else:
                expr = _qb.Filter(expr=operand, filters=all_exprs)

            return _qb.AnnotatedExpr(  # type: ignore [return-value]
                cls,
                expr,
            )

    @classmethod
    def __edgeql_qb_expr__(cls) -> _qb.Expr:  # pyright: ignore [reportIncompatibleMethodOverride]
        return _qb.Shape(
            type_=cls.__gel_reflection__.name,
            expr=_qb.SchemaSet(name=cls.__gel_reflection__.name),
            star_splat=True,
        )

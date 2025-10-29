# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Abstract type definitions for the EdgeQL query builder"""

from __future__ import annotations
from typing import TYPE_CHECKING, Any, TypeVar, overload
from typing_extensions import Self

import abc
import collections
import contextlib
import itertools
import weakref
from dataclasses import dataclass, field

from gel._internal import _edgeql

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping
    from gel._internal._schemapath import TypeNameExpr


@dataclass(kw_only=True, frozen=True)
class Node(abc.ABC):
    # The symbol sets are at least a little confusing.
    # I think the meanings are:
    # symrefs: symbol references that are bound by the top of current node
    #          or somewhere external. Symbols bound *inside* a subnode
    #          don't seem to be included.
    # outside_refs: specifically references that *reach past* at least
    #               one binding site and aren't bound inside a subnode.
    # must_bind_refs: path prefixes used in an expression *bare* (without
    #                 projecting from them) that thus must be bound if used.
    #                 Symbols bound in subnodes not included.
    #
    # visible_refs: This is outside_refs for ScopedExprs and symrefs
    #               otherwise.
    symrefs: frozenset[Symbol] = field(init=False, compare=False)
    outside_refs: frozenset[Symbol] = field(init=False, compare=False)
    must_bind_refs: frozenset[Symbol] = field(init=False, compare=False)

    @property
    def visible_refs(self) -> frozenset[Symbol]:
        return self.symrefs

    @property
    def visible_must_bind_refs(self) -> frozenset[Symbol]:
        return self.must_bind_refs

    @abc.abstractmethod
    def subnodes(self) -> Iterable[Node | None]: ...

    def compute_symrefs(
        self, subnodes: Iterable[Node | None]
    ) -> Iterable[Symbol]:
        return itertools.chain.from_iterable(
            n.visible_refs for n in subnodes if n is not None
        )

    def compute_outside_refs(
        self, subnodes: Iterable[Node | None]
    ) -> Iterable[Symbol]:
        return itertools.chain.from_iterable(
            n.outside_refs for n in subnodes if n is not None
        )

    def compute_must_bind_refs(
        self, subnodes: Iterable[Node | None]
    ) -> Iterable[Symbol]:
        return itertools.chain.from_iterable(
            n.visible_must_bind_refs for n in subnodes if n is not None
        )

    def __post_init__(self) -> None:
        subnodes = list(self.subnodes())
        symrefs = frozenset(self.compute_symrefs(subnodes))
        object.__setattr__(self, "symrefs", symrefs)
        outside_refs = frozenset(self.compute_outside_refs(subnodes))
        object.__setattr__(self, "outside_refs", outside_refs)
        must_bind_refs = frozenset(self.compute_must_bind_refs(subnodes))
        object.__setattr__(self, "must_bind_refs", must_bind_refs)


@dataclass(kw_only=True, frozen=True)
class Expr(Node):
    @abc.abstractproperty
    def precedence(self) -> _edgeql.Precedence: ...

    @abc.abstractproperty
    def type(self) -> TypeNameExpr: ...

    @abc.abstractmethod
    def __edgeql_expr__(self, *, ctx: ScopeContext) -> str: ...

    def __edgeql_qb_expr__(self) -> Self:
        return self


@dataclass(kw_only=True, frozen=True)
class TypedExpr(Expr):
    type_: TypeNameExpr

    @property
    def type(self) -> TypeNameExpr:
        return self.type_


@dataclass(kw_only=True, frozen=True)
class QueryText(TypedExpr):
    text: str

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Operation.RAW]

    def subnodes(self) -> Iterable[Node | None]:
        return ()

    def __edgeql_expr__(self, *, ctx: ScopeContext) -> str:
        return self.text


class AtomicExpr(TypedExpr):
    pass


class IdentLikeExpr(AtomicExpr):
    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Token.IDENT]

    def subnodes(self) -> Iterable[Node]:
        return ()


@dataclass(kw_only=True, frozen=True)
class Symbol(IdentLikeExpr):
    scope: Scope
    binding_stem: str = field(init=False, compare=False)

    def __post_init__(self) -> None:
        super().__post_init__()
        object.__setattr__(self, "binding_stem", self.type_.name.lower())

    def compute_symrefs(
        self, subnodes: Iterable[Node | None]
    ) -> Iterable[Symbol]:
        return (self,)


@dataclass(kw_only=True, frozen=True)
class PathExpr(AtomicExpr):
    source: Expr
    name: str
    is_lprop: bool = False
    is_link: bool = False
    is_backlink: bool = False

    def subnodes(self) -> Iterable[Node]:
        return (self.source,)

    def compute_must_bind_refs(
        self, subnodes: Iterable[Node | None]
    ) -> Iterable[Symbol]:
        if isinstance(self.source, PathPrefix):
            return ()
        else:
            return self.source.visible_must_bind_refs

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Operation.PATH]


class Scope:
    stmt: weakref.ref[Stmt]

    def __init__(self, stmt: Stmt | None = None) -> None:
        if stmt is not None:
            self.stmt = weakref.ref(stmt)

    def __repr__(self) -> str:
        return f"<Scope at {id(self):0x}>"


_Bindings = collections.ChainMap[Symbol, str]
_Counters = collections.ChainMap[str, int]


class _Namespace:
    def __init__(
        self,
        bindings: _Bindings | None = None,
        names: list[set[str]] | None = None,
        counters: _Counters | None = None,
    ) -> None:
        self._bindings = _Bindings() if bindings is None else bindings
        self._names = [set()] if names is None else names
        self._counters = (
            _Counters(collections.defaultdict(int))
            if counters is None
            else counters
        )

    def new_child(self) -> Self:
        return self.__class__(
            self._bindings.new_child(),
            [set(), *self._names],
            self._counters.new_child(collections.defaultdict(int)),
        )

    @property
    def bindings(self) -> _Bindings:
        return self._bindings

    def bind(self, sym: Symbol, stem: str) -> str:
        if sym in self._bindings:
            raise RuntimeError(f"symbol {sym} is already bound")

        suf = self._counters[stem]
        name = stem if suf == 0 else f"{stem}_{suf}"

        while any(name in names for names in self._names):
            suf += 1
            name = f"{stem}_{suf}"

        self._counters[stem] = suf
        self._bindings[sym] = name
        self._names[0].add(name)

        return name


class ScopeContext:
    def __init__(self, scope: Scope) -> None:
        self._ns = _Namespace()
        self._scopes: dict[Scope, _Namespace] = {scope: self._ns}
        self._scope = scope
        self._path_scope: Scope | None = None
        self._path_prefix_must_bind: bool = False
        self._bindings: dict[str, object] = {}

    def has_scope(self, scope: Scope) -> bool:
        return scope in self._scopes

    def bind(self, sym: Symbol, stem: str | None = None) -> str:
        ns = self._scopes.get(sym.scope)
        if ns is None:
            raise RuntimeError(
                f"symbol {sym} is not found in current scope context"
            )

        if stem is None:
            stem = sym.binding_stem

        return ns.bind(sym, stem)

    def bind_argument(self, val: object) -> str:
        k = f"_qb_arg_{len(self._bindings)}"
        self._bindings[k] = val
        return k

    def get_arguments(self) -> dict[str, object]:
        return self._bindings

    @contextlib.contextmanager
    def push(
        self,
        scope: Scope,
        *,
        switch_path_scope: bool = False,
        path_prefix_must_bind: bool | None = None,
    ) -> Iterator[Self]:
        self._ns = self._ns.new_child()
        if scope in self._scopes:
            raise AssertionError(f"{scope} is already in the scope context")
        self._scopes[scope] = self._ns
        cur_path_scope = self._path_scope
        if switch_path_scope:
            self._path_scope = scope
        cur_path_prefix_must_bind = self._path_prefix_must_bind
        if path_prefix_must_bind is not None:
            self._path_prefix_must_bind = path_prefix_must_bind
        try:
            yield self
        finally:
            self._scope, self._ns = self._scopes.popitem()
            self._path_scope = cur_path_scope
            self._path_prefix_must_bind = cur_path_prefix_must_bind

    @property
    def scope(self) -> Scope:
        return self._scope

    @property
    def path_scope(self) -> Scope | None:
        return self._path_scope

    @property
    def path_prefix_must_bind(self) -> bool:
        return self._path_prefix_must_bind

    @property
    def bindings(self) -> Mapping[Symbol, str]:
        return self._ns.bindings

    def upper_scope_bindings(self) -> Iterator[tuple[Symbol, str]]:
        current_scope = self._scope
        for symbol, var in self.bindings.items():
            if symbol.scope is not current_scope:
                yield symbol, var


_T = TypeVar("_T")


class ScopeDescriptor:
    def __set_name__(self, owner: type[Any], name: str) -> None:
        self._name = "_" + name

    @overload
    def __get__(self, instance: None, owner: type[_T]) -> Self: ...

    @overload
    def __get__(
        self, instance: _T, owner: type[_T] | None = None
    ) -> Scope: ...

    def __get__(
        self,
        instance: object | None,
        owner: type[Any] | None = None,
    ) -> Scope | Self:
        if instance is None:
            return self
        else:
            scope = getattr(instance, self._name, None)
            if scope is None:
                stmt = instance if isinstance(instance, Stmt) else None
                scope = Scope(stmt=stmt)
                object.__setattr__(instance, self._name, scope)
            return scope

    def __set__(
        self,
        obj: Any,
        value: Scope,
    ) -> None:
        if isinstance(value, Scope):
            object.__setattr__(obj, self._name, value)


@dataclass(kw_only=True, frozen=True)
class ScopedExpr(Expr):
    scope: ScopeDescriptor = ScopeDescriptor()

    @property
    def visible_refs(self) -> frozenset[Symbol]:
        return self.outside_refs

    @property
    def visible_must_bind_refs(self) -> frozenset[Symbol]:
        return frozenset()

    def compute_outside_refs(
        self, subnodes: Iterable[Node | None]
    ) -> Iterable[Symbol]:
        return (ref for ref in self.symrefs if ref.scope is not self.scope)

    @contextlib.contextmanager
    def context(
        self,
        parent: ScopeContext | None = None,
    ) -> Iterator[ScopeContext]:
        if parent is None:
            yield ScopeContext(self.scope)
        else:
            with parent.push(self.scope) as ctx:
                yield ctx

    @abc.abstractmethod
    def _edgeql(self, ctx: ScopeContext) -> str: ...

    def __edgeql_expr__(self, *, ctx: ScopeContext) -> str:
        with self.context(parent=ctx) as myctx:
            return self._edgeql(myctx)


@dataclass(kw_only=True, frozen=True)
class IteratorExpr(ScopedExpr):
    iter_expr: Expr
    body_scope: ScopeDescriptor = ScopeDescriptor()
    self_ref: Symbol | None = field(init=False, compare=False)
    self_ref_must_bind: bool = field(init=False, compare=False)

    def __post_init__(self) -> None:
        super().__post_init__()
        self_ref = self.compute_self_ref()
        object.__setattr__(self, "self_ref", self_ref)
        if self_ref is not None:
            self_ref_must_bind = self_ref in self.must_bind_refs
            if not self_ref_must_bind:
                self_ref_must_bind = self.compute_self_ref_in_subscopes()
        else:
            self_ref_must_bind = False
        object.__setattr__(self, "self_ref_must_bind", self_ref_must_bind)

    def compute_outside_refs(
        self, subnodes: Iterable[Node | None]
    ) -> Iterable[Symbol]:
        return (
            ref
            for ref in self.symrefs
            if ref.scope is not self.scope and ref.scope is not self.body_scope
        )

    def compute_self_ref(self) -> Symbol | None:
        refs = {ref for ref in self.symrefs if ref.scope is self.body_scope}
        ref_count = len(refs)
        if ref_count > 1:
            raise RuntimeError("more than one self-ref in an expression")
        elif ref_count == 0:
            return None
        else:
            return next(iter(refs))

    def compute_self_ref_in_subscopes(self) -> bool:
        return any(
            ref.scope is self.body_scope
            for node in self.subnodes()
            if node is not None
            for ref in node.outside_refs
        )

    def _edgeql_parts(self, ctx: ScopeContext) -> tuple[str, str]:
        iterable = self._iteration_edgeql(ctx)

        if ctx.has_scope(self.body_scope):
            # SELECTs share scope with shapes, so make sure we don't
            # try to re-enter the same scope twice.
            body = self._body_edgeql(ctx)
        else:
            with ctx.push(
                self.body_scope,
                switch_path_scope=True,
                path_prefix_must_bind=self.self_ref_must_bind,
            ) as body_ctx:
                if self.self_ref is not None:
                    body_ctx.bind(self.self_ref)
                body = self._body_edgeql(body_ctx)

        return (iterable, body)

    def _edgeql(self, ctx: ScopeContext) -> str:
        iterable, body = self._edgeql_parts(ctx)
        return f"{iterable} {body}"

    @abc.abstractmethod
    def _iteration_edgeql(self, ctx: ScopeContext) -> str: ...

    @abc.abstractmethod
    def _body_edgeql(self, ctx: ScopeContext) -> str: ...


@dataclass(kw_only=True, frozen=True)
class Stmt(ScopedExpr):
    stmt: _edgeql.Token
    aliases: dict[str, Expr] = field(default_factory=dict)

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[self.stmt]


@dataclass(kw_only=True, frozen=True)
class PathPrefix(Symbol):
    source_link: str | None = None
    lprop_pivot: bool = False

    def __edgeql_expr__(self, *, ctx: ScopeContext) -> str:
        if (
            ctx is not None
            and (ctx.path_scope is not self.scope or ctx.path_prefix_must_bind)
            and (var := ctx.bindings.get(self)) is not None
        ):
            return var
        else:
            return ""

    def compute_must_bind_refs(
        self, subnodes: Iterable[Node | None]
    ) -> Iterable[Symbol]:
        return (self,)


@dataclass(frozen=True, kw_only=True)
class PathTypeIntersectionPrefix(IdentLikeExpr):
    type_filter: TypeNameExpr

    def __edgeql_expr__(self, *, ctx: ScopeContext) -> str:
        return f"[is {self.type_filter.as_quoted_schema_name()}]"


@dataclass(kw_only=True, frozen=True)
class ImplicitIteratorStmt(IteratorExpr, Stmt):
    """Base class for statements that are implicit iterators"""

    @property
    def type(self) -> TypeNameExpr:
        return self.iter_expr.type

    @property
    def source_link(self) -> str | None:
        ix = self.iter_expr
        if isinstance(ix, IteratorExpr):
            ix = ix.iter_expr
        if isinstance(ix, PathExpr) and isinstance(ix.source, PathPrefix):
            return ix.name
        else:
            return None

    @property
    def path_prefix(self) -> PathPrefix:
        prefix = getattr(self, "_path_prefix", None)
        if prefix is None:
            prefix = PathPrefix(
                type_=self.type,
                scope=self.body_scope,
                source_link=self.source_link,
            )
            object.__setattr__(self, "_path_prefix", prefix)  # noqa: PLC2801

        return prefix


class AbstractDescriptor:
    pass


class AbstractFieldDescriptor(AbstractDescriptor):
    def get(self, owner: type[Any], expr: Any | None = None) -> Any:
        raise NotImplementedError(f"{type(self)}.get")

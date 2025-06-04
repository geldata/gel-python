# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from __future__ import annotations

from typing import NamedTuple, final

import enum

from gel._internal._polyfills._strenum import StrEnum


_token_str_map: dict[str, Token] = {}


@final
class Token(StrEnum):
    ADDASSIGN = "+="
    AMPER = "&"
    ARROW = "->"
    ASSIGN = ":="
    AT = "@"
    CIRCUMFLEX = "^"
    COLON = ":"
    COMMA = ""
    DISTINCTFROM = "?!="
    DOT = "."
    DOTBW = ".<"
    DOUBLECOLON = "::"
    DOUBLEPLUS = "++"
    DOUBLEQMARK = "??"
    DOUBLESLASH = "//"
    DOUBLESTAR = "**"
    EQUALS = "="
    GREATEREQ = ">="
    LANGBRACKET = "<"
    LBRACE = "{"
    LBRACKET = "["
    LESSEQ = "<="
    LPAREN = "("
    MINUS = "-"
    NOTDISTINCTFROM = "?="
    NOTEQ = "!="
    PERCENT = "%"
    PIPE = "|"
    PLUS = "+"
    RANGBRACKET = ">"
    RBRACE = "}"
    RBRACKET = "]"
    REMASSIGN = "-="
    RPAREN = ")"
    SCONST = "SCONST"
    SEMICOLON = ";"
    SLASH = "/"
    STAR = "*"

    BCONST = "BCONST"
    FCONST = "FCONST"
    ICONST = "ICONST"
    NFCONST = "NFCONST"
    NICONST = "NICONST"
    IDENT = "IDENT"

    # Reserved keywords
    __SOURCE__ = "__SOURCE__"
    __SUBJECT__ = "__SUBJECT__"
    __TYPE__ = "__TYPE__"
    __STD__ = "__STD__"
    __EDGEDBSYS__ = "__EDGEDBSYS__"
    __EDGEDBTPL__ = "__EDGEDBTPL__"
    __NEW__ = "__NEW__"
    __OLD__ = "__OLD__"
    __SPECIFIED__ = "__SPECIFIED__"
    __DEFAULT__ = "__DEFAULT__"
    ADMINISTER = "ADMINISTER"
    ALTER = "ALTER"
    ANALYZE = "ANALYZE"
    AND = "AND"
    ANYTUPLE = "ANYTUPLE"
    ANYTYPE = "ANYTYPE"
    ANYOBJECT = "ANYOBJECT"
    BY = "BY"
    COMMIT = "COMMIT"
    CONFIGURE = "CONFIGURE"
    CREATE = "CREATE"
    DELETE = "DELETE"
    DESCRIBE = "DESCRIBE"
    DETACHED = "DETACHED"
    DISTINCT = "DISTINCT"
    DO = "DO"
    DROP = "DROP"
    ELSE = "ELSE"
    EXISTS = "EXISTS"
    EXTENDING = "EXTENDING"
    FALSE = "FALSE"
    FILTER = "FILTER"
    FOR = "FOR"
    GLOBAL = "GLOBAL"
    GROUP = "GROUP"
    IF = "IF"
    ILIKE = "ILIKE"
    IN = "IN"
    INSERT = "INSERT"
    INTROSPECT = "INTROSPECT"
    IS = "IS"
    LIKE = "LIKE"
    LIMIT = "LIMIT"
    MODULE = "MODULE"
    NOT = "NOT"
    OFFSET = "OFFSET"
    OPTIONAL = "OPTIONAL"
    OR = "OR"
    ROLLBACK = "ROLLBACK"
    SELECT = "SELECT"
    SET = "SET"
    SINGLE = "SINGLE"
    START = "START"
    TRUE = "TRUE"
    TYPEOF = "TYPEOF"
    UPDATE = "UPDATE"
    VARIADIC = "VARIADIC"
    WITH = "WITH"

    # Unreserved keywords
    ABORT = "ABORT"
    ABSTRACT = "ABSTRACT"
    ACCESS = "ACCESS"
    AFTER = "AFTER"
    ALIAS = "ALIAS"
    ALL = "ALL"
    ALLOW = "ALLOW"
    ANNOTATION = "ANNOTATION"
    APPLIED = "APPLIED"
    AS = "AS"
    ASC = "ASC"
    ASSIGNMENT = "ASSIGNMENT"
    BEFORE = "BEFORE"
    BLOBAL = "BLOBAL"
    BRANCH = "BRANCH"
    CARDINALITY = "CARDINALITY"
    CAST = "CAST"
    COMMITTED = "COMMITTED"
    CONFIG = "CONFIG"
    CONFLICT = "CONFLICT"
    CONSTRAINT = "CONSTRAINT"
    CUBE = "CUBE"
    CURRENT = "CURRENT"
    DATA = "DATA"
    DATABASE = "DATABASE"
    DDL = "DDL"
    DECLARE = "DECLARE"
    DEFAULT = "DEFAULT"
    DEFERRABLE = "DEFERRABLE"
    DEFERRED = "DEFERRED"
    DELEGATED = "DELEGATED"
    DENY = "DENY"
    DESC = "DESC"
    EACH = "EACH"
    EMPTY = "EMPTY"
    EXCEPT = "EXCEPT"
    EXPRESSION = "EXPRESSION"
    EXTENSION = "EXTENSION"
    FINAL = "FINAL"
    FIRST = "FIRST"
    FORCE = "FORCE"
    FROM = "FROM"
    FUNCTION = "FUNCTION"
    FUTURE = "FUTURE"
    IMPLICIT = "IMPLICIT"
    INDEX = "INDEX"
    INFIX = "INFIX"
    INHERITABLE = "INHERITABLE"
    INSTANCE = "INSTANCE"
    INTERSECT = "INTERSECT"
    INTO = "INTO"
    ISOLATION = "ISOLATION"
    JSON = "JSON"
    LAST = "LAST"
    LINK = "LINK"
    MIGRATION = "MIGRATION"
    MULTI = "MULTI"
    NAMED = "NAMED"
    OBJECT = "OBJECT"
    OF = "OF"
    ONLY = "ONLY"
    ONTO = "ONTO"
    OPERATOR = "OPERATOR"
    OPTIONALITY = "OPTIONALITY"
    ORDER = "ORDER"
    ORDER_BY = "ORDER BY"
    ORPHAN = "ORPHAN"
    OVERLOADED = "OVERLOADED"
    OWNED = "OWNED"
    PACKAGE = "PACKAGE"
    POLICY = "POLICY"
    POPULATE = "POPULATE"
    POSTFIX = "POSTFIX"
    PREFIX = "PREFIX"
    PROPERTY = "PROPERTY"
    PROPOSED = "PROPOSED"
    PSEUDO = "PSEUDO"
    READ = "READ"
    REJECT = "REJECT"
    RELEASE = "RELEASE"
    RENAME = "RENAME"
    REPEATABLE = "REPEATABLE"
    REQUIRED = "REQUIRED"
    RESET = "RESET"
    RESTRICT = "RESTRICT"
    REWRITE = "REWRITE"
    ROLE = "ROLE"
    ROLES = "ROLES"
    ROLLUP = "ROLLUP"
    SAVEPOINT = "SAVEPOINT"
    SCALAR = "SCALAR"
    SCHEMA = "SCHEMA"
    SDL = "SDL"
    SERIALIZABLE = "SERIALIZABLE"
    SESSION = "SESSION"
    SOURCE = "SOURCE"
    SUPERUSER = "SUPERUSER"
    SYSTEM = "SYSTEM"
    TARGET = "TARGET"
    TEMPLATE = "TEMPLATE"
    TERNARY = "TERNARY"
    TEXT = "TEXT"
    THEN = "THEN"
    TO = "TO"
    TRANSACTION = "TRANSACTION"
    TRIGGER = "TRIGGER"
    TYPE = "TYPE"
    UNION = "UNION"
    UNLESS = "UNLESS"
    USING = "USING"
    VERBOSE = "VERBOSE"
    VERSION = "VERSION"
    VIEW = "VIEW"
    WRITE = "WRITE"

    @classmethod
    def from_str(cls, s: str, /) -> Token:
        if not _token_str_map:
            _token_str_map.update({
                str(v): v for v in cls.__members__.values()
            })
        try:
            return _token_str_map[s]
        except KeyError:
            raise ValueError(f"{s!r} is not a valid Token") from None


class Assoc(enum.Enum):
    LEFT = enum.auto()
    RIGHT = enum.auto()
    NONASSOC = enum.auto()


class Operation(enum.Enum):
    TYPECAST = enum.auto()
    PARENEXPR = enum.auto()
    PATH = enum.auto()
    CALL = enum.auto()


class Precedence(NamedTuple):
    value: int
    assoc: Assoc


PRECEDENCE: dict[Token | tuple[Token, int] | Operation, Precedence] = {
    Token.SELECT: Precedence(-4, Assoc.RIGHT),
    Token.INSERT: Precedence(-4, Assoc.RIGHT),
    Token.UPDATE: Precedence(-4, Assoc.RIGHT),
    Token.DELETE: Precedence(-4, Assoc.RIGHT),
    Token.FOR: Precedence(-4, Assoc.RIGHT),
    Token.WITH: Precedence(-4, Assoc.RIGHT),
    Token.THEN: Precedence(-3, Assoc.NONASSOC),
    Token.ASC: Precedence(-3, Assoc.NONASSOC),
    Token.DESC: Precedence(-3, Assoc.NONASSOC),
    Token.ASSIGN: Precedence(-3, Assoc.NONASSOC),
    Token.ADDASSIGN: Precedence(-3, Assoc.NONASSOC),
    Token.REMASSIGN: Precedence(-3, Assoc.NONASSOC),
    Token.COMMA: Precedence(-2, Assoc.LEFT),
    Token.UNION: Precedence(-1, Assoc.LEFT),
    Token.EXCEPT: Precedence(-1, Assoc.LEFT),
    Token.INTERSECT: Precedence(0, Assoc.LEFT),
    Token.FILTER: Precedence(1, Assoc.NONASSOC),
    Token.ORDER_BY: Precedence(1, Assoc.NONASSOC),
    Token.LIMIT: Precedence(1, Assoc.NONASSOC),
    Token.OFFSET: Precedence(1, Assoc.NONASSOC),
    Token.IF: Precedence(2, Assoc.RIGHT),
    Token.ELSE: Precedence(2, Assoc.RIGHT),
    Token.OR: Precedence(3, Assoc.LEFT),
    Token.AND: Precedence(4, Assoc.LEFT),
    Token.NOT: Precedence(5, Assoc.RIGHT),
    Token.LIKE: Precedence(6, Assoc.NONASSOC),
    Token.ILIKE: Precedence(6, Assoc.NONASSOC),
    Token.IN: Precedence(7, Assoc.NONASSOC),
    Token.IDENT: Precedence(8, Assoc.NONASSOC),
    Token.DISTINCTFROM: Precedence(9, Assoc.NONASSOC),
    Token.EQUALS: Precedence(9, Assoc.NONASSOC),
    Token.GREATEREQ: Precedence(9, Assoc.NONASSOC),
    Token.LANGBRACKET: Precedence(9, Assoc.NONASSOC),
    Token.LESSEQ: Precedence(9, Assoc.NONASSOC),
    Token.NOTDISTINCTFROM: Precedence(9, Assoc.NONASSOC),
    Token.NOTEQ: Precedence(9, Assoc.NONASSOC),
    Token.RANGBRACKET: Precedence(9, Assoc.NONASSOC),
    Token.IS: Precedence(10, Assoc.NONASSOC),
    Token.PLUS: Precedence(11, Assoc.LEFT),
    Token.MINUS: Precedence(11, Assoc.LEFT),
    Token.DOUBLEPLUS: Precedence(11, Assoc.LEFT),
    Token.STAR: Precedence(12, Assoc.LEFT),
    Token.SLASH: Precedence(12, Assoc.LEFT),
    Token.DOUBLESLASH: Precedence(12, Assoc.LEFT),
    Token.PERCENT: Precedence(12, Assoc.LEFT),
    Token.DOUBLEQMARK: Precedence(13, Assoc.RIGHT),
    Token.TYPEOF: Precedence(14, Assoc.NONASSOC),
    Token.INTROSPECT: Precedence(15, Assoc.NONASSOC),
    Token.PIPE: Precedence(16, Assoc.LEFT),
    Token.AMPER: Precedence(17, Assoc.LEFT),
    (Token.MINUS, 1): Precedence(18, Assoc.RIGHT),
    Operation.PARENEXPR: Precedence(18, Assoc.NONASSOC),
    Token.EXISTS: Precedence(18, Assoc.RIGHT),
    Token.DISTINCT: Precedence(18, Assoc.RIGHT),
    Token.CIRCUMFLEX: Precedence(19, Assoc.RIGHT),
    Operation.TYPECAST: Precedence(20, Assoc.RIGHT),
    Token.LBRACE: Precedence(21, Assoc.LEFT),
    Token.RBRACE: Precedence(21, Assoc.LEFT),
    Token.LBRACKET: Precedence(22, Assoc.LEFT),
    Token.RBRACKET: Precedence(22, Assoc.LEFT),
    Token.LPAREN: Precedence(23, Assoc.LEFT),
    Token.RPAREN: Precedence(23, Assoc.LEFT),
    Operation.CALL: Precedence(23, Assoc.LEFT),
    Token.DOT: Precedence(24, Assoc.LEFT),
    Token.DOTBW: Precedence(24, Assoc.LEFT),
    Operation.PATH: Precedence(24, Assoc.LEFT),
    Token.DETACHED: Precedence(24, Assoc.RIGHT),
    Token.GLOBAL: Precedence(25, Assoc.RIGHT),
    Token.DOUBLECOLON: Precedence(26, Assoc.LEFT),
    Token.AT: Precedence(27, Assoc.LEFT),
    Token.REQUIRED: Precedence(28, Assoc.RIGHT),
    Token.OPTIONAL: Precedence(28, Assoc.RIGHT),
    Token.MULTI: Precedence(28, Assoc.RIGHT),
    Token.SINGLE: Precedence(28, Assoc.RIGHT),
}


def need_left_parens(
    prod_prec: Precedence,
    left_prec: Precedence,
) -> bool:
    self_prec = prod_prec.value
    self_assoc = prod_prec.assoc
    lprec_value = left_prec.value

    return lprec_value < self_prec or (
        lprec_value == self_prec and self_assoc is Assoc.LEFT
    )


def need_right_parens(
    prod_prec: Precedence,
    right_prec: Precedence,
) -> bool:
    self_prec = prod_prec.value
    self_assoc = prod_prec.assoc
    rprec_value = right_prec.value

    return rprec_value < self_prec or (
        rprec_value == self_prec and self_assoc is Assoc.RIGHT
    )

"""A function that attempts to analyze the context of a Python boolean
expression.

This implementation uses bytecode analysis to distinguish between different
boolean contexts.

BYTECODE ANALYSIS APPROACH:
===========================

The key insight is that different boolean contexts generate distinctive
bytecode patterns, particularly around the COPY opcode:

1. SHORT-CIRCUIT OPERATIONS (and/or):
    - Need to preserve the left operand value for potential return
    - Use COPY opcode before conditional jumps
    - Example: obj and True
        -> LOAD_FAST (obj)
        -> COPY              <- Preserves obj on stack
        -> POP_JUMP_IF_FALSE
        -> POP_TOP
        -> LOAD_CONST (True)

2. CONDITIONAL STATEMENTS (if/while/ternary):
    - Only need the truth value for control flow
    - Use direct conditional jumps without COPY
    - Example: if obj:
        -> LOAD_FAST (obj)
        -> POP_JUMP_IF_FALSE  <- No COPY needed

3. EXPLICIT BOOLEAN CONVERSION:
    - bool() calls are detected by checking co_names
    - 'not' operations use UNARY_NOT opcode

CACHE OPCODES CHALLENGE:
=======================

Python 3.11+ introduced CACHE opcodes for adaptive optimization:
- Store specialization data for frequently executed operations
- Can appear between any two "real" instructions
- Break pattern detection if not handled properly

Example problem:
    TO_BOOL              (convert to boolean)
    CACHE                (optimization slot)  ← Breaks pattern detection!
    POP_JUMP_IF_FALSE    (conditional jump)

Solution: _skip_cache_opcodes() method ensures we find the actual next
instruction by skipping over CACHE opcodes during bytecode analysis.

Real-world impact: Modern Python bytecode contains many CACHE opcodes:
- Method calls: ~12 CACHE opcodes
- Attribute access: ~9 CACHE opcodes
- Global access: ~4 CACHE opcodes
- Binary operations: ~1 CACHE opcode

PYTHON VERSION COMPATIBILITY:
============================

- Python 3.12+: Uses COPY + POP_JUMP_IF_FALSE/TRUE patterns
- Python 3.13+: Also uses TO_BOOL with similar COPY patterns
- Python 3.11+: Must handle CACHE opcodes in bytecode analysis
- Python 3.10: Basic detection
"""

from __future__ import annotations
from typing import TYPE_CHECKING

import dis
import enum

from gel._internal import _inspect_extras

if TYPE_CHECKING:
    import types


class BoolOp(enum.Enum):
    NOT = enum.auto()
    """not foo"""
    OR = enum.auto()
    """foo or bar"""
    AND = enum.auto()
    """foo and bar"""
    EXPLICIT_BOOL = enum.auto()
    """bool(foo)"""
    IMPLICIT_BOOL = enum.auto()
    """if foo/while foo"""
    UNKNOWN = enum.auto()
    """unknown boolean context"""


_AND_OPCODES = frozenset(
    ("JUMP_IF_FALSE_OR_POP", "POP_JUMP_IF_FALSE", "POP_JUMP_FORWARD_IF_FALSE")
)
_OR_OPCODES = frozenset(
    ("JUMP_IF_TRUE_OR_POP", "POP_JUMP_IF_TRUE", "POP_JUMP_FORWARD_IF_TRUE")
)
_RETURN_OPCODES = frozenset(("RETURN_VALUE", "RETURN_CONST"))

# Python 3.10-3.11 use dedicated opcodes for short-circuit operations
_AND_SHORTCIRCUIT_OPCODES = frozenset(("JUMP_IF_FALSE_OR_POP",))
_OR_SHORTCIRCUIT_OPCODES = frozenset(("JUMP_IF_TRUE_OR_POP",))

_dis_opname = dis.opname


def guess_bool_op(stack_offset: int = 1) -> BoolOp:
    """Prevent boolean conversion and provide appropriate error message.

    ANALYSIS FLOW:
    ==============

    This method analyzes the calling context's bytecode to determine
    the specific boolean operation being performed, allowing for
    contextually appropriate error messages.

    The analysis follows this priority order:

    1. BOOLEAN OPERATORS (highest priority):
        - Check for 'and', 'or', 'not' operations using bytecode patterns
        - Use COPY opcode as key distinguishing feature
        - Provide specific std.and_/std.or_/std.not_ guidance

    2. EXPLICIT BOOL() CALLS:
        - Detect bool() function calls via co_names inspection
        - Guide users to std.exists(...) for existence checks

    3. GENERAL CONTEXTS (fallback):
        - All other boolean conversion contexts (if, while, ternary, etc.)
        - Provide general std.if_(...) or std.exists(...) guidance

    This prioritized approach ensures that the most common and important
    boolean contexts (short-circuit operations) get precise error messages,
    while still handling all other cases gracefully.
    """
    with _inspect_extras.frame(stack_offset + 1) as frame:
        if frame is None:
            return BoolOp.UNKNOWN

        # Get bytecode information - single bounds check
        code = frame.f_code.co_code
        lasti = frame.f_lasti

        if lasti >= len(code):
            return BoolOp.UNKNOWN

        # Get the current opcode - cache _dis_opname lookup
        opcode = code[lasti]
        opname = _dis_opname[opcode]

        # CORE STRATEGY: Use COPY opcode as distinguishing feature
        # =====================================================
        #
        # The COPY opcode is the key insight that makes precise context
        # detection possible. It reveals the semantic difference between:
        #
        # - Short-circuit operations (and/or): Need to preserve values
        #   -> Use COPY before conditional jumps
        # - Simple conditionals (if/while): Only need truth values
        #   -> Use direct conditional jumps without COPY
        #
        # This allows us to provide contextually appropriate error messages
        # instead of generic "boolean conversion not supported" errors.

        # Check for boolean operators - these are the most important to catch
        op = _check_boolean_operators(opname, code, lasti)
        if op is not None:
            return op

        # Check for explicit bool conversion (bool() calls)
        if _is_bool_call(frame.f_code):
            return BoolOp.EXPLICIT_BOOL

        # Other: assume implicit bool conversion
        return BoolOp.IMPLICIT_BOOL


def _check_boolean_operators(
    opname: str, code: bytes, lasti: int
) -> BoolOp | None:
    """Check for boolean operators.

    This method focuses on the most common and important boolean operators
    that we definitely want to catch, using stable opcode patterns.

    The key insight is that different boolean contexts generate different
    bytecode patterns, allowing us to distinguish between:
    - Short-circuit operations (and/or) that need to preserve values
    - Conditional statements (if/while) that only need truth values
    """
    # Check for 'not' operator - most reliable across versions
    if opname == "UNARY_NOT":
        return BoolOp.NOT

    # Python 3.10-3.11: Use dedicated opcodes for short-circuit operations
    if opname in _AND_SHORTCIRCUIT_OPCODES:
        return BoolOp.AND

    if opname in _OR_SHORTCIRCUIT_OPCODES:
        return BoolOp.OR

    # Python 3.12+: Check for 'and' and 'or' operators using COPY patterns
    if opname in _AND_OPCODES:
        # Distinguish between 'and' and 'if' contexts using COPY opcode
        # 'and' operations: obj and True -> COPY + POP_JUMP_IF_FALSE
        # 'if' statements: if obj: -> direct POP_JUMP_IF_FALSE (no COPY)
        if _has_preceding_copy(code, lasti):
            return BoolOp.AND
        else:
            # For Python 3.10-3.11, check if this is part of a nested
            # expression by looking for short-circuit opcodes later
            # in the bytecode
            if _is_nested_bool_expr(code, lasti, _OR_SHORTCIRCUIT_OPCODES):
                return BoolOp.AND
            else:
                return BoolOp.IMPLICIT_BOOL

    if opname in _OR_OPCODES:
        # Distinguish between 'or' and other contexts using COPY opcode
        # 'or' operations: obj or False -> COPY + POP_JUMP_IF_TRUE
        # Other contexts use direct POP_JUMP_IF_TRUE (no COPY)
        if _has_preceding_copy(code, lasti):
            return BoolOp.OR
        else:
            # For Python 3.10-3.11, check if this is part of a nested
            # expression by looking for short-circuit opcodes later in
            # the bytecode
            if _is_nested_bool_expr(code, lasti, _AND_SHORTCIRCUIT_OPCODES):
                return BoolOp.OR
            else:
                return BoolOp.IMPLICIT_BOOL

    # Python 3.13+ uses TO_BOOL with different patterns
    # TO_BOOL explicitly converts to boolean, but we still need to
    # distinguish between short-circuit operations and conditionals
    if opname == "TO_BOOL":
        return _check_to_bool_context(code, lasti)

    return None


def _check_to_bool_context(code: bytes, lasti: int) -> BoolOp | None:
    """Check TO_BOOL context for Python 3.13+.

    Python 3.13+ introduced TO_BOOL as an explicit boolean conversion
    opcode, but we still need to distinguish between different contexts:
    - Single not: TO_BOOL followed by UNARY_NOT
    - Double not: TO_BOOL NOT followed by UNARY_NOT (optimized away)
    - Short-circuit operations: COPY + TO_BOOL + conditional jump
    - Simple conditionals: TO_BOOL + conditional jump

    This method uses improved heuristics for TO_BOOL contexts.
    """
    # Look at the next instruction to determine context
    # Critical: Skip CACHE opcodes to find the actual next instruction
    next_offset = _skip_cache_opcodes(code, lasti + 1)
    if next_offset >= len(code):
        return None

    next_opname = _dis_opname[code[next_offset]]

    # Check for single 'not' operation
    if next_opname == "UNARY_NOT":
        return BoolOp.NOT

    # Check for conditional jumps (if/while/ternary statements)
    if next_opname in {"POP_JUMP_IF_FALSE", "POP_JUMP_IF_TRUE"}:
        # Check if this is a short-circuit operation (has preceding COPY)
        if _has_preceding_copy(code, lasti):
            if next_opname == "POP_JUMP_IF_FALSE":
                return BoolOp.AND
            else:
                return BoolOp.OR
        else:
            # Simple conditional statement
            return BoolOp.IMPLICIT_BOOL

    # If TO_BOOL is not followed by UNARY_NOT or conditional jumps,
    # it's likely a double not operation that was optimized away
    # In Python 3.13+, 'not not obj' becomes just 'TO_BOOL'
    return BoolOp.NOT


def _has_preceding_copy(code: bytes, lasti: int) -> bool:
    """Check if there's a COPY instruction before the current position.

    This unified method replaces the separate _is_likely_and_context and
    _is_likely_or_context methods since they had identical logic.

    Key insight: The COPY opcode is the distinguishing feature for Python 3.12+

    Short-circuit operations (and/or):
        obj and True / obj or False
        -> LOAD_FAST (obj)
        -> COPY           ← Preserves obj value on stack
        -> POP_JUMP_IF_FALSE/TRUE
        -> POP_TOP
        -> LOAD_CONST (operand)

    Conditional statements (if/while/ternary):
        if obj: / while obj: / "yes" if obj else "no"
        -> LOAD_FAST (obj)
        -> POP_JUMP_IF_FALSE/TRUE  ← No COPY needed, just truth value

    The COPY is necessary because short-circuit operations might return
    the left operand, while conditionals only need the truth value.

    Note: This method is only used for Python 3.12+ patterns. Python 3.10-3.11
    use dedicated opcodes (JUMP_IF_FALSE_OR_POP, JUMP_IF_TRUE_OR_POP) that
    are handled separately.
    """
    # For Python 3.12+, short-circuit operations have a COPY instruction
    if lasti >= 2:
        prev_op = code[lasti - 2]
        return _dis_opname[prev_op] == "COPY"
    return False  # No COPY found


def _skip_cache_opcodes(code: bytes, start_offset: int) -> int:
    """Skip CACHE opcodes and return the next meaningful instruction offset.

    CRITICAL IMPORTANCE:
    ===================

    This method is essential for accurate bytecode analysis because CACHE
    opcodes (introduced in Python 3.11) can appear between any two "real"
    instructions, breaking pattern detection logic.

    CACHE opcodes are runtime optimization slots that:
    - Store specialization data for frequently executed operations
    - Enable adaptive bytecode optimization
    - Are "transparent" to program logic but visible in raw bytecode
    - Can appear in large numbers (up to 12+ per operation)

    Example problem without this method:
        TO_BOOL              (convert to boolean)
        CACHE                (optimization slot)  ← Breaks pattern detection!
        CACHE                (another slot)       ← More interference!
        POP_JUMP_IF_FALSE    (conditional jump)  ← Never found!

    With this method:
        TO_BOOL              (convert to boolean)
        CACHE                (skipped)
        CACHE                (skipped)
        POP_JUMP_IF_FALSE    (found correctly!)

    This enables the sophisticated COPY opcode detection that distinguishes
    between short-circuit operations (and/or) and conditional statements
    (if/while), allowing for contextually appropriate error messages.

    Without this method, NoBool would provide generic error messages
    instead of the precise guidance users need.
    """
    offset = start_offset
    while offset < len(code) and _dis_opname[code[offset]] == "CACHE":
        offset += 1
    return offset


def _is_bool_call(f_code: types.CodeType) -> bool:
    """Simplified bool() call detection.

    This uses a simple heuristic: if 'bool' is in the code's names,
    it's likely a bool() call. This is much simpler than complex
    bytecode analysis and works well in practice.

    bool() call bytecode:
        bool(obj)
        -> LOAD_GLOBAL (bool)
        -> LOAD_FAST (obj)
        -> CALL

    The presence of 'bool' in co_names is a reliable indicator.
    """
    co_names = f_code.co_names
    return co_names is not None and "bool" in co_names


def _is_nested_bool_expr(
    code: bytes,
    lasti: int,
    opcodes: frozenset[str],
    max_lookahead: int = 10,
) -> bool:
    """Check if this is part of a nested AND/OR expression for
    Python 3.10-3.11.

    In nested expressions like (obj and True) or False, the first part
    compiles to POP_JUMP_IF_FALSE followed by JUMP_IF_TRUE_OR_POP.
    This pattern indicates the POP_JUMP_IF_FALSE is actually part of
    an AND operation within parentheses.
    """
    # Look ahead to see if there's a short-circuit OR opcode
    # This indicates we're in a pattern like (obj and True) or False
    offset = lasti + 1
    instruction_count = 0

    while offset < len(code) and instruction_count < max_lookahead:
        # Skip CACHE opcodes (Python 3.11+)
        offset = _skip_cache_opcodes(code, offset)
        if offset >= len(code):
            break

        opname = _dis_opname[code[offset]]

        # If we find a short-circuit OR opcode, this indicates we're in
        # a nested expression where the AND is within parentheses
        if opname in opcodes:
            return True

        # If we hit a return or other terminating instruction, stop looking
        if opname in _RETURN_OPCODES:
            break

        offset += 1
        instruction_count += 1

    return False

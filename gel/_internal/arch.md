# gel-python Architecture & Onboarding Guide

## Overview

Gel Python (referred to as "gel-python" internally) is a comprehensive Python client library for Gel that provides a fully type-safe API with query builder capabilities, ORM features, and seamless integration with Pydantic models. This document serves as an onboarding guide for engineers contributing to the project.

## Project Structure

The core code is organized with all non-public API code under `gel/_internal/` with module names prefixed with underscores to clearly indicate internal implementation:

```
gel/
├── _internal/
│   ├── _qb/                    # Query builder AST and code generation
│   ├── _qbmodel/               # Query builder model implementation
│   │   ├── _abstract/          # Abstract model layer
│   │   └── _pydantic/          # Pydantic-specific bindings
│   ├── _reflection/            # Schema reflection system
│   ├── _codegen/               # Code generation for reflected schemas
│   │   └── _models/            # Model generation
│   │       └── _pydantic.py    # Main generator (~6000 lines)
│   ├── _save.py                # Save/persistence implementation
│   ├── _tracked_list.py        # Multi property tracking for changes
│   └── _link_set.py            # Multi links & multi links with props
```

## Core Components

### 1. Query Builder (`_qb/`)

The query builder is implemented as an AST (Abstract Syntax Tree) with self-contained code generation:

- **`_abstract.py`**: Base query builder expressions (AST nodes)
- **`_expressions.py`**: Expression types and operations
- **`_generics.py`**: Custom implementation of Python's `Annotated` for type metadata
- **`_protocols.py`**: Two key protocols:
  - `__edgeql_qbexpr__`: Classes implementing this return AST nodes
  - `__edgeql_expr__`: Code generation protocol returning EdgeQL strings

Key insight: Query builder nodes implement their own code generation, making them self-contained units that produce EdgeQL.

### 2. Model System (`_qbmodel/`)

The model system has two layers:

#### Abstract Layer (`_abstract/`)
- **Platform-agnostic** implementation of query builder methods
- Defines `GelModel` base class for object types
- Implements descriptors for property/link access patterns

#### Pydantic Layer (`_pydantic/`)
- Contains necessary workarounds to make Pydantic work with database models
- Handles partial data loading (database models may have missing required fields)
- Implements custom validation and JSON schema generation
- Makes link properties fit Pydantic & Python object model

### 3. Code Generation (`_codegen/_models/_pydantic.py`)

This file is responsible for:

- Generating type-safe Python models from Gel schema
- Creating overloads for generic functions
- Managing imports and module structure
- Handling complex type mappings (Gel → Python)

Key challenges:
- **Function overloads**: Must generate specific overloads for each type to maintain type safety
- **Implicit casting**: Gel's implicit casts must be carefully ordered to avoid MyPy overlap errors
- **Type checking**: Implements rudimentary type checker for callable types

### 4. Save Implementation (`_save.py`)

The save system traverses object graphs and generates EdgeQL mutations:

1. **`make_plan()`**: Analyzes objects and creates a delta tree of changes
2. **Change nodes**: Each operation (property change, link addition, etc.) has a corresponding node type
3. **Batching**: Groups similar operations for efficiency (up to 100 per batch)
4. **Transaction handling**: The generation is structured in a way to allow transactional execution of save queries

### 5. Model Classes (`_qbmodel/_pydantic/_models.py`)

The model hierarchy:

```python
GelSourceModel          # Base Pydantic wrapper with change tracking
    ├── GelModel        # Handles objects
    ├── GelLinkModel    # Handles link properties
    └── ProxyModel      # The nightmare - wraps objects with link properties
```

**ProxyModel is the most complex part** - it's technically a Pydantic model but doesn't behave like one, routing attributes to wrapped objects and link properties dynamically.

## Link Properties: The Complexity Multiplier

Link properties are attributes on relationships (e.g., a "friendship" link with a "since_date" property). They complicate everything:

- **No native Python concept** for properties on properties
- **ProxyModel hack**: Wraps objects to add link property support
- **Type safety challenges**: Must maintain transparency when link properties are added
- **Collection complexity**: Multi-links with properties require custom collection implementations

Without link properties, the codebase would be **3x simpler** (save.py would be 3x shorter, queries 10x smaller).

## Testing Infrastructure

### Type-Safe Testing

Tests use a custom `@tb.typecheck` decorator that:
1. Extracts test code into separate files
2. Runs MyPy on each test individually
3. Supports `assertEqual(reveal_type(), ...)` to ensure correct type inference
4. Most tests are in `tests/test_model_generator.py`, QB tests are in `tests/test_qb.py`


### Test Models Generation

Run `python tools/gen_models.py` to generate test models into your virtual environment's site-packages. This enables IDE support for test development.

## Development Setup

### Prerequisites

1. gel development VM with gel server binary in PATH
2. Install with: `pip install -e .`
3. **Critical**: Edit `.pth` files because editable wheel install is broken:

   ```bash
   python -c 'import pathlib, gel; print(pathlib.Path(gel.__path__[0]).parent)' > \
     $(python -c 'import site; print(site.getsitepackages()[0])')/gel.pth
   ```

4. Set environment variables for gel server path if needed

   * If you want to use the dev server in your dev Gel environment:

     ```
     export GEL_SERVER_BINARY=<your-server-venv>/bin/gel-server
     ```

     then run `pytest` with `$ env __EDGEDB_DEVMODE=1 pytest`

   * or

     ```
     export GEL_SERVER_BINARY=$(gel server info --bin-path --version '6')
     ```

     and you should be able to just run `$ pytest`.


### Setup gotchas

#### Using a system-wide `pytest` install

Errors like `ModuleNotFoundError: No module named typing_inspection’` can
occur when using a system wide pytest installation.

This can be caused by having run either `pip install pytest` outside a venv
or `apt install python3-pytest`.

If you want to keep these installations, you can still run tests locally by
running `python -m pytest`.

#### `make clean && make` to fix binary incompatibilities

Errors such as:
```bash
gel.protocol.protocol.ExecuteContext size changed, may indicate binary
incompatibility. Expected 152 from C header, got 184 from PyObject
```

Indicate that a `pyx` file has changed. A clean rebuild is necessary.


#### Fix `site-packages/gel.pth` to resolve wall of mypy test errors

A large number of mypy errors such as:
```
RuntimeError: mypy check failed for test_modelgen_operators_integer_arithmetic 

test code:
...

mypy stdout:
models/__shapes__/std/net/__init__.py:14: error: Cannot find implementation or library stub for module named "gel.models.pydantic"  [import-not-found]
models/__shapes__/std/net/__init__.py:17: error: Class cannot subclass "AnyEnum" (has type "Any")  [misc]
models/__shapes__/std/net/__init__.py:22: error: Class cannot subclass "AnyEnum" (has type "Any")  [misc]
models/__shapes__/std/enc.py:14: error: Cannot find implementation or library stub for module named "gel.models.pydantic"  [import-not-found]
models/__shapes__/std/enc.py:17: error: Class cannot subclass "AnyEnum" (has type "Any")  [misc]
models/__shapes__/sys/__init__.py:20: error: Cannot find implementation or library stub for module named "gel.models.pydantic"  [import-not-found]
models/__shapes__/sys/__init__.py:53: error: Class cannot subclass "AnyEnum" (has type "Any")  [misc]
models/__shapes__/sys/__init__.py:60: error: Class cannot subclass "AnyEnum" (has type "Any")  [misc]
models/__shapes__/sys/__init__.py:65: error: Class cannot subclass "AnyEnum" (has type "Any")  [misc]
models/__shapes__/sys/__init__.py:70: error: Class cannot subclass "AnyEnum" (has type "Any")  [misc]
models/__shapes__/sys/__init__.py:75: error: Class cannot subclass "AnyEnum" (has type "Any")  [misc]
models/__shapes__/sys/__init__.py:80: error: Class cannot subclass "AnyEnum" (has type "Any")  [misc]
```

Indicates the `site-packages/gel.pth` file is not set up correctly.
Possible causes include:
- missing the prerequisite step
- changing the project directory name
- etc.

For a better understanding of why this error occurs, look at in `_testbase.py`
for the functions `BaseModelTestCase.setUpClass()` and `_typecheck`. A test
class annotated with `@tb.typecheck` will:
- set up a temp directory with the pydantic model.
- create a copy of test function in a dummy class
- run mypy on this file in a subprocess
- check the result code


#### Other errors

Some other errors that are caused by a weird environment, but more details are
needed. If you see one of these, please note the steps used to fix them!

```bash
Traceback (most recent call last):
  File "<string>", line 1, in <module>
  File "/home/dnwpark/work/dev-3.12/edgedb-python/gel/__init__.py", line 32,
  in <module>
    from gel.datatypes.datatypes import Record, Set, Object, Array
ImportError: cannot import name 'Record' from 'gel.datatypes.datatypes'
```

### Running Tests

```bash
# Basic test run
pytest tests/test_qb.py

# Parallel execution (requires pytest-xdist)
pytest -n 5  # Warning: High RAM usage, each process starts own DB

# Run failing tests first
pytest --ff

# Run specific tests
pytest -k "test_name or other_test"
```

## Key Technical Decisions

### Equality and Hashing

- Objects with same ID are equal regardless of data differences
- New objects (no ID) are only equal to themselves
- Link properties are ignored in equality comparisons
- Objects with IDs are hashable; new objects are not

### Descriptor Magic

The system heavily uses Python descriptors for the query builder:

- Accessing `User.friends` on the class returns a query builder path
- Accessing `user.friends` on an instance returns actual data
- This duality enables intuitive API: `User.friends.name` for queries

### Type System Integration

- Returns proper Pydantic models from queries
- Codec layer adapted to accept return types from query builder
- Type inference works through the entire pipeline

## Common Pitfalls and Gotchas

1. **UUID Performance**: Type IDs use integers instead of UUIDs due to Python's slow UUID constructor
2. **Type IDs Issue**: Currently rely on database-specific IDs (needs fixing to use type names)
3. **Pydantic Validation**: Custom validation pipeline through Rust layer requires careful schema generation
4. **Stack Inspection**: Used to work around Pydantic limitations - fragile but necessary
5. **Collection Tracking**: Custom collections track changes for save operations

## Where to Start Contributing

### Easier Areas
- Query builder bugs (once familiar with the system)
- Test coverage improvements
- Documentation and comments

### Medium Complexity
- Code generation improvements
- Save compiler refactoring (currently template-based, needs proper compiler)
- Performance optimizations

### High Complexity
- Link property handling
- Pydantic integration layer
- Type system mapping

## Important Files to Understand

1. **`_codegen/_models/_pydantic.py`**: Main code generator
2. **`_qbmodel/_pydantic/_models.py`**: Model implementations with all the hacks
3. **`_save.py`**: Persistence layer
4. **`_qbmodel/_pydantic/_fields.py`**: Field type definitions
5. **`_tracked_list.py` & link collections**: Change tracking implementations

The complexity comes from:
- Link properties (the #1 complexity source)
- Working around Pydantic limitations
- Ensuring complete type safety
- Gel-specific edge cases

Remember: When stuck, ask in Slack. The team is responsive and the learning curve, while steep initially, becomes manageable once you understand the core patterns.

import abc
import enum
import logging
import random
import typing
import sys
from collections import namedtuple

from . import errors


logger = logging.getLogger('gel')


_RetryRule = namedtuple("_RetryRule", ["attempts", "backoff"])
TAG_NAME = "tag"


def default_backoff(attempt):
    return (2 ** attempt) * 0.1 + random.randrange(100) * 0.001


WarningHandler = typing.Callable[
    [typing.Tuple[errors.EdgeDBError, ...], typing.Any],
    typing.Any,
]


def raise_warnings(warnings, res):
    if (
        len(warnings) > 1
        and sys.version_info >= (3, 11)
    ):
        raise ExceptionGroup(  # noqa
            "Query produced warnings", warnings
        )
    else:
        raise warnings[0]


def log_warnings(warnings, res):
    for w in warnings:
        logger.warning("EdgeDB warning: %s", str(w))
    return res


class RetryCondition:
    """Specific condition to retry on for fine-grained control"""
    TransactionConflict = enum.auto()
    NetworkError = enum.auto()


class IsolationLevel:
    """Isolation level for transaction"""
    Serializable = "SERIALIZABLE"
    RepeatableRead = "REPEATABLE READ"


class RetryOptions:
    """An immutable class that contains rules for `transaction()`"""
    __slots__ = ['_default', '_overrides']

    def __init__(self, attempts: int, backoff=default_backoff):
        self._default = _RetryRule(attempts, backoff)
        self._overrides = None

    def with_rule(self, condition, attempts=None, backoff=None):
        default = self._default
        overrides = self._overrides
        if overrides is None:
            overrides = {}
        else:
            overrides = overrides.copy()
        overrides[condition] = _RetryRule(
            default.attempts if attempts is None else attempts,
            default.backoff if backoff is None else backoff,
        )
        result = RetryOptions.__new__(RetryOptions)
        result._default = default
        result._overrides = overrides
        return result

    @classmethod
    def defaults(cls):
        return cls(
            attempts=3,
            backoff=default_backoff,
        )

    def get_rule_for_exception(self, exception):
        default = self._default
        overrides = self._overrides
        res = default
        if overrides:
            if isinstance(exception, errors.TransactionConflictError):
                res = overrides.get(RetryCondition.TransactionConflict, res)
            elif isinstance(exception, errors.ClientError):
                res = overrides.get(RetryCondition.NetworkError, res)
        return res


class TransactionOptions:
    """Options for `transaction()`"""
    __slots__ = ['_isolation', '_readonly', '_deferrable']

    def __init__(
        self,
        isolation: IsolationLevel=IsolationLevel.Serializable,
        readonly: bool = False,
        deferrable: bool = False,
    ):
        self._isolation = isolation
        self._readonly = readonly
        self._deferrable = deferrable

    @classmethod
    def defaults(cls):
        return cls()

    def start_transaction_query(self):
        isolation = str(self._isolation)
        if self._readonly:
            mode = 'READ ONLY'
        else:
            mode = 'READ WRITE'

        if self._deferrable:
            defer = 'DEFERRABLE'
        else:
            defer = 'NOT DEFERRABLE'

        return f'START TRANSACTION ISOLATION {isolation}, {mode}, {defer};'

    def __repr__(self):
        return (
            f'<{self.__class__.__name__} '
            f'isolation:{self._isolation}, '
            f'readonly:{self._readonly}, '
            f'deferrable:{self._deferrable}>'
        )


class State:
    __slots__ = ['_module', '_aliases', '_config', '_globals']

    def __init__(
        self,
        default_module: typing.Optional[str] = None,
        module_aliases: typing.Mapping[str, str] = None,
        config: typing.Mapping[str, typing.Any] = None,
        globals_: typing.Mapping[str, typing.Any] = None,
    ):
        self._module = default_module
        self._aliases = {} if module_aliases is None else dict(module_aliases)
        self._config = {} if config is None else dict(config)
        self._globals = (
            {} if globals_ is None else self.with_globals(globals_)._globals
        )

    @classmethod
    def _new(cls, default_module, module_aliases, config, globals_):
        rv = cls.__new__(cls)
        rv._module = default_module
        rv._aliases = module_aliases
        rv._config = config
        rv._globals = globals_
        return rv

    @classmethod
    def defaults(cls):
        return cls()

    def with_default_module(self, module: typing.Optional[str] = None):
        return self._new(
            default_module=module,
            module_aliases=self._aliases,
            config=self._config,
            globals_=self._globals,
        )

    def with_module_aliases(self, *args, **aliases):
        if len(args) > 1:
            raise errors.InvalidArgumentError(
                "with_module_aliases() takes from 0 to 1 positional arguments "
                "but {} were given".format(len(args))
            )
        aliases_dict = args[0] if args else {}
        aliases_dict.update(aliases)
        new_aliases = self._aliases.copy()
        new_aliases.update(aliases_dict)
        return self._new(
            default_module=self._module,
            module_aliases=new_aliases,
            config=self._config,
            globals_=self._globals,
        )

    def with_config(self, *args, **config):
        if len(args) > 1:
            raise errors.InvalidArgumentError(
                "with_config() takes from 0 to 1 positional arguments "
                "but {} were given".format(len(args))
            )
        config_dict = args[0] if args else {}
        config_dict.update(config)
        new_config = self._config.copy()
        new_config.update(config_dict)
        return self._new(
            default_module=self._module,
            module_aliases=self._aliases,
            config=new_config,
            globals_=self._globals,
        )

    def resolve(self, name: str) -> str:
        parts = name.split("::", 1)
        if len(parts) == 1:
            return f"{self._module or 'default'}::{name}"
        elif len(parts) == 2:
            mod, name = parts
            mod = self._aliases.get(mod, mod)
            return f"{mod}::{name}"
        else:
            raise AssertionError('broken split')

    def with_globals(self, *args, **globals_):
        if len(args) > 1:
            raise errors.InvalidArgumentError(
                "with_globals() takes from 0 to 1 positional arguments "
                "but {} were given".format(len(args))
            )
        new_globals = self._globals.copy()
        if args:
            for k, v in args[0].items():
                new_globals[self.resolve(k)] = v
        for k, v in globals_.items():
            new_globals[self.resolve(k)] = v
        return self._new(
            default_module=self._module,
            module_aliases=self._aliases,
            config=self._config,
            globals_=new_globals,
        )

    def without_module_aliases(self, *aliases):
        if not aliases:
            new_aliases = {}
        else:
            new_aliases = self._aliases.copy()
            for alias in aliases:
                new_aliases.pop(alias, None)
        return self._new(
            default_module=self._module,
            module_aliases=new_aliases,
            config=self._config,
            globals_=self._globals,
        )

    def without_config(self, *config_names):
        if not config_names:
            new_config = {}
        else:
            new_config = self._config.copy()
            for name in config_names:
                new_config.pop(name, None)
        return self._new(
            default_module=self._module,
            module_aliases=self._aliases,
            config=new_config,
            globals_=self._globals,
        )

    def without_globals(self, *global_names):
        if not global_names:
            new_globals = {}
        else:
            new_globals = self._globals.copy()
            for name in global_names:
                new_globals.pop(self.resolve(name), None)
        return self._new(
            default_module=self._module,
            module_aliases=self._aliases,
            config=self._config,
            globals_=new_globals,
        )

    def as_dict(self):
        rv = {}
        if self._module is not None:
            rv["module"] = self._module
        if self._aliases:
            rv["aliases"] = list(self._aliases.items())
        if self._config:
            rv["config"] = self._config
        if self._globals:
            rv["globals"] = self._globals
        return rv


class _OptionsMixin:
    def __init__(self, *args, **kwargs):
        self._options = _Options.defaults()
        super().__init__(*args, **kwargs)

    @abc.abstractmethod
    def _shallow_clone(self):
        pass

    def with_transaction_options(self, options: TransactionOptions = None):
        """Returns object with adjusted options for future transactions.

        :param options TransactionOptions:
            Object that encapsulates transaction options.

        This method returns a "shallow copy" of the current object
        with modified transaction options.

        Both ``self`` and returned object can be used after, but when using
        them transaction options applied will be different.

        Transaction options are used by the ``transaction`` method.
        """
        result = self._shallow_clone()
        result._options = self._options.with_transaction_options(options)
        return result

    def with_retry_options(self, options: RetryOptions=None):
        """Returns object with adjusted options for future retrying
        transactions.

        :param options RetryOptions:
            Object that encapsulates retry options.

        This method returns a "shallow copy" of the current object
        with modified retry options.

        Both ``self`` and returned object can be used after, but when using
        them retry options applied will be different.
        """

        result = self._shallow_clone()
        result._options = self._options.with_retry_options(options)
        return result

    def with_warning_handler(self, warning_handler: WarningHandler=None):
        """Returns object with adjusted options for handling warnings.

        :param warning_handler WarningHandler:
            Function for handling warnings. It is passed a tuple of warnings
            and the query result and returns a potentially updated query
            result.

        This method returns a "shallow copy" of the current object
        with modified retry options.

        Both ``self`` and returned object can be used after, but when using
        them retry options applied will be different.
        """

        result = self._shallow_clone()
        result._options = self._options.with_warning_handler(warning_handler)
        return result

    def with_state(self, state: State):
        result = self._shallow_clone()
        result._options = self._options.with_state(state)
        return result

    def with_default_module(self, module: typing.Optional[str] = None):
        result = self._shallow_clone()
        result._options = self._options.with_state(
            self._options.state.with_default_module(module)
        )
        return result

    def with_module_aliases(self, *args, **aliases):
        result = self._shallow_clone()
        result._options = self._options.with_state(
            self._options.state.with_module_aliases(*args, **aliases)
        )
        return result

    def with_config(self, *args, **config):
        result = self._shallow_clone()
        result._options = self._options.with_state(
            self._options.state.with_config(*args, **config)
        )
        return result

    def with_globals(self, *args, **globals_):
        result = self._shallow_clone()
        result._options = self._options.with_state(
            self._options.state.with_globals(*args, **globals_)
        )
        return result

    def without_module_aliases(self, *aliases):
        result = self._shallow_clone()
        result._options = self._options.with_state(
            self._options.state.without_module_aliases(*aliases)
        )
        return result

    def without_config(self, *config_names):
        result = self._shallow_clone()
        result._options = self._options.with_state(
            self._options.state.without_config(*config_names)
        )
        return result

    def without_globals(self, *global_names):
        result = self._shallow_clone()
        result._options = self._options.with_state(
            self._options.state.without_globals(*global_names)
        )
        return result

    def with_query_tag(self, tag: str):
        for prefix in ["edgedb/", "gel/"]:
            if tag.startswith(prefix):
                raise errors.InvalidArgumentError(f"reserved tag: {prefix}*")
        if len(tag) > 128:
            raise errors.InvalidArgumentError(
                "tag too long (> 128 characters)"
            )

        result = self._shallow_clone()
        result._options = self._options.with_annotations(
            self._options.annotations | {TAG_NAME: tag}
        )
        return result

    def without_query_tag(self):
        result = self._shallow_clone()
        annotations = self._options.annotations.copy()
        annotations.pop(TAG_NAME, None)
        result._options = self._options.with_annotations(annotations)
        return result


class _Options:
    """Internal class for storing connection options"""

    __slots__ = [
        '_retry_options', '_transaction_options', '_state',
        '_warning_handler', '_annotations'
    ]

    def __init__(
        self,
        retry_options: RetryOptions,
        transaction_options: TransactionOptions,
        state: State,
        warning_handler: WarningHandler,
        annotations: typing.Dict[str, str],
    ):
        self._retry_options = retry_options
        self._transaction_options = transaction_options
        self._state = state
        self._warning_handler = warning_handler
        self._annotations = annotations

    @property
    def retry_options(self):
        return self._retry_options

    @property
    def transaction_options(self):
        return self._transaction_options

    @property
    def state(self):
        return self._state

    @property
    def warning_handler(self):
        return self._warning_handler

    @property
    def annotations(self):
        return self._annotations

    def with_retry_options(self, options: RetryOptions):
        return _Options(
            options,
            self._transaction_options,
            self._state,
            self._warning_handler,
            self._annotations,
        )

    def with_transaction_options(self, options: TransactionOptions):
        return _Options(
            self._retry_options,
            options,
            self._state,
            self._warning_handler,
            self._annotations,
        )

    def with_state(self, state: State):
        return _Options(
            self._retry_options,
            self._transaction_options,
            state,
            self._warning_handler,
            self._annotations,
        )

    def with_warning_handler(self, warning_handler: WarningHandler):
        return _Options(
            self._retry_options,
            self._transaction_options,
            self._state,
            warning_handler,
            self._annotations,
        )

    def with_annotations(self, annotations: typing.Dict[str, str]):
        return _Options(
            self._retry_options,
            self._transaction_options,
            self._state,
            self._warning_handler,
            annotations,
        )

    @classmethod
    def defaults(cls):
        return cls(
            RetryOptions.defaults(),
            TransactionOptions.defaults(),
            State.defaults(),
            log_warnings,
            {},
        )

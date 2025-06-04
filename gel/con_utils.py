#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import base64
import binascii
import errno
import json
import os
import re
import ssl
import typing
import urllib.parse
import warnings
import hashlib

from gel._internal import _platform

from . import errors
from . import credentials as cred_utils


EDGEDB_PORT = 5656
ERRNO_RE = re.compile(r"\[Errno (\d+)\]")
TEMPORARY_ERRORS = (
    ConnectionAbortedError,
    ConnectionRefusedError,
    ConnectionResetError,
    FileNotFoundError,
)
TEMPORARY_ERROR_CODES = frozenset({
    errno.ECONNREFUSED,
    errno.ECONNABORTED,
    errno.ECONNRESET,
    errno.ENOENT,
})

ISO_SECONDS_RE = re.compile(r'(-?\d+|-?\d+\.\d*|-?\d*\.\d+)S')
ISO_MINUTES_RE = re.compile(r'(-?\d+|-?\d+\.\d*|-?\d*\.\d+)M')
ISO_HOURS_RE = re.compile(r'(-?\d+|-?\d+\.\d*|-?\d*\.\d+)H')
ISO_UNITLESS_HOURS_RE = re.compile(r'^(-?\d+|-?\d+\.\d*|-?\d*\.\d+)$')
ISO_DAYS_RE = re.compile(r'(-?\d+|-?\d+\.\d*|-?\d*\.\d+)D')
ISO_WEEKS_RE = re.compile(r'(-?\d+|-?\d+\.\d*|-?\d*\.\d+)W')
ISO_MONTHS_RE = re.compile(r'(-?\d+|-?\d+\.\d*|-?\d*\.\d+)M')
ISO_YEARS_RE = re.compile(r'(-?\d+|-?\d+\.\d*|-?\d*\.\d+)Y')

HUMAN_HOURS_RE = re.compile(
    r'((?:(?:\s|^)-\s*)?\d*\.?\d*)\s*(?i:h(\s|\d|\.|$)|hours?(\s|$))',
)
HUMAN_MINUTES_RE = re.compile(
    r'((?:(?:\s|^)-\s*)?\d*\.?\d*)\s*(?i:m(\s|\d|\.|$)|minutes?(\s|$))',
)
HUMAN_SECONDS_RE = re.compile(
    r'((?:(?:\s|^)-\s*)?\d*\.?\d*)\s*(?i:s(\s|\d|\.|$)|seconds?(\s|$))',
)
HUMAN_MS_RE = re.compile(
    r'((?:(?:\s|^)-\s*)?\d*\.?\d*)\s*(?i:ms(\s|\d|\.|$)|milliseconds?(\s|$))',
)
HUMAN_US_RE = re.compile(
    r'((?:(?:\s|^)-\s*)?\d*\.?\d*)\s*(?i:us(\s|\d|\.|$)|microseconds?(\s|$))',
)
INSTANCE_NAME_RE = re.compile(
    r'^(\w(?:-?\w)*)$',
    re.ASCII,
)
CLOUD_INSTANCE_NAME_RE = re.compile(
    r'^([A-Za-z0-9_-](?:-?[A-Za-z0-9_])*)/([A-Za-z0-9](?:-?[A-Za-z0-9])*)$',
    re.ASCII,
)
DSN_RE = re.compile(
    r'^[a-z]+://',
    re.IGNORECASE,
)
DOMAIN_LABEL_MAX_LENGTH = 63


class ClientConfiguration(typing.NamedTuple):

    connect_timeout: float
    command_timeout: float
    wait_until_available: float


def _validate_port_spec(hosts, port):
    if isinstance(port, list):
        # If there is a list of ports, its length must
        # match that of the host list.
        if len(port) != len(hosts):
            raise errors.InterfaceError(
                'could not match {} port numbers to {} hosts'.format(
                    len(port), len(hosts)))
    else:
        port = [port for _ in range(len(hosts))]

    return port


def _parse_hostlist(hostlist, port):
    if ',' in hostlist:
        # A comma-separated list of host addresses.
        hostspecs = hostlist.split(',')
    else:
        hostspecs = [hostlist]

    hosts = []
    hostlist_ports = []

    if not port:
        portspec = _getenv('PORT')
        if portspec:
            if ',' in portspec:
                default_port = [int(p) for p in portspec.split(',')]
            else:
                default_port = int(portspec)
        else:
            default_port = EDGEDB_PORT

        default_port = _validate_port_spec(hostspecs, default_port)

    else:
        port = _validate_port_spec(hostspecs, port)

    for i, hostspec in enumerate(hostspecs):
        addr, _, hostspec_port = hostspec.partition(':')
        hosts.append(addr)

        if not port:
            if hostspec_port:
                hostlist_ports.append(int(hostspec_port))
            else:
                hostlist_ports.append(default_port[i])

    if not port:
        port = hostlist_ports

    return hosts, port


def _hash_path(path):
    path = os.path.realpath(path)
    if _platform.IS_WINDOWS and not path.startswith('\\\\'):
        path = '\\\\?\\' + path
    return hashlib.sha1(str(path).encode('utf-8')).hexdigest()


def _stash_path(path):
    base_name = os.path.basename(path)
    dir_name = base_name + '-' + _hash_path(path)
    return _platform.search_config_dir('projects', dir_name)


def _validate_tls_security(val: str) -> str:
    val = val.lower()
    if val not in {"insecure", "no_host_verification", "strict", "default"}:
        raise ValueError(
            "tls_security can only be one of "
            "`insecure`, `no_host_verification`, `strict` or `default`"
        )

    return val


def _getenv_and_key(key: str) -> typing.Tuple[typing.Optional[str], str]:
    edgedb_key = f'EDGEDB_{key}'
    edgedb_val = os.getenv(edgedb_key)
    gel_key = f'GEL_{key}'
    gel_val = os.getenv(gel_key)
    if edgedb_val is not None and gel_val is not None:
        warnings.warn(
            f'Both {gel_key} and {edgedb_key} are set; '
            f'{edgedb_key} will be ignored',
            stacklevel=1,
        )

    if gel_val is None and edgedb_val is not None:
        return edgedb_val, edgedb_key
    else:
        return gel_val, gel_key


def _getenv(key: str) -> typing.Optional[str]:
    return _getenv_and_key(key)[0]


class ResolvedConnectConfig:
    _host = None
    _host_source = None

    _port = None
    _port_source = None

    # We keep track of database and branch separately, because we want to make
    # sure that we don't use both at the same time on the same configuration
    # level.
    _database = None
    _database_source = None

    _branch = None
    _branch_source = None

    _user = None
    _user_source = None

    _password = None
    _password_source = None

    _secret_key = None
    _secret_key_source = None

    _tls_ca_data = None
    _tls_ca_data_source = None

    _tls_server_name = None
    _tls_security = None
    _tls_security_source = None

    _wait_until_available = None

    _cloud_profile = None
    _cloud_profile_source = None

    server_settings = {}

    def _set_param(self, param, value, source, validator=None):
        param_name = '_' + param
        if getattr(self, param_name) is None:
            setattr(self, param_name + '_source', source)
            if value is not None:
                setattr(
                    self,
                    param_name,
                    validator(value) if validator else value
                )

    def set_host(self, host, source):
        self._set_param('host', host, source, _validate_host)

    def set_port(self, port, source):
        self._set_param('port', port, source, _validate_port)

    def set_database(self, database, source):
        self._set_param('database', database, source, _validate_database)

    def set_branch(self, branch, source):
        self._set_param('branch', branch, source, _validate_branch)

    def set_user(self, user, source):
        self._set_param('user', user, source, _validate_user)

    def set_password(self, password, source):
        self._set_param('password', password, source)

    def set_secret_key(self, secret_key, source):
        self._set_param('secret_key', secret_key, source)

    def set_tls_ca_data(self, ca_data, source):
        self._set_param('tls_ca_data', ca_data, source)

    def set_tls_ca_file(self, ca_file, source):
        def read_ca_file(file_path):
            with open(file_path) as f:
                return f.read()

        self._set_param('tls_ca_data', ca_file, source, read_ca_file)

    def set_tls_server_name(self, ca_data, source):
        self._set_param('tls_server_name', ca_data, source)

    def set_tls_security(self, security, source):
        self._set_param('tls_security', security, source,
                        _validate_tls_security)

    def set_wait_until_available(self, wait_until_available, source):
        self._set_param(
            'wait_until_available',
            wait_until_available,
            source,
            _validate_wait_until_available,
        )

    def add_server_settings(self, server_settings):
        _validate_server_settings(server_settings)
        self.server_settings = {**server_settings, **self.server_settings}

    @property
    def address(self):
        return (
            self._host if self._host else 'localhost',
            self._port if self._port else 5656
        )

    # The properties actually merge database and branch, but "default" is
    # different. If you need to know the underlying config use the _database
    # and _branch.
    @property
    def database(self):
        return (
            self._database if self._database else
            self._branch if self._branch else
            'edgedb'
        )

    @property
    def branch(self):
        return (
            self._database if self._database else
            self._branch if self._branch else
            '__default__'
        )

    @property
    def user(self):
        return self._user if self._user else 'edgedb'

    @property
    def password(self):
        return self._password

    @property
    def secret_key(self):
        return self._secret_key

    @property
    def tls_server_name(self):
        return self._tls_server_name

    @property
    def tls_security(self):
        tls_security = self._tls_security or 'default'
        security, security_key = _getenv_and_key('CLIENT_SECURITY')
        security = security or 'default'
        if security not in {'default', 'insecure_dev_mode', 'strict'}:
            raise ValueError(
                f'environment variable {security_key} should be '
                f'one of strict, insecure_dev_mode or default, '
                f'got: {security!r}')

        if security == 'default':
            pass
        elif security == 'insecure_dev_mode':
            if tls_security == 'default':
                tls_security = 'insecure'
        elif security == 'strict':
            if tls_security == 'default':
                tls_security = 'strict'
            elif tls_security in {'no_host_verification', 'insecure'}:
                raise ValueError(
                    f'{security_key}=strict but '
                    f'tls_security={tls_security}, tls_security must be '
                    f'set to strict when {security_key} is strict')

        if tls_security != 'default':
            return tls_security

        if self._tls_ca_data is not None:
            return "no_host_verification"

        return "strict"

    _ssl_ctx = None

    @property
    def ssl_ctx(self):
        if (self._ssl_ctx):
            return self._ssl_ctx

        self._ssl_ctx = self.make_ssl_ctx()
        self._ssl_ctx.set_alpn_protocols(['edgedb-binary'])
        return self._ssl_ctx

    def make_ssl_ctx(self):
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

        if self._tls_ca_data:
            ssl_ctx.load_verify_locations(
                cadata=self._tls_ca_data
            )
        else:
            ssl_ctx.load_default_certs(ssl.Purpose.SERVER_AUTH)
            if _platform.IS_WINDOWS:
                import certifi
                ssl_ctx.load_verify_locations(cafile=certifi.where())

        tls_security = self.tls_security
        ssl_ctx.check_hostname = tls_security == "strict"

        if tls_security in {"strict", "no_host_verification"}:
            ssl_ctx.verify_mode = ssl.CERT_REQUIRED
        else:
            ssl_ctx.verify_mode = ssl.CERT_NONE

        return ssl_ctx

    @property
    def wait_until_available(self):
        return (
            self._wait_until_available
            if self._wait_until_available is not None
            else 30
        )


def _validate_host(host):
    if '/' in host:
        raise ValueError('unix socket paths not supported')
    if host == '' or ',' in host:
        raise ValueError(f'invalid host: "{host}"')
    return host


def _prepare_host_for_dsn(host):
    host = _validate_host(host)
    if ':' in host:
        # IPv6
        host = f'[{host}]'
    return host


def _validate_port(port):
    try:
        if isinstance(port, str):
            port = int(port)
        if not isinstance(port, int):
            raise ValueError()
    except Exception:
        raise ValueError(f'invalid port: {port}, not an integer')
    if port < 1 or port > 65535:
        raise ValueError(f'invalid port: {port}, must be between 1 and 65535')
    return port


def _validate_database(database):
    if database == '':
        raise ValueError(f'invalid database name: {database}')
    return database


def _validate_branch(branch):
    if branch == '':
        raise ValueError(f'invalid branch name: {branch}')
    return branch


def _validate_user(user):
    if user == '':
        raise ValueError(f'invalid user name: {user}')
    return user


def _pop_iso_unit(rgex: re.Pattern, string: str) -> typing.Tuple[float, str]:
    s = string
    total = 0
    match = rgex.search(string)
    if match:
        total += float(match.group(1))
        s = s.replace(match.group(0), "", 1)

    return (total, s)


def _parse_iso_duration(string: str) -> typing.Union[float, int]:
    if not string.startswith("PT"):
        raise ValueError(f"invalid duration {string!r}")

    time = string[2:]
    match = ISO_UNITLESS_HOURS_RE.search(time)
    if match:
        hours = float(match.group(0))
        return 3600 * hours

    hours, time = _pop_iso_unit(ISO_HOURS_RE, time)
    minutes, time = _pop_iso_unit(ISO_MINUTES_RE, time)
    seconds, time = _pop_iso_unit(ISO_SECONDS_RE, time)

    if time:
        raise ValueError(f'invalid duration {string!r}')

    return 3600 * hours + 60 * minutes + seconds


def _remove_white_space(s: str) -> str:
    return ''.join(c for c in s if not c.isspace())


def _pop_human_duration_unit(
    rgex: re.Pattern,
    string: str,
) -> typing.Tuple[float, bool, str]:
    match = rgex.search(string)
    if not match:
        return 0, False, string

    number = 0
    if match.group(1):
        literal = _remove_white_space(match.group(1))
        if literal.endswith('.'):
            return 0, False, string

        if literal.startswith('-.'):
            return 0, False, string

        number = float(literal)
        string = string.replace(
            match.group(0),
            match.group(2) or match.group(3) or "",
            1,
        )

    return number, True, string


def _parse_human_duration(string: str) -> float:
    found = False

    hour, f, s = _pop_human_duration_unit(HUMAN_HOURS_RE, string)
    found |= f

    minute, f, s = _pop_human_duration_unit(HUMAN_MINUTES_RE, s)
    found |= f

    second, f, s = _pop_human_duration_unit(HUMAN_SECONDS_RE, s)
    found |= f

    ms, f, s = _pop_human_duration_unit(HUMAN_MS_RE, s)
    found |= f

    us, f, s = _pop_human_duration_unit(HUMAN_US_RE, s)
    found |= f

    if s.strip() or not found:
        raise ValueError(f'invalid duration {string!r}')

    return 3600 * hour + 60 * minute + second + 0.001 * ms + 0.000001 * us


def _parse_duration_str(string: str) -> float:
    if string.startswith('PT'):
        return _parse_iso_duration(string)
    return _parse_human_duration(string)


def _validate_wait_until_available(wait_until_available):
    if isinstance(wait_until_available, str):
        return _parse_duration_str(wait_until_available)

    if isinstance(wait_until_available, (int, float)):
        return wait_until_available

    raise ValueError(f"invalid duration {wait_until_available!r}")


def _validate_server_settings(server_settings):
    if (
        not isinstance(server_settings, dict) or
        not all(isinstance(k, str) for k in server_settings) or
        not all(isinstance(v, str) for v in server_settings.values())
    ):
        raise ValueError(
            'server_settings is expected to be None or '
            'a Dict[str, str]')


def _parse_connect_dsn_and_args(
    *,
    dsn,
    host,
    port,
    credentials,
    credentials_file,
    user,
    password,
    secret_key,
    database,
    branch,
    tls_ca,
    tls_ca_file,
    tls_security,
    tls_server_name,
    server_settings,
    wait_until_available,
):
    resolved_config = ResolvedConnectConfig()

    if dsn and DSN_RE.match(dsn):
        instance_name = None
    else:
        instance_name, dsn = dsn, None

    def _get(key: str) -> typing.Optional[typing.Tuple[str, str]]:
        val, env = _getenv_and_key(key)
        return (
            (val, f'"{env}" environment variable')
            if val is not None else None
        )

    # The cloud profile is potentially relevant to resolving credentials at
    # any stage, including the config stage when other environment variables
    # are not yet read.
    cloud_profile_tuple = _get('CLOUD_PROFILE')
    cloud_profile = cloud_profile_tuple[0] if cloud_profile_tuple else None

    has_compound_options = _resolve_config_options(
        resolved_config,
        'Cannot have more than one of the following connection options: '
        + '"dsn", "credentials", "credentials_file" or "host"/"port"',
        dsn=(dsn, '"dsn" option') if dsn is not None else None,
        instance_name=(
            (instance_name, '"dsn" option (parsed as instance name)')
            if instance_name is not None else None
        ),
        credentials=(
            (credentials, '"credentials" option')
            if credentials is not None else None
        ),
        credentials_file=(
            (credentials_file, '"credentials_file" option')
            if credentials_file is not None else None
        ),
        host=(host, '"host" option') if host is not None else None,
        port=(port, '"port" option') if port is not None else None,
        database=(
            (database, '"database" option')
            if database is not None else None
        ),
        branch=(
            (branch, '"branch" option')
            if branch is not None else None
        ),
        user=(user, '"user" option') if user is not None else None,
        password=(
            (password, '"password" option')
            if password is not None else None
        ),
        secret_key=(
            (secret_key, '"secret_key" option')
            if secret_key is not None else None
        ),
        tls_ca=(
            (tls_ca, '"tls_ca" option')
            if tls_ca is not None else None
        ),
        tls_ca_file=(
            (tls_ca_file, '"tls_ca_file" option')
            if tls_ca_file is not None else None
        ),
        tls_security=(
            (tls_security, '"tls_security" option')
            if tls_security is not None else None
        ),
        tls_server_name=(
            (tls_server_name, '"tls_server_name" option')
            if tls_server_name is not None else None
        ),
        server_settings=(
            (server_settings, '"server_settings" option')
            if server_settings is not None else None
        ),
        wait_until_available=(
            (wait_until_available, '"wait_until_available" option')
            if wait_until_available is not None else None
        ),
        cloud_profile=cloud_profile_tuple,
    )

    if has_compound_options is False:
        env_port_tuple = _get("PORT")
        if (
            resolved_config._port is None
            and env_port_tuple
            and env_port_tuple[0].startswith('tcp://')
        ):
            # EDGEDB_PORT is set by 'docker --link' so ignore and warn
            warnings.warn('EDGEDB_PORT in "tcp://host:port" format, ' +
                          'so will be ignored', stacklevel=1)
            env_port_tuple = None

        has_compound_options = _resolve_config_options(
            resolved_config,
            # XXX
            'Cannot have more than one of the following connection '
            + 'environment variables: "EDGEDB_DSN", "EDGEDB_INSTANCE", '
            + '"EDGEDB_CREDENTIALS_FILE" or "EDGEDB_HOST"/"EDGEDB_PORT"',
            dsn=_get('DSN'),
            instance_name=_get('INSTANCE'),
            credentials_file=_get('CREDENTIALS_FILE'),
            host=_get('HOST'),
            port=env_port_tuple,
            database=_get('DATABASE'),
            branch=_get('BRANCH'),
            user=_get('USER'),
            password=_get('PASSWORD'),
            secret_key=_get('SECRET_KEY'),
            tls_ca=_get('TLS_CA'),
            tls_ca_file=_get('TLS_CA_FILE'),
            tls_security=_get('CLIENT_TLS_SECURITY'),
            tls_server_name=_get('TLS_SERVER_NAME'),
            wait_until_available=_get('WAIT_UNTIL_AVAILABLE'),
        )

    if not has_compound_options:
        dir = find_gel_project_dir()
        stash_dir = _stash_path(dir)
        if os.path.exists(stash_dir):
            with open(os.path.join(stash_dir, 'instance-name'), 'rt') as f:
                instance_name = f.read().strip()
            cloud_profile_file = os.path.join(stash_dir, 'cloud-profile')
            if os.path.exists(cloud_profile_file):
                with open(cloud_profile_file, 'rt') as f:
                    cloud_profile = f.read().strip()
            else:
                cloud_profile = None

            _resolve_config_options(
                resolved_config,
                '',
                instance_name=(
                    instance_name,
                    f'project linked instance ("{instance_name}")'
                ),
                cloud_profile=(
                    cloud_profile,
                    f'project defined cloud profile ("{cloud_profile}")'
                ),
            )

            opt_database_file = os.path.join(stash_dir, 'database')
            if os.path.exists(opt_database_file):
                with open(opt_database_file, 'rt') as f:
                    database = f.read().strip()
                resolved_config.set_database(database, "project")
        else:
            raise errors.ClientConnectionError(
                f'Found `gel.toml` but the project is not initialized. '
                f'Run `gel project init`.'
            )

    return resolved_config


def _parse_dsn_into_config(
    resolved_config: ResolvedConnectConfig,
    dsn: typing.Tuple[str, str]
):
    dsn_str, source = dsn

    try:
        parsed = urllib.parse.urlparse(dsn_str)
        host = (
            urllib.parse.unquote(parsed.hostname) if parsed.hostname else None
        )
        port = parsed.port
        database = parsed.path
        user = parsed.username
        password = parsed.password
    except Exception as e:
        raise ValueError(f'invalid DSN or instance name: {str(e)}')

    if parsed.scheme != 'edgedb' and parsed.scheme != 'gel':
        raise ValueError(
            f'invalid DSN: scheme is expected to be '
            f'"edgedb" or "gel", got {parsed.scheme!r}')

    query = (
        urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        if parsed.query != ''
        else {}
    )
    for key, val in query.items():
        if isinstance(val, list):
            if len(val) > 1:
                raise ValueError(
                    f'invalid DSN: duplicate query parameter {key}')
            query[key] = val[-1]

    def handle_dsn_part(
        paramName, value, currentValue, setter,
        formatter=lambda val: val
    ):
        param_values = [
            (value if value != '' else None),
            query.get(paramName),
            query.get(paramName + '_env'),
            query.get(paramName + '_file')
        ]
        if len([p for p in param_values if p is not None]) > 1:
            raise ValueError(
                f'invalid DSN: more than one of ' +
                f'{(paramName + ", ") if value else ""}' +
                f'?{paramName}=, ?{paramName}_env=, ?{paramName}_file= ' +
                f'was specified'
            )

        if currentValue is None:
            param = (
                value if (value is not None and value != '')
                else query.get(paramName)
            )
            paramSource = source

            if param is None:
                env = query.get(paramName + '_env')
                if env is not None:
                    param = os.getenv(env)
                    if param is None:
                        raise ValueError(
                            f'{paramName}_env environment variable "{env}" ' +
                            f'doesn\'t exist')
                    paramSource = paramSource + f' ({paramName}_env: {env})'
            if param is None:
                filename = query.get(paramName + '_file')
                if filename is not None:
                    with open(filename) as f:
                        param = f.read()
                    paramSource = (
                        paramSource + f' ({paramName}_file: {filename})'
                    )

            param = formatter(param) if param is not None else None

            setter(param, paramSource)

        query.pop(paramName, None)
        query.pop(paramName + '_env', None)
        query.pop(paramName + '_file', None)

    handle_dsn_part(
        'host', host, resolved_config._host, resolved_config.set_host
    )

    handle_dsn_part(
        'port', port, resolved_config._port, resolved_config.set_port
    )

    def strip_leading_slash(str):
        return str[1:] if str.startswith('/') else str

    if (
        'branch' in query or
        'branch_env' in query or
        'branch_file' in query
    ):
        if (
            'database' in query or
            'database_env' in query or
            'database_file' in query
        ):
            raise ValueError(
                f"invalid DSN: `database` and `branch` cannot be present "
                f"at the same time"
            )
        if resolved_config._database is None:
            # Only update the config if 'database' has not been already
            # resolved.
            handle_dsn_part(
                'branch', strip_leading_slash(database),
                resolved_config._branch, resolved_config.set_branch,
                strip_leading_slash
            )
        else:
            # Clean up the query, if config already has 'database'
            query.pop('branch', None)
            query.pop('branch_env', None)
            query.pop('branch_file', None)

    else:
        if resolved_config._branch is None:
            # Only update the config if 'branch' has not been already
            # resolved.
            handle_dsn_part(
                'database', strip_leading_slash(database),
                resolved_config._database, resolved_config.set_database,
                strip_leading_slash
            )
        else:
            # Clean up the query, if config already has 'branch'
            query.pop('database', None)
            query.pop('database_env', None)
            query.pop('database_file', None)

    handle_dsn_part(
        'user', user, resolved_config._user, resolved_config.set_user
    )

    handle_dsn_part(
        'password', password,
        resolved_config._password, resolved_config.set_password
    )

    handle_dsn_part(
        'secret_key', None,
        resolved_config._secret_key, resolved_config.set_secret_key
    )

    handle_dsn_part(
        'tls_ca_file', None,
        resolved_config._tls_ca_data, resolved_config.set_tls_ca_file
    )

    handle_dsn_part(
        'tls_server_name', None,
        resolved_config._tls_server_name,
        resolved_config.set_tls_server_name
    )

    handle_dsn_part(
        'tls_security', None,
        resolved_config._tls_security,
        resolved_config.set_tls_security
    )

    handle_dsn_part(
        'wait_until_available', None,
        resolved_config._wait_until_available,
        resolved_config.set_wait_until_available
    )

    resolved_config.add_server_settings(query)


def _jwt_base64_decode(payload):
    remainder = len(payload) % 4
    if remainder == 2:
        payload += '=='
    elif remainder == 3:
        payload += '='
    elif remainder != 0:
        raise errors.ClientConnectionError("Invalid secret key")
    payload = base64.urlsafe_b64decode(payload.encode("utf-8"))
    return json.loads(payload.decode("utf-8"))


def _parse_cloud_instance_name_into_config(
    resolved_config: ResolvedConnectConfig,
    source: str,
    org_slug: str,
    instance_name: str,
):
    org_slug = org_slug.lower()
    instance_name = instance_name.lower()

    label = f"{instance_name}--{org_slug}"
    if len(label) > DOMAIN_LABEL_MAX_LENGTH:
        raise ValueError(
            f"invalid instance name: cloud instance name length cannot exceed "
            f"{DOMAIN_LABEL_MAX_LENGTH - 1} characters: "
            f"{org_slug}/{instance_name}"
        )
    secret_key = resolved_config.secret_key
    if secret_key is None:
        try:
            config_dir = _platform.config_dir()
            if resolved_config._cloud_profile is None:
                profile = profile_src = "default"
            else:
                profile = resolved_config._cloud_profile
                profile_src = resolved_config._cloud_profile_source
            path = config_dir / "cloud-credentials" / f"{profile}.json"
            with open(path, "rt") as f:
                secret_key = json.load(f)["secret_key"]
        except Exception:
            raise errors.ClientConnectionError(
                "Cannot connect to cloud instances without secret key."
            )
        resolved_config.set_secret_key(
            secret_key,
            f"cloud-credentials/{profile}.json specified by {profile_src}",
        )
    try:
        dns_zone = _jwt_base64_decode(secret_key.split(".", 2)[1])["iss"]
    except errors.EdgeDBError:
        raise
    except Exception:
        raise errors.ClientConnectionError("Invalid secret key")
    payload = f"{org_slug}/{instance_name}".encode("utf-8")
    dns_bucket = binascii.crc_hqx(payload, 0) % 100
    host = f"{label}.c-{dns_bucket:02d}.i.{dns_zone}"
    resolved_config.set_host(host, source)


def _resolve_config_options(
    resolved_config: ResolvedConnectConfig,
    compound_error: str,
    *,
    dsn=None,
    instance_name=None,
    credentials=None,
    credentials_file=None,
    host=None,
    port=None,
    database=None,
    branch=None,
    user=None,
    password=None,
    secret_key=None,
    tls_ca=None,
    tls_ca_file=None,
    tls_security=None,
    tls_server_name=None,
    server_settings=None,
    wait_until_available=None,
    cloud_profile=None,
):
    if database is not None:
        if branch is not None:
            raise errors.ClientConnectionError(
                f"{database[1]} and {branch[1]} are mutually exclusive"
            )
        if resolved_config._branch is None:
            # Only update the config if 'branch' has not been already
            # resolved.
            resolved_config.set_database(*database)
    if branch is not None:
        if resolved_config._database is None:
            # Only update the config if 'database' has not been already
            # resolved.
            resolved_config.set_branch(*branch)
    if user is not None:
        resolved_config.set_user(*user)
    if password is not None:
        resolved_config.set_password(*password)
    if secret_key is not None:
        resolved_config.set_secret_key(*secret_key)
    if tls_ca_file is not None:
        if tls_ca is not None:
            raise errors.ClientConnectionError(
                f"{tls_ca[1]} and {tls_ca_file[1]} are mutually exclusive"
            )
        resolved_config.set_tls_ca_file(*tls_ca_file)
    if tls_ca is not None:
        resolved_config.set_tls_ca_data(*tls_ca)
    if tls_security is not None:
        resolved_config.set_tls_security(*tls_security)
    if tls_server_name is not None:
        resolved_config.set_tls_server_name(*tls_server_name)
    if server_settings is not None:
        resolved_config.add_server_settings(server_settings[0])
    if wait_until_available is not None:
        resolved_config.set_wait_until_available(*wait_until_available)
    if cloud_profile is not None:
        resolved_config._set_param('cloud_profile', *cloud_profile)

    compound_params = [
        dsn,
        instance_name,
        credentials,
        credentials_file,
        host or port,
    ]
    compound_params_count = len([p for p in compound_params if p is not None])

    if compound_params_count > 1:
        raise errors.ClientConnectionError(compound_error)

    elif compound_params_count == 1:
        if dsn is not None or host is not None or port is not None:
            if port is not None:
                resolved_config.set_port(*port)
            if dsn is None:
                dsn = (
                    'edgedb://' +
                    (_prepare_host_for_dsn(host[0]) if host else ''),
                    host[1] if host is not None else port[1]
                )
            _parse_dsn_into_config(resolved_config, dsn)
        else:
            if credentials_file is not None:
                creds = cred_utils.read_credentials(credentials_file[0])
                source = "credentials"
            elif credentials is not None:
                try:
                    cred_data = json.loads(credentials[0])
                except ValueError as e:
                    raise RuntimeError(f"cannot read credentials") from e
                else:
                    creds = cred_utils.validate_credentials(cred_data)
                source = "credentials"
            elif INSTANCE_NAME_RE.match(instance_name[0]):
                source = instance_name[1]
                creds = cred_utils.read_credentials(
                    cred_utils.get_credentials_path(instance_name[0]),
                )
            else:
                name_match = CLOUD_INSTANCE_NAME_RE.match(instance_name[0])
                if name_match is None:
                    raise ValueError(
                        f'invalid DSN or instance name: "{instance_name[0]}"'
                    )
                source = instance_name[1]
                org, inst = name_match.groups()
                _parse_cloud_instance_name_into_config(
                    resolved_config, source, org, inst
                )
                return True

            resolved_config.set_host(creds.get('host'), source)
            resolved_config.set_port(creds.get('port'), source)
            if 'database' in creds and resolved_config._branch is None:
                # Only update the config if 'branch' has not been already
                # resolved.
                resolved_config.set_database(creds.get('database'), source)

            elif 'branch' in creds and resolved_config._database is None:
                # Only update the config if 'database' has not been already
                # resolved.
                resolved_config.set_branch(creds.get('branch'), source)
            resolved_config.set_user(creds.get('user'), source)
            resolved_config.set_password(creds.get('password'), source)
            resolved_config.set_tls_ca_data(creds.get('tls_ca'), source)
            resolved_config.set_tls_security(
                creds.get('tls_security'),
                source
            )

        return True

    else:
        return False


def find_gel_project_dir() -> str:
    dir = os.getcwd()
    dev = os.stat(dir).st_dev

    while True:
        gel_toml = os.path.join(dir, 'gel.toml')
        edgedb_toml = os.path.join(dir, 'edgedb.toml')
        if not os.path.isfile(gel_toml) and not os.path.isfile(edgedb_toml):
            parent = os.path.dirname(dir)
            if parent == dir:
                raise errors.ClientConnectionError(
                    f'no `gel.toml` found and '
                    f'no connection options specified'
                )
            parent_dev = os.stat(parent).st_dev
            if parent_dev != dev:
                raise errors.ClientConnectionError(
                    f'no `gel.toml` found and '
                    f'no connection options specified'
                    f'(stopped searching for `gel.toml` at file system'
                    f'boundary {dir!r})'
                )
            dir = parent
            dev = parent_dev
            continue
        return dir


def parse_connect_arguments(
    *,
    dsn,
    host,
    port,
    credentials,
    credentials_file,
    database,
    branch,
    user,
    password,
    secret_key,
    tls_ca,
    tls_ca_file,
    tls_security,
    tls_server_name,
    timeout,
    command_timeout,
    wait_until_available,
    server_settings,
) -> typing.Tuple[ResolvedConnectConfig, ClientConfiguration]:

    if command_timeout is not None:
        try:
            if isinstance(command_timeout, bool):
                raise ValueError
            command_timeout = float(command_timeout)
            if command_timeout <= 0:
                raise ValueError
        except ValueError:
            raise ValueError(
                'invalid command_timeout value: '
                'expected greater than 0 float (got {!r})'.format(
                    command_timeout)) from None

    connect_config = _parse_connect_dsn_and_args(
        dsn=dsn,
        host=host,
        port=port,
        credentials=credentials,
        credentials_file=credentials_file,
        database=database,
        branch=branch,
        user=user,
        password=password,
        secret_key=secret_key,
        tls_ca=tls_ca,
        tls_ca_file=tls_ca_file,
        tls_security=tls_security,
        tls_server_name=tls_server_name,
        server_settings=server_settings,
        wait_until_available=wait_until_available,
    )

    client_config = ClientConfiguration(
        connect_timeout=timeout,
        command_timeout=command_timeout,
        wait_until_available=connect_config.wait_until_available,
    )

    return connect_config, client_config


def check_alpn_protocol(ssl_obj):
    if ssl_obj.selected_alpn_protocol() != 'edgedb-binary':
        raise errors.ClientConnectionFailedError(
            "The server doesn't support the edgedb-binary protocol."
        )


def render_client_no_connection_error(prefix, addr, attempts, duration):
    if isinstance(addr, str):
        msg = (
            f'{prefix}'
            f'\n\tAfter {attempts} attempts in {duration:.1f} sec'
            f'\n\tIs the server running locally and accepting '
            f'\n\tconnections on Unix domain socket {addr!r}?'
        )
    else:
        msg = (
            f'{prefix}'
            f'\n\tAfter {attempts} attempts in {duration:.1f} sec'
            f'\n\tIs the server running on host {addr[0]!r} '
            f'and accepting '
            f'\n\tTCP/IP connections on port {addr[1]}?'
        )
    return msg


def _extract_errno(s):
    """Extract multiple errnos from error string

    When we connect to a host that has multiple underlying IP addresses, say
    ``localhost`` having ``::1`` and ``127.0.0.1``, we get
    ``OSError("Multiple exceptions:...")`` error without ``.errno`` attribute
    set. There are multiple ones in the text, so we extract all of them.
    """
    result = []
    for match in ERRNO_RE.finditer(s):
        result.append(int(match.group(1)))
    if result:
        return result


def wrap_error(e):
    message = str(e)
    if e.errno is None:
        errnos = _extract_errno(message)
    else:
        errnos = [e.errno]

    if errnos:
        is_temp = any((code in TEMPORARY_ERROR_CODES for code in errnos))
    else:
        is_temp = isinstance(e, TEMPORARY_ERRORS)

    if is_temp:
        return errors.ClientConnectionFailedTemporarilyError(message)
    else:
        return errors.ClientConnectionFailedError(message)

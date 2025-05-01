#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
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


import importlib
import pathlib
import os
import unittest
from unittest import mock

from gel import credentials
from gel import platform


class _MockExists:
    def __init__(self):
        self.called = False

    def __call__(self, *args, **kwargs):
        if self.called:
            return True
        else:
            self.called = True
            return False


class TestCredentials(unittest.TestCase):
    def tearDown(self):
        importlib.reload(platform)

    def test_credentials_read(self):
        creds = credentials.read_credentials(
            pathlib.Path(__file__).parent / 'credentials1.json')
        self.assertEqual(creds, {
            'database': 'test3n',
            'password': 'lZTBy1RVCfOpBAOwSCwIyBIR',
            'port': 10702,
            'user': 'test3n',
        })

    def test_credentials_empty(self):
        with self.assertRaisesRegex(ValueError, '`user` key is required'):
            credentials.validate_credentials({})

    def test_credentials_port(self):
        with self.assertRaisesRegex(ValueError, 'invalid `port` value'):
            credentials.validate_credentials({
                'user': 'u1',
                'port': '1234',
            })

        with self.assertRaisesRegex(ValueError, 'invalid `port` value'):
            credentials.validate_credentials({
                'user': 'u1',
                'port': 0,
            })

        with self.assertRaisesRegex(ValueError, 'invalid `port` value'):
            credentials.validate_credentials({
                'user': 'u1',
                'port': -1,
            })

        with self.assertRaisesRegex(ValueError, 'invalid `port` value'):
            credentials.validate_credentials({
                'user': 'u1',
                'port': 65536,
            })

    def test_credentials_extra_key(self):
        creds = credentials.validate_credentials(dict(
            user='user1',
            some_extra_data='test',
        ))
        # extra keys are ignored for forward compatibility
        # but aren't exported through validator
        self.assertEqual(creds, {"user": "user1", "port": 5656})

    @mock.patch("sys.platform", "darwin")
    @mock.patch("pathlib.Path.home")
    def test_get_credentials_path_macos(self, home_method):
        importlib.reload(platform)
        home_method.return_value = pathlib.PurePosixPath("/Users/edgedb")
        with mock.patch(
            "pathlib.PurePosixPath.exists", lambda x: True, create=True,
        ):
            self.assertEqual(
                str(credentials.get_credentials_path("test")),
                "/Users/edgedb/Library/Application Support/"
                "edgedb/credentials/test.json",
            )
        with mock.patch(
            "pathlib.PurePosixPath.exists", _MockExists(), create=True,
        ):
            self.assertEqual(
                str(credentials.get_credentials_path("test")),
                "/Users/edgedb/.edgedb/credentials/test.json",
            )

    @mock.patch("sys.platform", "win32")
    @mock.patch("pathlib.Path", pathlib.PureWindowsPath)
    @mock.patch("ctypes.windll", create=True)
    def test_get_credentials_path_win(self, windll):
        importlib.reload(platform)

        def get_folder_path(_a, _b, _c, _d, path_buf):
            path_buf.value = r"c:\Users\edgedb\AppData\Local"

        windll.shell32 = mock.Mock()
        windll.shell32.SHGetFolderPathW = get_folder_path

        with mock.patch(
            "pathlib.PureWindowsPath.exists", lambda x: True, create=True
        ):
            self.assertEqual(
                str(credentials.get_credentials_path("test")),
                r"c:\Users\edgedb\AppData\Local"
                r"\EdgeDB\config\credentials\test.json",
            )
        with mock.patch(
            "pathlib.PureWindowsPath.exists", _MockExists(), create=True
        ), mock.patch(
            'pathlib.PureWindowsPath.home',
            lambda: pathlib.PureWindowsPath(r"c:\Users\edgedb"),
            create=True,
        ):
            self.assertEqual(
                str(credentials.get_credentials_path("test")),
                r"c:\Users\edgedb\.edgedb\credentials\test.json",
            )

    @mock.patch("sys.platform", "linux2")
    @mock.patch("pathlib.Path", pathlib.PurePosixPath)
    @mock.patch("pathlib.PurePosixPath.home", mock.Mock(), create=True)
    @mock.patch.dict(
        os.environ, {"XDG_CONFIG_HOME": "/home/edgedb/.config"}, clear=True
    )
    def test_get_credentials_path_linux_xdg(self):
        importlib.reload(platform)
        with mock.patch(
            "pathlib.PurePosixPath.exists", lambda x: True, create=True
        ):
            self.assertEqual(
                str(credentials.get_credentials_path("test")),
                "/home/edgedb/.config/edgedb/credentials/test.json",
            )
        with mock.patch(
            "pathlib.PurePosixPath.exists", _MockExists(), create=True
        ):
            pathlib.PurePosixPath.home.return_value = pathlib.PurePosixPath(
                "/home/edgedb"
            )
            self.assertEqual(
                str(credentials.get_credentials_path("test")),
                "/home/edgedb/.edgedb/credentials/test.json",
            )

    @mock.patch("sys.platform", "linux2")
    @mock.patch("pathlib.Path", pathlib.PurePosixPath)
    @mock.patch("pathlib.PurePosixPath.home", create=True)
    @mock.patch.dict(os.environ, {}, clear=True)
    def test_get_credentials_path_linux_no_xdg(self, home_method):
        importlib.reload(platform)
        home_method.return_value = pathlib.PurePosixPath("/home/edgedb")

        with mock.patch(
            "pathlib.PurePosixPath.exists", lambda x: True, create=True
        ):
            self.assertEqual(
                str(credentials.get_credentials_path("test")),
                "/home/edgedb/.config/edgedb/credentials/test.json",
            )
        with mock.patch(
            "pathlib.PurePosixPath.exists", _MockExists(), create=True
        ):
            self.assertEqual(
                str(credentials.get_credentials_path("test")),
                "/home/edgedb/.edgedb/credentials/test.json",
            )

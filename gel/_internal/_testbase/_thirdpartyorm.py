# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import Any, ClassVar

import importlib.util
import os.path
import sys
import tempfile
import unittest
import warnings

from gel.orm.introspection import get_schema_json, GelORMWarning
from gel.orm.sqla import ModelGenerator as SQLAModGen
from gel.orm.sqlmodel import ModelGenerator as SQLModGen
from gel.orm.django.generator import ModelGenerator as DjangoModGen

from ._base import SyncQueryTestCase


class ORMTestCase(SyncQueryTestCase):
    MODEL_PACKAGE: str | None = None
    DEFAULT_MODULE = "default"

    tmpormdir: ClassVar[tempfile.TemporaryDirectory[str]]
    spec: ClassVar[Any]

    @classmethod
    def setUpClass(cls) -> None:
        # ORMs rely on psycopg2 to connect to Postgres and thus we
        # need it to run tests. Unfortunately not all test environemnts might
        # have psycopg2 installed, as long as we run this in the test
        # environments that have this, it is fine since we're not expecting
        # different functionality based on flavours of psycopg2.
        if importlib.util.find_spec("psycopg2") is None:
            raise unittest.SkipTest("need psycopg2 for ORM tests")

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", GelORMWarning)

            super().setUpClass()

            class_set_up = os.environ.get("EDGEDB_TEST_CASES_SET_UP")
            if not class_set_up:
                # We'll need a temp directory to setup the generated Python
                # package
                cls.tmpormdir = tempfile.TemporaryDirectory()
                sys.path.append(cls.tmpormdir.name)
                # Now that the DB is setup, generate the ORM models from it
                cls.spec = get_schema_json(cls.client)  # type: ignore[no-untyped-call]
                cls.setupORM()

    @classmethod
    def setupORM(cls) -> None:  # noqa: N802
        raise NotImplementedError

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            super().tearDownClass()
        finally:
            # cleanup the temp modules
            sys.path.remove(cls.tmpormdir.name)
            cls.tmpormdir.cleanup()


class SQLATestCase(ORMTestCase):
    @classmethod
    def setupORM(cls) -> None:  # noqa: N802
        if cls.MODEL_PACKAGE is None:
            raise RuntimeError("MODEL_PACKAGE cannot be None")
        gen = SQLAModGen(  # type: ignore[no-untyped-call]
            outdir=os.path.join(cls.tmpormdir.name, cls.MODEL_PACKAGE),
            basemodule=cls.MODEL_PACKAGE,
        )
        gen.render_models(cls.spec)  # type: ignore[no-untyped-call]

    @classmethod
    def get_dsn_for_sqla(cls) -> str:
        cargs = cls.get_connect_args(database=cls.get_database_name())
        dsn = (
            f"postgresql://{cargs['user']}:{cargs['password']}"
            f"@{cargs['host']}:{cargs['port']}/{cargs['database']}"
        )

        return dsn


class SQLModelTestCase(ORMTestCase):
    @classmethod
    def setupORM(cls) -> None:  # noqa: N802
        if cls.MODEL_PACKAGE is None:
            raise RuntimeError("MODEL_PACKAGE cannot be None")
        gen = SQLModGen(  # type: ignore[no-untyped-call]
            outdir=os.path.join(cls.tmpormdir.name, cls.MODEL_PACKAGE),
            basemodule=cls.MODEL_PACKAGE,
        )
        gen.render_models(cls.spec)  # type: ignore[no-untyped-call]

    @classmethod
    def get_dsn_for_sqla(cls) -> str:
        cargs = cls.get_connect_args(database=cls.get_database_name())
        dsn = (
            f"postgresql://{cargs['user']}:{cargs['password']}"
            f"@{cargs['host']}:{cargs['port']}/{cargs['database']}"
        )

        return dsn


APPS_PY = """\
from django.apps import AppConfig


class TestConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = {name!r}
"""

SETTINGS_PY = """\
from pathlib import Path

mysettings = dict(
    INSTALLED_APPS=[
        '{appname}.apps.TestConfig',
        'gel.orm.django.gelmodels.apps.GelPGModel',
    ],
    DATABASES={{
        'default': {{
            'ENGINE': 'django.db.backends.postgresql',
            'NAME': {database!r},
            'USER': {user!r},
            'PASSWORD': {password!r},
            'HOST': {host!r},
            'PORT': {port!r},
        }}
    }},
)
"""


class DjangoTestCase(ORMTestCase):
    @classmethod
    def setupORM(cls) -> None:  # noqa: N802
        if cls.MODEL_PACKAGE is None:
            raise RuntimeError("MODEL_PACKAGE cannot be None")
        pkgbase = os.path.join(cls.tmpormdir.name, cls.MODEL_PACKAGE)
        # Set up the package for testing Django models
        os.mkdir(pkgbase)
        open(os.path.join(pkgbase, "__init__.py"), "w").close()  # noqa: PLW1514
        with open(os.path.join(pkgbase, "apps.py"), "w") as f:  # noqa: PLW1514
            print(
                APPS_PY.format(name=cls.MODEL_PACKAGE),
                file=f,
            )

        with open(os.path.join(pkgbase, "settings.py"), "w") as f:  # noqa: PLW1514
            cargs = cls.get_connect_args(database=cls.get_database_name())
            print(
                SETTINGS_PY.format(
                    appname=cls.MODEL_PACKAGE,
                    database=cargs["database"],
                    user=cargs["user"],
                    password=cargs["password"],
                    host=cargs["host"],
                    port=cargs["port"],
                ),
                file=f,
            )

        models = os.path.join(pkgbase, "models.py")
        gen = DjangoModGen(out=models)  # type: ignore[no-untyped-call]
        gen.render_models(cls.spec)  # type: ignore[no-untyped-call]

#
# This source file is part of the Gel open source project.
#
# Copyright 2008-present Gel Data Inc. and the Gel contributors.
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


import sys

if sys.version_info < (3, 10):
    raise RuntimeError("gel requires Python 3.10 or greater")

import os
import os.path
import pathlib
import re
import subprocess
import typing

import setuptools
from setuptools import extension as setuptools_extension
from setuptools.command import build_py as setuptools_build_py
from setuptools.command import sdist as setuptools_sdist
from setuptools.command import build_ext as setuptools_build_ext


CYTHON_DEPENDENCY = "Cython(>=3.1.1,<4.0.0)"

if flag := os.environ.get('EDGEDB_OPT_CFLAG'):
    CFLAGS = [flag]
else:
    CFLAGS = ["-O2"]
SYSTEM = sys.platform

if SYSTEM != "win32":
    CFLAGS.extend(
        [
            "-std=gnu99",
            "-fsigned-char",
            "-Wall",
            "-Wsign-compare",
            "-Wconversion",
            # See also: https://github.com/cython/cython/issues/5240
            "-Wno-error=incompatible-pointer-types",
        ]
    )

if SYSTEM == "darwin":
    # Lots of warnings from the standard library on macOS 10.14
    CFLAGS.extend(["-Wno-nullability-completeness"])

_ROOT = pathlib.Path(__file__).parent


with open(str(_ROOT / "gel" / "_version.py")) as f:
    for line in f:
        if line.startswith("__version__ ="):
            _, _, version = line.partition("=")
            VERSION = version.strip(" \n'\"")
            break
    else:
        raise RuntimeError("unable to read the version from gel/_version.py")


if (_ROOT / ".git").is_dir() and "dev" in VERSION:
    # This is a git checkout, use git to
    # generate a precise version.
    def git_commitish():
        env = {}
        v = os.environ.get("PATH")
        if v is not None:
            env["PATH"] = v

        git = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            env=env,
            cwd=str(_ROOT),
            stdout=subprocess.PIPE,
        )
        if git.returncode == 0:
            commitish = git.stdout.strip().decode("ascii")
        else:
            commitish = "unknown"

        return commitish

    VERSION += "+" + git_commitish()[:7]


class VersionMixin:
    distribution: setuptools.dist.Distribution

    def _fix_version(self, filename):
        # Replace edgedb.__version__ with the actual version
        # of the distribution (possibly inferred from git).

        with open(str(filename)) as f:
            content = f.read()

        version_re = r"(.*__version__\s*=\s*)'[^']+'(.*)"
        repl = r"\1'{}'\2".format(self.distribution.metadata.version)
        content = re.sub(version_re, repl, content)

        with open(str(filename), "w") as f:
            f.write(content)


class sdist(setuptools_sdist.sdist, VersionMixin):
    def make_release_tree(self, base_dir, files):
        super().make_release_tree(base_dir, files)
        self._fix_version(pathlib.Path(base_dir) / "gel" / "_version.py")


class build_py(setuptools_build_py.build_py, VersionMixin):
    def build_module(self, module, module_file, package):
        outfile, copied = super().build_module(module, module_file, package)

        if module == "_version" and package == "gel":
            self._fix_version(outfile)

        return outfile, copied


class build_ext(setuptools_build_ext.build_ext):
    user_options = setuptools_build_ext.build_ext.user_options + [
        (
            "cython-always",
            None,
            "run cythonize() even if .c files are present",
        ),
        (
            "cython-annotate",
            None,
            "Produce a colorized HTML version of the Cython source.",
        ),
        ("cython-directives=", None, "Cython compiler directives"),
    ]

    def initialize_options(self):
        # initialize_options() may be called multiple times on the
        # same command object, so make sure not to override previously
        # set options.
        if getattr(self, "_initialized", False):
            return

        super(build_ext, self).initialize_options()

        if os.environ.get("GEL_PYTHON_DEBUG"):
            self.cython_always = True
            self.cython_annotate = True
            self.cython_directives = "linetrace=True"
            self.define = "PG_DEBUG,CYTHON_TRACE,CYTHON_TRACE_NOGIL"
            self.debug = True
        else:
            self.cython_always = False
            self.cython_annotate = None
            self.cython_directives = None
            self.debug = False

    def finalize_options(self) -> None:
        # finalize_options() may be called multiple times on the
        # same command object, so make sure not to override previously
        # set options.
        if getattr(self, "_initialized", False):
            return

        need_cythonize = self.cython_always
        cfiles = {}

        for extension in self.distribution.ext_modules:
            for i, sfile in enumerate(extension.sources):
                if sfile.endswith(".pyx"):
                    prefix, ext = os.path.splitext(sfile)
                    cfile = prefix + ".c"

                    if os.path.exists(cfile) and not self.cython_always:
                        extension.sources[i] = cfile
                    else:
                        if os.path.exists(cfile):
                            cfiles[cfile] = os.path.getmtime(cfile)
                        else:
                            cfiles[cfile] = 0
                        need_cythonize = True

        if need_cythonize:
            import pkg_resources

            # Double check Cython presence in case setup_requires
            # didn't go into effect (most likely because someone
            # imported Cython before setup_requires injected the
            # correct egg into sys.path.
            try:
                import Cython
            except ImportError as e:
                raise RuntimeError(
                    f"please install {CYTHON_DEPENDENCY} "
                    f"to compile gel from source"
                ) from e

            cython_dep = pkg_resources.Requirement.parse(CYTHON_DEPENDENCY)
            if Cython.__version__ not in cython_dep:
                raise RuntimeError(
                    "gel requires {}, got Cython=={}".format(
                        CYTHON_DEPENDENCY, Cython.__version__
                    )
                )

            from Cython.Build import cythonize

            directives: typing.Dict[str, typing.Union[str, bool]] = {
                "language_level": "3"
            }
            if self.cython_directives:
                for directive in self.cython_directives.split(","):
                    k, _, v = directive.partition("=")
                    if v.lower() == "false":
                        v = False
                    elif v.lower() == "true":
                        v = True

                    directives[k] = v

            self.distribution.ext_modules[:] = cythonize(
                self.distribution.ext_modules,
                compiler_directives=directives,
                annotate=self.cython_annotate,
            )

        super(build_ext, self).finalize_options()


INCLUDE_DIRS = [
    "gel/pgproto/",
    "gel/datatypes",
]


setup_requires = []

if (
    not (_ROOT / "gel" / "protocol" / "protocol.c").exists()
    or "--cython-always" in sys.argv
):
    # No Cython output, require Cython to build.
    setup_requires.append(CYTHON_DEPENDENCY)


setuptools.setup(
    name="gel",
    version=VERSION,
    ext_modules=[
        setuptools_extension.Extension(
            "gel.pgproto.pgproto",
            ["gel/pgproto/pgproto.pyx"],
            extra_compile_args=CFLAGS,
        ),
        setuptools_extension.Extension(
            "gel.datatypes.datatypes",
            [
                "gel/datatypes/args.c",
                "gel/datatypes/record_desc.c",
                "gel/datatypes/namedtuple.c",
                "gel/datatypes/object.c",
                "gel/datatypes/record.c",
                "gel/datatypes/hash.c",
                "gel/datatypes/repr.c",
                "gel/datatypes/comp.c",
                "gel/datatypes/datatypes.pyx",
            ],
            extra_compile_args=CFLAGS,
        ),
        setuptools_extension.Extension(
            "gel.protocol.protocol",
            ["gel/protocol/protocol.pyx"],
            extra_compile_args=CFLAGS,
            include_dirs=INCLUDE_DIRS,
        ),
        setuptools_extension.Extension(
            "gel.protocol.asyncio_proto",
            ["gel/protocol/asyncio_proto.pyx"],
            extra_compile_args=CFLAGS,
            include_dirs=INCLUDE_DIRS,
        ),
        setuptools_extension.Extension(
            "gel.protocol.blocking_proto",
            ["gel/protocol/blocking_proto.pyx"],
            extra_compile_args=CFLAGS,
            include_dirs=INCLUDE_DIRS,
        ),
    ],
    cmdclass={"build_ext": build_ext},
)

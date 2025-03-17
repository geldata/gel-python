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

# This file MUST NOT contain anything but the __version__ assignment.
#
# When making a release, change the value of __version__
# to an appropriate value, and open a pull request against
# the correct branch (master if making a new feature release).
# The commit message MUST contain a properly formatted release
# log, and the commit must be signed.
#
# The release automation will: build and test the packages for the
# supported platforms, publish the packages on PyPI, merge the PR
# to the target branch, create a Git tag pointing to the commit.

__version__ = "3.1.0b1"

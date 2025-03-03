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


import pathlib
import re


if __name__ == '__main__':
    this = pathlib.Path(__file__)

    errors_fn = this.parent.parent / 'gel' / 'errors' / '__init__.py'
    init_fn = this.parent.parent / 'gel' / '__init__.py'

    with open(errors_fn, 'rt') as f:
        errors_txt = f.read()

    names = re.findall(r'^class\s+(?P<name>\w+)', errors_txt, re.M)
    names_list = '\n'.join(f'    {name},' for name in names)
    all_list = '\n'.join(f'    "{name}",' for name in names)
    code = (
        f'''from .errors import (\n{names_list}\n)\n'''
        f'''\n__all__.extend([\n{all_list}\n])\n'''
    ).splitlines()

    with open(init_fn, 'rt') as f:
        lines = f.read().splitlines()
        start = end = -1
        for no, line in enumerate(lines):
            if line.startswith('# <ERRORS-AUTOGEN>'):
                start = no
            elif line.startswith('# </ERRORS-AUTOGEN>'):
                end = no

    if start == -1:
        raise RuntimeError('could not find the <ERRORS-AUTOGEN> tag')

    if end == -1:
        raise RuntimeError('could not find the </ERRORS-AUTOGEN> tag')

    lines[start + 1:end] = code

    with open(init_fn, 'w') as f:
        f.write('\n'.join(lines))
        f.write('\n')

# Copyright 2014 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Contains benchmark imports and a list of benchmarks.

All modules within this package are considered benchmarks, and are loaded
dynamically. Add non-benchmark code to other packages.
"""

import importlib
import pkgutil


def _LoadModulesForPath(path, package_prefix=None):
  """Load all modules on 'path', with prefix 'package_prefix'.

  Example usage:
    _LoadModulesForPath(__path__, __name__)

  Args:
    path: Path containing python modules.
    package_prefix: prefix (e.g., package name) to prefix all modules.
      'path' and 'package_prefix' will be joined with a '.'.
  Yields:
    Imported modules.
  """
  prefix = ''
  if package_prefix:
    prefix = package_prefix + '.'
  module_iter = pkgutil.iter_modules(path, prefix=prefix)
  for _, modname, ispkg in module_iter:
    if not ispkg:
      yield importlib.import_module(modname)


def _LoadBenchmarks():
  return list(_LoadModulesForPath(__path__, __name__))


BENCHMARKS = _LoadBenchmarks()

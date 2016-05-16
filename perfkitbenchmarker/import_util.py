# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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

"""Utilities for dynamically importing python files."""

import importlib
import pkgutil


def LoadModulesForPath(path, package_prefix=None):
  """Load all modules on 'path', with prefix 'package_prefix'.

  Example usage:
    LoadModulesForPath(__path__, __name__)

  Args:
    path: Path containing python modules.
    package_prefix: prefix (e.g., package name) to prefix all modules.
      'path' and 'package_prefix' will be joined with a '.'.
  Yields:
    Imported modules.
  """
  prefix = package_prefix + '.' if package_prefix else ''
  # If iter_modules is invoked within a zip file, the zipimporter adds the
  # prefix to the names of archived modules, but not archived packages. Because
  # the prefix is necessary to correctly import a package, this behavior is
  # undesirable, so do not pass the prefix to iter_modules. Instead, apply it
  # explicitly afterward.
  for _, modname, _ in pkgutil.iter_modules(path):
    # Skip recursively listed modules (e.g. 'subpackage.module').
    if '.' not in modname:
      yield importlib.import_module(prefix + modname)

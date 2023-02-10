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
import logging
import pkgutil


def LoadModulesForPath(path, package_prefix=None):
  """Recursively load all modules on 'path', with prefix 'package_prefix'.

  Example usage:
    LoadModulesForPath(__path__, __name__)

  Args:
    path: Path containing python modules.
    package_prefix: prefix (e.g., package name) to prefix all modules. 'path'
      and 'package_prefix' will be joined with a '.'.

  Yields:
    Imported modules.
  """

  prefix = package_prefix + '.' if package_prefix else ''
  def LogImportError(modname):
    logging.exception('Error importing module %s', modname)

  for _, modname, is_pkg in pkgutil.walk_packages(
      path, prefix=prefix, onerror=LogImportError
  ):
    if not is_pkg:
      yield importlib.import_module(modname)

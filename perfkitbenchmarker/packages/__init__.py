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
"""Contains package imports and a dictionary of package names and modules.

All modules within this package are considered packages, and are loaded
dynamically. Add non-package code to other packages.

Packages should, at a minimum, define install functions for each type of
package manager (e.g. YumInstall(vm) and AptInstall(vm)).
They may also define functions that return the path to a configuration file
(e.g. AptGetPathToConfig(vm)) and functions that return the linux service
name (e.g. YumGetServiceName(vm)). If the package only installs
packages through the package manager or places files in the temp directory
on the VM (~/pkb/), then it does not need to define an uninstall function.
If the package manually places files in other locations (e.g. /user/bin), then
it also needs to define uninstall functions (e.g. YumUninstall(vm)).

All functions in each package module should be prefixed with the type of package
manager, and all functions should accept a BaseVirtualMachine object as their
only arguments.

See perfkitbenchmarker/package_managers.py for more information on how to use
packages in benchmarks.
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


def _LoadPackages():
  return dict([(module.__name__.split('.')[-1], module)
               for module in _LoadModulesForPath(__path__, __name__)])


PACKAGES = _LoadPackages()

# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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

Packages should, at a minimum, define an install function (Install(vm)).
If the package manually places files in locations other than the VM's temp
directory, then it also needs to define an uninstall function (Uninstall(vm)).
"""

from perfkitbenchmarker import import_util


def _LoadPackages():
  """Imports all package modules and returns a dictionary of packages.

  This imports all package modules in this directory and then creates a
  mapping from module names to the modules themselves and returns it.
  """
  return {module.__name__.split('.')[-1]: module for module in
          import_util.LoadModulesForPath(__path__, __name__)}


PACKAGES = _LoadPackages()

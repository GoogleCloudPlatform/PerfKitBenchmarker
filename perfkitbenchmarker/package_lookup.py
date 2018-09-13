# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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

"""Function to lookup modules from package names.

PackageModule: Returns a package module given its name.

This module works around a circular import issue where we cannot import
benchmark_sets.py directly into virtual_machine.py. After SetUpPKB is called,
package_lookup.PackageModule is equivalent to benchmark_sets.PackageModule.
"""

from perfkitbenchmarker import errors

_global_package_module_function = None


def SetPackageModuleFunction(function):
  """Sets the function called by PackageModule; See benchmark_sets.py."""
  global _global_package_module_function
  _global_package_module_function = function


def PackageModule(package_name):
  """Finds the module for a benchmark by name.

  Args:
    package_name: The name of the package.

  Returns:
    The package's module, or None if the package is invalid.
  """
  if not _global_package_module_function:
    raise errors.Setup.InvalidSetupError(
        'Cannot call package_lookup.py; Was SetUpPKB called?')
  return _global_package_module_function(package_name)

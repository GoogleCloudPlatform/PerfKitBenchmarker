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
"""Contains package imports and a dictionary of package names and modules.

All modules within this package are considered packages, and are loaded
dynamically. Add non-package code to other packages.

Packages should, at a minimum, define install functions for each type of
package manager (e.g. YumInstall(vm) and AptInstall(vm)).
They may also define functions that return the path to a configuration file
(e.g. AptGetPathToConfig(vm)) and functions that return the linux service
name (e.g. YumGetServiceName(vm)). If the package only installs
packages through the package manager, then it does not need to define an
uninstall function.  If the package manually places files in other locations
(e.g. /usr/bin, /opt/pkb), then it also needs to define uninstall functions
(e.g.  YumUninstall(vm)).

Package installation should persist across reboots.

All functions in each package module should be prefixed with the type of package
manager, and all functions should accept a BaseVirtualMachine object as their
only arguments.

See perfkitbenchmarker/package_managers.py for more information on how to use
packages in benchmarks.
"""

from perfkitbenchmarker import import_util


# Place to install stuff. Persists across reboots.
INSTALL_DIR = '/opt/pkb'


def _LoadPackages():
  packages = dict([(module.__name__.split('.')[-1], module) for module in
                   import_util.LoadModulesForPath(__path__, __name__)])
  packages.update(packages['docker'].CreateImagePackages())
  return packages


PACKAGES = _LoadPackages()


def GetPipPackageVersion(vm, package_name):
  """This function returns the version of a pip package installed on a vm.

  Args:
    vm: the VM the package is installed on.
    package_name: the name of the package.

  Returns:
    The version string of the package.
  """
  version, _ = vm.RemoteCommand('pip show %s |grep Version' % package_name)
  return version

# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing lmod installation.

Lmod: https://lmod.readthedocs.io/en/latest/

"Lmod is a Lua based module system that easily handles the MODULEPATH
 Hierarchical problem."
"""

from perfkitbenchmarker import errors


def YumInstall(vm) -> None:
  vm.InstallPackages('Lmod')


def AptInstall(vm) -> None:
  vm.InstallPackages('lmod')
  _FixLuaInstall(vm)


def _FixLuaInstall(vm) -> None:
  """Fixes Lua 5.2 install if required.

  https://bugs.launchpad.net/ubuntu/+source/lua-posix/+bug/1752082

  lua-posix package broken in version that comes with Ubuntu1804.  Symlinks
  lua's posix_c.so to posix.so.

  Args:
    vm: the remote VM to fix lua install.

  Raises:
    InvalidSetupError: If lua could not be setup correctly.
  """
  # will need to change from 5.2 when necessary
  lua_dir = '/usr/lib/x86_64-linux-gnu/lua/5.2'
  lua_posix_c = f'{lua_dir}/posix_c.so'
  missing_dir_error = f'Lua directory {lua_dir} does not exist'
  missing_file_error = f'Lua posix_c file {lua_posix_c} missing'
  if not vm.TryRemoteCommand(f'test -d {lua_dir}'):
    raise errors.Setup.InvalidSetupError(missing_dir_error)
  if not vm.TryRemoteCommand(f'test -f {lua_posix_c}'):
    raise errors.Setup.InvalidSetupError(missing_file_error)
  vm.RemoteCommand(f'sudo ln -s {lua_posix_c} {lua_dir}/posix.so')

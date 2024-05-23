# Copyright 2024 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing KeyDB installation and cleanup functions.

See https://docs.keydb.dev/blog/2020/04/15/blog-post/
and https://github.com/Snapchat/KeyDB?tab=readme-ov-file
"""

import logging
from typing import Any, Dict

from absl import flags
from perfkitbenchmarker import linux_packages

SERVER_VCPU_RATIO = 0.75

_KEYDB_RELEASE = flags.DEFINE_string(
    'keydb_release', 'RELEASE_6', 'Git Release of KeyDB to use.'
)
_KEYDB_SERVER_THREADS = flags.DEFINE_integer(
    'keydb_server_threads',
    0,
    'Number of server threads to use. If set to 0, uses '
    f'num_cpus * {SERVER_VCPU_RATIO}',
    lower_bound=0,
)

# Default port for KeyDB
DEFAULT_PORT = 6379
FLAGS = flags.FLAGS
# TODO(user): Move to Snapchat repro
KEYDB_GIT = 'https://github.com/EQ-Alpha/KeyDB.git'


def GetKeydbDir() -> str:
  return f'{linux_packages.INSTALL_DIR}/KeyDB'


def _GetNumServerThreads(vm) -> int:
  num_server_threads = _KEYDB_SERVER_THREADS.value
  if num_server_threads == 0 and vm is not None:
    num_server_threads = vm.NumCpusForBenchmark() * SERVER_VCPU_RATIO
  assert num_server_threads >= 0, 'num_server_threads must be >=0.'
  return num_server_threads


def _Install(vm) -> None:
  """Installs the keydb package on the VM."""
  vm.Install('build_tools')
  vm.Install('wget')
  vm.RemoteCommand(f'cd {linux_packages.INSTALL_DIR}; git clone {KEYDB_GIT}')
  vm.RemoteCommand(
      f'cd {GetKeydbDir()} && '
      'git fetch --all && '
      f'git checkout {_KEYDB_RELEASE.value} && '
      'git pull'
  )
  vm.RemoteCommand(
      f'cd {GetKeydbDir()} && '
      # 'make BUILD_TLS=yes && '
      'make && '
      'sudo make install'
  )


def YumInstall(_) -> None:
  """Installs the keydb package on the VM."""
  # TODO(user): Implement; see https://docs.keydb.dev/docs/build/
  raise NotImplementedError()


def AptInstall(vm) -> None:
  """Installs the keydb package on the VM."""
  vm.RemoteCommand('sudo apt update')
  vm.InstallPackages(
      'build-essential nasm autotools-dev autoconf libjemalloc-dev tcl tcl-dev'
      ' uuid-dev libcurl4-openssl-dev git'
  )
  vm.InstallPackages('libssl-dev')  # not mentioned in docs
  _Install(vm)


def _BuildStartCommand(vm) -> str:
  """Returns the run command used to start KeyDB.

  See https://docs.keydb.dev/blog/2020/04/15/blog-post/#keydb
  and https://github.com/Snapchat/KeyDB/blob/main/keydb.conf

  Args:
    vm: The KeyDB server VM.

  Returns:
    A command that can be used to start KeyDB in the background.
  """
  keydb_dir = GetKeydbDir()
  cmd = 'nohup sudo {keydb_dir}/src/keydb-server {args} &> /dev/null &'
  cmd_args = [
      '--protected-mode no',
      f'--server-threads {_GetNumServerThreads(vm)}',
      '--save',
  ]
  # TODO(spencerkim): Add TLS support
  # TODO(spencerkim): Add --server-thread-affinity argument.
  # TODO(spencerkim): Add --maxmemory and eviction arguments.
  # TODO(user): Consider adding persistence support
  # https://docs.keydb.dev/docs/persistence/

  return cmd.format(keydb_dir=keydb_dir, args=' '.join(cmd_args))


def Start(vm) -> None:
  """Start KeyDB server process."""
  # Redis tuning parameters (applicable to KeyDB which is a fork of Redis), see
  # https://www.techandme.se/performance-tips-for-redis-cache-server/.
  # This command works on 2nd generation of VMs only.
  update_sysctl = vm.TryRemoteCommand(
      'echo "'
      'vm.overcommit_memory = 1\n'
      'net.core.somaxconn = 65535\n'
      '" | sudo tee -a /etc/sysctl.conf'
  )
  # /usr/sbin/sysctl is not applicable on certain distros.
  commit_sysctl = vm.TryRemoteCommand(
      'sudo /usr/sbin/sysctl -p || sudo sysctl -p'
  )
  if not (update_sysctl and commit_sysctl):
    logging.info('Fail to optimize overcommit_memory and socket connections.')

  vm.RemoteCommand(_BuildStartCommand(vm))


def GetMetadata(vm) -> Dict[str, Any]:
  num_processes = _GetNumServerThreads(vm)
  return {
      'keydb_release': _KEYDB_RELEASE.value,
      'keydb_server_threads': num_processes,
  }

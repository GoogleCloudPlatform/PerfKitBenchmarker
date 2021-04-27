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


"""Module containing redis installation and cleanup functions."""

from typing import Any, Dict
from absl import flags
from perfkitbenchmarker import linux_packages

_VERSION = flags.DEFINE_string('redis_server_version', '6.2.1',
                               'Version of redis server to use.')
flags.register_validator(
    'redis_server_version',
    lambda version: int(version.split('.')[0]) >= 6,
    message='Redis version must be 6 or greater.')
_IO_THREADS = flags.DEFINE_integer(
    'redis_server_io_threads', 4, 'Only supported for redis version >= 6, the '
    'number of redis server IO threads to use.')
_IO_THREADS_DO_READS = flags.DEFINE_bool(
    'redis_memtier_io_threads_do_reads', False,
    'If true, makes both reads and writes use IO threads instead of just '
    'writes.')

DEFAULT_PORT = 6379
REDIS_PID_FILE = 'redis.pid'
FLAGS = flags.FLAGS
REDIS_GIT = 'https://github.com/antirez/redis.git'


def _GetRedisTarName() -> str:
  return f'redis-{_VERSION.value}.tar.gz'


def GetRedisDir() -> str:
  return f'{linux_packages.INSTALL_DIR}/redis'


def _Install(vm) -> None:
  """Installs the redis package on the VM."""
  vm.Install('build_tools')
  vm.Install('wget')
  vm.RemoteCommand(f'cd {linux_packages.INSTALL_DIR}; git clone {REDIS_GIT}')
  vm.RemoteCommand(
      f'cd {GetRedisDir()} && git checkout {_VERSION.value} && make')


def YumInstall(vm) -> None:
  """Installs the redis package on the VM."""
  vm.InstallPackages('tcl-devel')
  _Install(vm)


def AptInstall(vm) -> None:
  """Installs the redis package on the VM."""
  vm.InstallPackages('tcl-dev')
  _Install(vm)


def _BuildStartCommand() -> str:
  """Returns the run command used to start the redis server."""
  redis_dir = GetRedisDir()
  cmd = 'nohup sudo {redis_dir}/src/redis-server {args} &> /dev/null &'
  cmd_args = [
      '--protected-mode no',
      '--save ""',
      f'--io-threads {_IO_THREADS.value}',
  ]
  do_reads = 'yes' if _IO_THREADS_DO_READS.value else 'no'
  cmd_args.append(f'--io-threads-do-reads {do_reads}')
  return cmd.format(redis_dir=redis_dir, args=' '.join(cmd_args))


def Start(vm) -> None:
  """Start redis server process."""
  # Redis tuning parameters, see
  # https://www.techandme.se/performance-tips-for-redis-cache-server/.
  vm.RemoteCommand(
      'echo "'
      'vm.overcommit_memory = 1\n'
      'net.core.somaxconn = 65535\n'
      '" | sudo tee -a /etc/sysctl.conf')
  vm.RemoteCommand('sudo sysctl -p')
  vm.RemoteCommand(_BuildStartCommand())


def GetMetadata() -> Dict[str, Any]:
  return {
      'redis_server_version': _VERSION.value,
      'redis_server_io_threads': _IO_THREADS.value,
      'redis_server_io_threads_do_reads': _IO_THREADS_DO_READS.value,
  }

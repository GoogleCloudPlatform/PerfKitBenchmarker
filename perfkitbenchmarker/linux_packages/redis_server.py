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

from absl import flags
from perfkitbenchmarker import linux_packages
from six.moves import range

_NUM_PROCESSES = flags.DEFINE_integer(
    'redis_total_num_processes',
    1,
    'Total number of redis server processes.',
    lower_bound=1)
_ENABLE_AOF = flags.DEFINE_boolean(
    'redis_enable_aof', False,
    'Enable append-only file (AOF) with appendfsync always.')
_VERSION = flags.DEFINE_string('redis_server_version', '5.0.5',
                               'Version of redis server to use.')

REDIS_FIRST_PORT = 6379
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


def Configure(vm) -> None:
  """Configure redis server."""
  redis_dir = GetRedisDir()
  vm.RemoteCommand(
      f'sudo sed -i "s/bind/#bind/g" {redis_dir}/redis.conf')
  vm.RemoteCommand(
      'sudo sed -i "s/protected-mode yes/protected-mode no/g" '
      f'{redis_dir}/redis.conf')
  sed_cmd = (
      r"sed -i -e '/^save /d' -e 's/# *save \"\"/save \"\"/' "
      f"{redis_dir}/redis.conf")
  vm.RemoteCommand(sed_cmd)
  if _ENABLE_AOF.value:
    vm.RemoteCommand(
        r'sed -i -e "s/appendonly no/appendonly yes/g" '
        f'{redis_dir}/redis.conf')
    vm.RemoteCommand(
        r'sed -i -e "s/appendfsync everysec/# appendfsync everysec/g" '
        rf'{redis_dir}/redis.conf')
    vm.RemoteCommand(
        r'sed -i -e "s/# appendfsync always/appendfsync always/g" '
        rf'{redis_dir}/redis.conf')
  for i in range(_NUM_PROCESSES.value):
    port = REDIS_FIRST_PORT + i
    vm.RemoteCommand(
        f'cp {redis_dir}/redis.conf {redis_dir}/redis-{port}.conf')
    vm.RemoteCommand(
        rf'sed -i -e "s/port {REDIS_FIRST_PORT}/port {port}/g" '
        f'{redis_dir}/redis-{port}.conf')


def Start(vm) -> None:
  """Start redis server process."""
  for i in range(_NUM_PROCESSES.value):
    port = REDIS_FIRST_PORT + i
    redis_dir = GetRedisDir()
    vm.RemoteCommand(
        f'nohup sudo {redis_dir}/src/redis-server '
        f'{redis_dir}/redis-{port}.conf '
        f'&> /dev/null & echo $! > {redis_dir}/{REDIS_PID_FILE}-{port}')


def Cleanup(vm) -> None:
  """Remove redis."""
  vm.RemoteCommand('sudo pkill redis-server')

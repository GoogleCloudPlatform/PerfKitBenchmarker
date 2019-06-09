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

from perfkitbenchmarker import flags
from perfkitbenchmarker.linux_packages import INSTALL_DIR


flags.DEFINE_integer('redis_total_num_processes', 1,
                     'Total number of redis server processes.',
                     lower_bound=1)
flags.DEFINE_boolean('redis_enable_aof', False,
                     'Enable append-only file (AOF) with appendfsync always.')
flags.DEFINE_string('redis_server_version', '5.0.5',
                    'Version of redis server to use.')


REDIS_FIRST_PORT = 6379
REDIS_PID_FILE = 'redis.pid'
FLAGS = flags.FLAGS


def _GetRedisTarName():
  return 'redis-%s.tar.gz' % FLAGS.redis_server_version


def GetRedisDir():
  return '%s/redis-%s' % (INSTALL_DIR, FLAGS.redis_server_version)


def _Install(vm):
  """Installs the redis package on the VM."""
  vm.Install('build_tools')
  vm.Install('wget')
  redis_url = 'http://download.redis.io/releases/' + _GetRedisTarName()
  vm.RemoteCommand('wget %s -P %s' % (redis_url, INSTALL_DIR))
  vm.RemoteCommand('cd %s && tar xvfz %s' % (INSTALL_DIR, _GetRedisTarName()))
  vm.RemoteCommand('cd %s && make' % GetRedisDir())


def YumInstall(vm):
  """Installs the redis package on the VM."""
  vm.InstallPackages('tcl-devel')
  _Install(vm)


def AptInstall(vm):
  """Installs the redis package on the VM."""
  vm.InstallPackages('tcl-dev')
  _Install(vm)


def Configure(vm):
  """Configure redis server."""
  sed_cmd = (
      r"sed -i -e '/^save /d' -e 's/# *save \"\"/save \"\"/' "
      "{0}/redis.conf").format(GetRedisDir())
  vm.RemoteCommand(
      'sudo sed -i "s/bind/#bind/g" {0}/redis.conf'.format(GetRedisDir()))
  vm.RemoteCommand(
      'sudo sed -i "s/protected-mode yes/protected-mode no/g" {0}/redis.conf'.
      format(GetRedisDir()))
  vm.RemoteCommand(sed_cmd)
  if FLAGS.redis_enable_aof:
    vm.RemoteCommand(
        r'sed -i -e "s/appendonly no/appendonly yes/g" {0}/redis.conf'.format(
            GetRedisDir()))
    vm.RemoteCommand((
        r'sed -i -e "s/appendfsync everysec/# appendfsync everysec/g" '
        r'{0}/redis.conf'
    ).format(GetRedisDir()))
    vm.RemoteCommand((
        r'sed -i -e "s/# appendfsync always/appendfsync always/g" '
        r'{0}/redis.conf'
    ).format(GetRedisDir()))
  for i in range(FLAGS.redis_total_num_processes):
    port = REDIS_FIRST_PORT + i
    vm.RemoteCommand(
        ('cp {0}/redis.conf {0}/redis-{1}.conf').format(GetRedisDir(), port))
    vm.RemoteCommand(
        r'sed -i -e "s/port %d/port %d/g" %s/redis-%d.conf' %
        (REDIS_FIRST_PORT, port, GetRedisDir(), port))


def Start(vm):
  """Start redis server process."""
  for i in range(FLAGS.redis_total_num_processes):
    port = REDIS_FIRST_PORT + i
    vm.RemoteCommand(
        ('nohup sudo {0}/src/redis-server {0}/redis-{1}.conf '
         '&> /dev/null & echo $! > {0}/{2}-{1}').format(
             GetRedisDir(), port, REDIS_PID_FILE))


def Cleanup(vm):
  """Remove redis."""
  vm.RemoteCommand('sudo pkill redis-server')

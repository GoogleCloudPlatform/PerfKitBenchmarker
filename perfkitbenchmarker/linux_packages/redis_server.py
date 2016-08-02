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
from perfkitbenchmarker import vm_util


flags.DEFINE_integer('redis_total_num_processes', 1,
                     'Total number of redis server processes.',
                     lower_bound=1)


REDIS_VERSION = '2.8.9'
REDIS_TAR = 'redis-%s.tar.gz' % REDIS_VERSION
REDIS_DIR = '%s/redis-%s' % (vm_util.VM_TMP_DIR, REDIS_VERSION)
REDIS_URL = 'http://download.redis.io/releases/' + REDIS_TAR
REDIS_FIRST_PORT = 6379
REDIS_PID_FILE = 'redis.pid'
FLAGS = flags.FLAGS


def _Install(vm):
  """Installs the redis package on the VM."""
  vm.Install('build_tools')
  vm.Install('wget')
  vm.RemoteCommand('wget %s -P %s' % (REDIS_URL, vm_util.VM_TMP_DIR))
  vm.RemoteCommand('cd %s && tar xvfz %s' % (vm_util.VM_TMP_DIR, REDIS_TAR))
  vm.RemoteCommand('cd %s && make' % REDIS_DIR)


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
  sed_cmd = (r"sed -i -e '/^save /d' -e 's/# *save \"\"/save \"\"/' "
             "{0}/redis.conf").format(REDIS_DIR)
  vm.RemoteCommand(sed_cmd)
  for i in range(FLAGS.redis_total_num_processes):
    port = REDIS_FIRST_PORT + i
    vm.RemoteCommand(
        ('cp {0}/redis.conf {0}/redis-{1}.conf').format(REDIS_DIR, port))
    vm.RemoteCommand(
        r'sed -i -e "s/port %d/port %d/g" %s/redis-%d.conf' %
        (REDIS_FIRST_PORT, port, REDIS_DIR, port))


def Start(vm):
  """Start redis server process."""
  for i in range(FLAGS.redis_total_num_processes):
    port = REDIS_FIRST_PORT + i
    vm.RemoteCommand(
        ('nohup sudo {0}/src/redis-server {0}/redis-{1}.conf '
         '&> /dev/null & echo $! > {0}/{2}-{1}').format(
             REDIS_DIR, port, REDIS_PID_FILE))


def Cleanup(vm):
  """Remove redis."""
  vm.RemoteCommand('sudo pkill redis-server')

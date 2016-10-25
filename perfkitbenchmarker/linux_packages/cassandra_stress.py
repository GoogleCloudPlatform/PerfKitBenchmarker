# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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

"""Installs the cassandra-stress tool.

Cassandra homepage: http://cassandra.apache.org
cassandra-stress documentation:
https://docs.datastax.com/en/cassandra/2.1/cassandra/tools/toolsCStress_t.html
"""

import posixpath

from perfkitbenchmarker.linux_packages import cassandra
from perfkitbenchmarker.linux_packages import INSTALL_DIR

CASSANDRA_DIR = posixpath.join(INSTALL_DIR, 'cassandra')


def YumInstall(vm):
  """Installs cassandra-stress on the VM."""
  cassandra.YumInstall(vm)


def AptInstall(vm):
  """Installs cassandra-stress on the VM."""
  cassandra.AptInstall(vm)


def JujuInstall(vm, vm_group_name):
  """Installs the cassandra-stress charm on the VM."""
  vm.JujuDeploy('cs:~marcoceppi/trusty/cassandra-stress', vm_group_name)

  # The assumption is that cassandra-stress will always be installed
  # alongside cassandra
  vm.JujuRelate('cassandra', 'cassandra-stress')

  # Wait for the cassandra-stress to install and configure
  vm.JujuWait()

  for unit in vm.units:
    # Make sure the cassandra/conf dir is created, since we're skipping
    # the manual installation to /opt/pkb.
    remote_path = posixpath.join(CASSANDRA_DIR, 'conf')
    unit.RemoteCommand('mkdir -p %s' % remote_path)

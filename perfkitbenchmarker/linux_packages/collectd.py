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
"""Builds collectd from source, installs to INSTALL_DIR.

https://collectd.org/

collectd is extremely configurable. We enable some basic monitoring, gathered
every 10s, saving in .csv format. See
perfkitbenchmarker/data/build_collectd.sh.j2 for configuration details.
"""

import posixpath

from perfkitbenchmarker import data
from perfkitbenchmarker.linux_packages import INSTALL_DIR

# TODO: Make collection interval configurable.
INTERVAL = 10
SCRIPT_NAME = 'build_collectd.sh.j2'
COLLECTD_URL = ('https://github.com/collectd/collectd/archive/'
                'collectd-5.5.0.tar.gz')
BUILD_DIR = posixpath.join(INSTALL_DIR, 'collectd-build')
CSV_DIR = posixpath.join(INSTALL_DIR, 'collectd-csv')
PREFIX = posixpath.join(INSTALL_DIR, 'collectd')
PID_FILE = posixpath.join(PREFIX, 'var', 'run', 'collectd.pid')


def _Install(vm):
  context = {
      'collectd_url': COLLECTD_URL,
      'build_dir': BUILD_DIR,
      'root_dir': PREFIX,
      'csv_dir': CSV_DIR,
      'interval': INTERVAL}

  remote_path = posixpath.join(
      INSTALL_DIR,
      posixpath.splitext(posixpath.basename(SCRIPT_NAME))[0])
  vm.RenderTemplate(data.ResourcePath(SCRIPT_NAME),
                    remote_path, context=context)
  vm.RemoteCommand('bash ' + remote_path)


def _Uninstall(vm):
  vm.RemoteCommand('kill $(cat {0})'.format(PID_FILE), ignore_failure=True)


def YumInstall(vm):
  """Installs collectd on 'vm'."""
  _Install(vm)


def AptInstall(vm):
  """Installs collectd on 'vm'."""
  _Install(vm)


def AptUninstall(vm):
  """Stops collectd on 'vm'."""
  _Uninstall(vm)


def YumUninstall(vm):
  """Stops collectd on 'vm'."""
  _Uninstall(vm)

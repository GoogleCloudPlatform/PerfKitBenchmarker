# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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
"""Installation function for the Cloud Bigtable YCSB HBase client connector."""

import os
import posixpath
from absl import flags
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import maven
from perfkitbenchmarker.linux_packages import ycsb

FLAGS = flags.FLAGS

CLIENT_VERSION = flags.DEFINE_string(
    'google_bigtable_client_version',
    '2.5.0',
    'Google Bigtable client version, e.g. "2.5.0"',
)

_MVN_DEPENDENCY_PLUGIN_VERSION = '3.3.0'
_MVN_DEPENDENCY_PLUGIN = 'org.apache.maven.plugins:maven-dependency-plugin'
_MVN_DEPENDENCY_COPY_GOAL = (
    f'{_MVN_DEPENDENCY_PLUGIN}:{_MVN_DEPENDENCY_PLUGIN_VERSION}'
    ':copy-dependencies')

_CLIENT_CONNECTOR_PROJECT_POM_TEMPLATE = (
    'cloudbigtable/pom-cbt-client-connector.xml.j2')


@vm_util.Retry()
def Install(vm):
  """Installs the Cloud Bigtable YCSB HBase client connector on the VM."""
  if not CLIENT_VERSION.value:
    raise errors.Setup.InvalidFlagConfigurationError(
        '--google_bigtable_client_version must be set')

  # Ensure YCSB is set up as that's where we copy the artifacts to.
  vm.Install('ycsb')
  vm.Install('maven')

  context = {
      'hbase_major_version': FLAGS.hbase_version.split('.')[0],
      'google_bigtable_client_version': CLIENT_VERSION.value,
  }

  # The POM template includes a single dependency on the cloud bigtable client
  # and when we use the :copy-dependencies goal, maven will download the client
  # and all its runtime dependencies the the specified folder.
  pom_template = data.ResourcePath(_CLIENT_CONNECTOR_PROJECT_POM_TEMPLATE)
  remote_pom_path = posixpath.join(
      ycsb.YCSB_DIR,
      os.path.basename(pom_template)[:-3])  # strip .j2 suffix
  vm.RenderTemplate(pom_template, remote_pom_path, context)

  dependency_install_dir = posixpath.join(ycsb.YCSB_DIR,
                                          f'{FLAGS.hbase_binding}-binding',
                                          'lib')
  maven_args = [
      '-f', remote_pom_path, _MVN_DEPENDENCY_COPY_GOAL,
      f'-DoutputDirectory={dependency_install_dir}'
  ]
  vm.RemoteCommand(maven.GetRunCommand(' '.join(maven_args)))

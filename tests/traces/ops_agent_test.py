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
"""Tests for ops_agent utility."""

import unittest
from absl import flags
from absl.testing import flagsaver
import mock
from perfkitbenchmarker import data
from tests import pkb_common_test_case
from perfkitbenchmarker.traces import ops_agent

FLAGS = flags.FLAGS

_COLLECTOR_FILE = 'collector.stdout'
_RUN_COMMAND = (
    f'sudo service google-cloud-ops-agent restart > {_COLLECTOR_FILE} 2>&1 &'
    ' echo $!'
)
OPS_AGENT_BASH_SCRIPT_PATH = './ops_agent/add-google-cloud-ops-agent-repo.sh'


class OpsAgentTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(OpsAgentTestCase, self).setUp()
    self.enter_context(mock.patch('os.path.isdir', return_value=True))
    self.collector = ops_agent._OpsAgentCollector()

  def testCollectorName(self):
    name = self.collector._CollectorName()
    self.assertEqual(name, 'ops_agent')

  def testRunCommand(self):
    actual_command = self.collector._CollectorRunCommand(None, _COLLECTOR_FILE)
    self.assertEqual(actual_command, _RUN_COMMAND)

  def testInstallCommand(self):
    vm = mock.Mock()
    self.collector._InstallCollector(vm)
    vm.RemoteCommand.assert_called_with(
        'sudo rm -rf ops_agent && mkdir ops_agent'
    )
    vm.RenderTemplate.assert_called_with(
        data.ResourcePath(OPS_AGENT_BASH_SCRIPT_PATH),
        OPS_AGENT_BASH_SCRIPT_PATH,
        context={
            'DEBIAN_REPO_NAME': str(FLAGS.ops_agent_debian_rapture_repo),
            'RPM_REPO_NAME': str(FLAGS.ops_agent_rpm_rapture_repo),
            'SUSE_REPO_NAME': str(FLAGS.ops_agent_suse_rapture_repo),
        },
    )
    vm.RobustRemoteCommand.assert_called_with(
        f'sudo bash {OPS_AGENT_BASH_SCRIPT_PATH} --also-install'
    )

    vm.PushFile.assert_not_called()

  @flagsaver.flagsaver(ops_agent_debian_rapture_repo='debian_repo')
  @flagsaver.flagsaver(ops_agent_rpm_rapture_repo='rpm_repo')
  @flagsaver.flagsaver(ops_agent_suse_rapture_repo='suse_repo')
  def testInstallCommandWithCustomRepoFlag(self):
    vm = mock.Mock()
    self.collector._InstallCollector(vm)
    vm.RemoteCommand.assert_called_with(
        'sudo rm -rf ops_agent && mkdir ops_agent'
    )
    vm.RenderTemplate.assert_called_with(
        data.ResourcePath(OPS_AGENT_BASH_SCRIPT_PATH),
        OPS_AGENT_BASH_SCRIPT_PATH,
        context={
            'DEBIAN_REPO_NAME': 'debian_repo',
            'RPM_REPO_NAME': 'rpm_repo',
            'SUSE_REPO_NAME': 'suse_repo',
        },
    )
    vm.RobustRemoteCommand.assert_called_with(
        f'sudo bash {OPS_AGENT_BASH_SCRIPT_PATH} --also-install'
    )

    vm.PushFile.assert_not_called()

  @flagsaver.flagsaver(ops_agent_config_file='config.yaml')
  def testInstallCommandWithConfig(self):
    vm = mock.Mock()
    self.collector._InstallCollector(vm)
    vm.PushFile.assert_called_with('config.yaml', '/tmp/config.yaml')
    vm.RemoteCommand.assert_called_with(
        'sudo cp /tmp/config.yaml /etc/google-cloud-ops-agent/config.yaml'
    )


if __name__ == '__main__':
  unittest.main()

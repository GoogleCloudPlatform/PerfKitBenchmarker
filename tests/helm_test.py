# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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
"""Unit tests for helm CLI command execution and chart installation."""

import unittest
from unittest import mock

from absl import flags
from absl.testing import flagsaver
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.resources.container_service import helm
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class HelmTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.mock_issue_command = self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand', return_value=('', '', 0))
    )

  @flagsaver.flagsaver(helm='/custom/helm', kubeconfig='')
  def testRunHelmCommand(self):
    helm.RunHelmCommand(['install', 'my-release', 'my-chart'])
    self.mock_issue_command.assert_called_once_with(
        ['/custom/helm', 'install', 'my-release', 'my-chart'],
        stack_level=2,
    )

  @flagsaver.flagsaver(helm='helm', kubeconfig='/path/to/kubeconfig')
  def testRunHelmCommandWithKubeconfig(self):
    helm.RunHelmCommand(['list'])
    self.mock_issue_command.assert_called_once_with(
        ['helm', '--kubeconfig', '/path/to/kubeconfig', 'list'],
        stack_level=2,
    )


class InstallChartsTest(pkb_common_test_case.PkbCommonTestCase):

  def testInstallChartsRunsHelmUpgradeInstallPerChart(self):
    run_helm_command = self.enter_context(
        mock.patch.object(helm, 'RunHelmCommand', return_value=('', '', 0))
    )
    charts = [
        helm.HelmChart(
            release_name='dynamo-crds',
            chart_ref='oci://example.com/dynamo-crds.tgz',
            namespace='default',
            wait=True,
            atomic=True,
        ),
        helm.HelmChart(
            release_name='dynamo-platform',
            chart_ref='oci://example.com/dynamo-platform.tgz',
            namespace='default',
            set_values={'grove.enabled': 'true'},
            wait=False,
            atomic=False,
        ),
    ]

    helm.InstallCharts(charts)

    self.assertEqual(run_helm_command.call_count, 2)
    first_call_args = run_helm_command.call_args_list[0].args[0]
    self.assertEqual(
        first_call_args,
        [
            'upgrade',
            '--install',
            'dynamo-crds',
            'oci://example.com/dynamo-crds.tgz',
            '--namespace',
            'default',
            '--wait',
            '--atomic',
        ],
    )
    second_call_args = run_helm_command.call_args_list[1].args[0]
    self.assertEqual(
        second_call_args,
        [
            'upgrade',
            '--install',
            'dynamo-platform',
            'oci://example.com/dynamo-platform.tgz',
            '--namespace',
            'default',
            '--set',
            'grove.enabled=true',
        ],
    )


if __name__ == '__main__':
  unittest.main()

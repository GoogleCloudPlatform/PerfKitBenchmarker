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
"""Unit tests for kubernetes_commands pod discovery, readiness, and exec."""

import time
import unittest
from unittest import mock

from perfkitbenchmarker import vm_util
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_commands
from tests import pkb_common_test_case


class FindPodByNameSubstringTest(pkb_common_test_case.PkbCommonTestCase):

  def testFindPodByNameSubstringReturnsMatchingPod(self):
    self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            return_value=(
                'dynamo-decode-0 dynamo-frontend-abc123',
                '',
                0,
            ),
        )
    )

    result = kubernetes_commands.FindPodByNameSubstring(
        'dynamo-cloud', 'frontend'
    )

    self.assertEqual(result, 'dynamo-frontend-abc123')

  def testFindPodByNameSubstringRaisesWhenNoMatch(self):
    self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            return_value=('dynamo-decode-0', '', 0),
        )
    )

    with self.assertRaises(RuntimeError):
      kubernetes_commands.FindPodByNameSubstring('dynamo-cloud', 'frontend')


class WaitForPodsReadyTest(pkb_common_test_case.PkbCommonTestCase):

  def testWaitForPodsReadyPollsThenWaitsForCondition(self):
    self.enter_context(mock.patch.object(time, 'sleep'))
    run_kubectl_command = self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            return_value=('frontend-abc123', '', 0),
        )
    )
    wait_for_resource = self.enter_context(
        mock.patch.object(kubernetes_commands, 'WaitForResource')
    )

    kubernetes_commands.WaitForPodsReady('dynamo-cloud', timeout=300)

    run_kubectl_command.assert_called_once_with(
        [
            'get',
            'pods',
            '-o',
            'jsonpath={.items[*].metadata.name}',
            '-n',
            'dynamo-cloud',
        ],
        stack_level=3,
        should_pre_log=False,
        suppress_logging=True,
    )
    wait_for_resource.assert_called_once_with(
        'pod', 'ready', namespace='dynamo-cloud', timeout=300, wait_for_all=True
    )

  def testWaitForPodsReadyRaisesIfNoPodsAppear(self):
    self.enter_context(mock.patch.object(time, 'sleep'))
    times = iter([0, 1, 301])
    self.enter_context(
        mock.patch.object(time, 'time', side_effect=lambda: next(times))
    )
    self.enter_context(
        mock.patch.object(
            kubectl, 'RunKubectlCommand', return_value=('', '', 0)
        )
    )

    with self.assertRaises(vm_util.TimeoutExceededRetryError):
      kubernetes_commands.WaitForPodsReady('dynamo-cloud', timeout=300)


class ExecAndParseResultTest(pkb_common_test_case.PkbCommonTestCase):

  def testExecAndParseResultRunsCommandAndParsesStdout(self):
    self.enter_context(
        mock.patch.object(
            kubernetes_commands,
            'FindPodByNameSubstring',
            return_value='pod/dynamo-frontend-abc123',
        )
    )
    run_kubectl_command = self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            return_value=('{"mean_ttft_ms": 123.4}\n', '', 0),
        )
    )

    result = kubernetes_commands.ExecAndParseResult(
        pod_selector='frontend',
        command=['cat', '/tmp/output.json'],
        parse_fn=lambda stdout: {'mean_ttft_ms': 123.4},
        namespace='dynamo-cloud',
    )

    self.assertEqual(result, {'mean_ttft_ms': 123.4})
    run_kubectl_command.assert_called_once_with([
        'exec',
        'pod/dynamo-frontend-abc123',
        '-n',
        'dynamo-cloud',
        '--',
        'cat',
        '/tmp/output.json',
    ])


if __name__ == '__main__':
  unittest.main()

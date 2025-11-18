# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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

import unittest
from unittest import mock

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks import kubernetes_deployment_startup_benchmark as kdsb
from perfkitbenchmarker.linux_benchmarks import kubernetes_scale_benchmark as ksb
from tests import pkb_common_test_case


class KubernetesDeploymentStartupBenchmarkTest(
    pkb_common_test_case.PkbCommonTestCase
):
  """Tests for the kubernetes_deployment_startup_benchmark."""

  def setUp(self):
    super().setUp()
    self.spec = mock.Mock(spec=benchmark_spec.BenchmarkSpec)
    self.spec.container_cluster = mock.Mock()
    self.spec.container_specs = {
        'kubernetes_deployment_startup': mock.Mock(image='test_image')
    }

  @mock.patch.object(ksb, 'GetStatusConditionsForResourceType')
  def testRun(self, mock_get_conditions):
    """Tests the Run method with mock pod data."""
    mock_get_conditions.return_value = [
        mock.Mock(
            event='PodReadyToStartContainers',
            resource_name='pod1',
            epoch_time=10,
        ),
        mock.Mock(event='Ready', resource_name='pod1', epoch_time=20),
        mock.Mock(
            event='PodReadyToStartContainers',
            resource_name='pod2',
            epoch_time=12,
        ),
        mock.Mock(event='Ready', resource_name='pod2', epoch_time=25),
    ]

    result = kdsb.Run(self.spec)

    self.spec.container_cluster.ApplyManifest.assert_called_with(
        kdsb.DEPLOYMENT_YAML.value, name='startup', image='test_image'
    )
    self.spec.container_cluster.WaitForRollout.assert_called_with(
        'deployment/startup', timeout=600
    )
    self.assertEqual(len(result), 1)
    self.assertEqual(
        result[0],
        sample.Sample(
            'max_pod_ready_time', 13, 'seconds', {}, result[0].timestamp
        ),
    )

  @mock.patch.object(ksb, 'GetStatusConditionsForResourceType')
  def testRunNoPods(self, mock_get_conditions):
    """Tests the Run method when no pods are found."""
    mock_get_conditions.return_value = []
    with self.assertRaises(RuntimeError):
      kdsb.Run(self.spec)


if __name__ == '__main__':
  unittest.main()

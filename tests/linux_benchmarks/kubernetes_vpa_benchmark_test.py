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

import threading
import unittest
from unittest import mock

from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks import kubernetes_vpa_benchmark
from tests import pkb_common_test_case


class KubernetesVpaBenchmarkTest(pkb_common_test_case.PkbCommonTestCase):

  def testGetConfig(self):
    config = kubernetes_vpa_benchmark.GetConfig({})
    self.assertIsInstance(config, dict)

  def testComputeOverUnderProvisioning(self):
    samples = [
        sample.Sample(
            'kubernetes_cpu_request', 1, 'cores', {'pod': 'p1'}, timestamp=0
        ),
        sample.Sample(
            'kubernetes_cpu_request', 1, 'cores', {'pod': 'p1'}, timestamp=10
        ),
        sample.Sample(
            'kubernetes_cpu_usage', 0.5, 'cores', {'pod': 'p1'}, timestamp=0
        ),
        sample.Sample(
            'kubernetes_cpu_usage', 1.5, 'cores', {'pod': 'p1'}, timestamp=10
        ),
    ]
    result = kubernetes_vpa_benchmark._ComputeOverUnderProvisioning(samples)
    self.assertEqual(result.metric, 'total_over_under_provisioning')
    # NB: "true" value of the integral is 2.5 - but we're using the trapazoid
    # method, which means we only look at the data points and assume the
    # in-between parts are averaged out.
    self.assertAlmostEqual(result.value, 5.0)

  def testComputeOverUnderProvisioning_NoSamples(self):
    result = kubernetes_vpa_benchmark._ComputeOverUnderProvisioning([])
    self.assertEqual(result.metric, 'total_over_under_provisioning')
    self.assertEqual(result.value, 0)


class KubernetesMetricsCollectorTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.samples = []
    self.stop_event = threading.Event()
    self.collector = kubernetes_vpa_benchmark.KubernetesMetricsCollector(
        self.samples, self.stop_event
    )

  def testObserve(self):
    observe_fn = mock.Mock()
    observe_fn.return_value = [sample.Sample('metric', 1, 'unit')]

    with mock.patch.object(
        self.stop_event, 'wait', side_effect=[False, False, True]
    ):
      self.collector._Observe(observe_fn)

    self.assertEqual(observe_fn.call_count, 3)
    self.assertEqual(len(self.samples), 3)

  def testObserve_IgnoresOccasionalErrors(self):
    observe_fn = mock.Mock(
        side_effect=[
            errors.VmUtil.IssueCommandError('error'),
        ]
        + [[sample.Sample('metric', 1, 'unit')]] * 10,
    )

    with mock.patch.object(
        self.stop_event, 'wait', side_effect=[False] * 10 + [True]
    ):
      self.collector._Observe(observe_fn)

    self.assertEqual(observe_fn.call_count, 11)
    self.assertEqual(len(self.samples), 10)

  def testObserve_AssertsOnTooManyFailures(self):
    observe_fn = mock.Mock(side_effect=errors.VmUtil.IssueCommandError('error'))

    with mock.patch.object(self.stop_event, 'wait', side_effect=[False, True]):
      with self.assertRaises(AssertionError):
        self.collector._Observe(observe_fn)


if __name__ == '__main__':
  unittest.main()

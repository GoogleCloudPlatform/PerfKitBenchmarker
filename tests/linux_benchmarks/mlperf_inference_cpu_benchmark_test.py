# Copyright 2023 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for mlperf_inference_cpu_benchmark."""
import os
import unittest

from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import mlperf_inference_cpu_benchmark
from perfkitbenchmarker.sample import Sample
from tests import pkb_common_test_case


class MlperfInferenceCpuBenchmarkTestCase(
    pkb_common_test_case.PkbCommonTestCase, test_util.SamplesTestMixin
):

  def setUp(self):
    super(MlperfInferenceCpuBenchmarkTestCase, self).setUp()
    path = os.path.join(
        os.path.dirname(__file__),
        '..',
        'data',
        'mlperf_inference_cpu.txt',
    )
    with open(path) as fp:
      self.performance_contents = fp.read()

  def testTrainResults(self):
    result = mlperf_inference_cpu_benchmark.MakeSamplesFromOutput(
        {}, self.performance_contents
    )
    metadata = {
        'SUT name': 'PySUT',
        'Scenario': 'Server',
        'Mode': 'PerformanceOnly',
        'Samples per second': '0.98',
        'Result is': 'INVALID',
        'Performance constraints satisfied': 'NO',
        'Min duration satisfied': 'Yes',
        'Min queries satisfied': 'Yes',
        'Early stopping satisfied': 'NO',
        'Recommendations': '',
        'Early Stopping Result': '',
        'Completed samples per second': '0.98',
        'Min latency (ns)': '10419853',
        'Max latency (ns)': '31020875',
        'Mean latency (ns)': '17048146',
        '50.00 percentile latency (ns)': '18753138',
        '90.00 percentile latency (ns)': '19580269',
        '95.00 percentile latency (ns)': '19742106',
        '97.00 percentile latency (ns)': '19965075',
        '99.00 percentile latency (ns)': '27129671',
        '99.90 percentile latency (ns)': '31020875',
        'samples_per_query': '1',
        'target_qps': '1.01',
        'target_latency (ns)': '15000000',
        'max_async_queries': '0',
        'min_duration (ms)': '600000',
        'max_duration (ms)': '0',
        'min_query_count': '100',
        'max_query_count': '0',
        'qsl_rng_seed': '10003631887983097364',
        'sample_index_rng_seed': '17183018601990103738',
        'schedule_rng_seed': '12134888396634371638',
        'accuracy_log_rng_seed': '0',
        'accuracy_log_probability': '0',
        'accuracy_log_sampling_target': '0',
        'print_timestamps': '0',
        'performance_issue_unique': '0',
        'performance_issue_same': '0',
        'performance_issue_same_index': '0',
        'performance_sample_count': '1024',
    }
    golden = Sample(
        metric='throughput',
        value=0.98,
        unit='samples per second',
        metadata=metadata,
    )
    self.assertSamplesEqualUpToTimestamp(golden, result)

  def testValid(self):
    is_valid = mlperf_inference_cpu_benchmark._IsValid(
        self.performance_contents
    )
    self.assertFalse(is_valid)


if __name__ == '__main__':
  unittest.main()

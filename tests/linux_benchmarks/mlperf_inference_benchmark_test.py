# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for mlperf_inference_benchmark."""
import os
import unittest

from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import mlperf_inference_benchmark
from perfkitbenchmarker.sample import Sample
from tests import pkb_common_test_case


class MlperfInferenceBenchmarkTestCase(pkb_common_test_case.PkbCommonTestCase,
                                       test_util.SamplesTestMixin):

  def setUp(self):
    super(MlperfInferenceBenchmarkTestCase, self).setUp()
    path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "data",
        "mlperf_inference_metadata.json",
    )
    with open(path) as fp:
      self.performance_contents = fp.read()

  def testTrainResults(self):
    samples = mlperf_inference_benchmark.MakePerformanceSamplesFromOutput(
        {}, self.performance_contents
    )
    metadata = {
        "benchmark_full": "bert-99",
        "benchmark_short": "bert",
        "config_name": "CloudL4x1_TRT-custom_k_99_MaxP-Offline",
        "detected_system": (
            'SystemConfiguration(host_cpu_conf=CPUConfiguration(layout={CPU(name="Intel(R)'
            ' Xeon(R) CPU @ 2.20GHz", architecture=CPUArchitecture.x86_64,'
            " core_count=8, threads_per_core=2): 1}),"
            " host_mem_conf=MemoryConfiguration(host_memory_capacity=Memory(quantity=65.858964,"
            " byte_suffix=ByteSuffix.GB), comparison_tolerance=0.05),"
            ' accelerator_conf=AcceleratorConfiguration(layout={GPU(name="NVIDIA'
            ' L4", accelerator_type=AcceleratorType.Discrete,'
            " vram=Memory(quantity=22.494140625, byte_suffix=ByteSuffix.GiB),"
            ' max_power_limit=75.0, pci_id="0x27B810DE", compute_sm=89): 1}),'
            ' numa_conf=None, system_id="CloudL4x1")'
        ),
        "early_stopping_met": True,
        "result_scheduled_samples_per_sec": 858.759,
        "result_validity": "VALID",
        "satisfies_query_constraint": False,
        "scenario": "Offline",
        "scenario_key": "result_samples_per_second",
        "summary_string": "result_samples_per_second: 858.759, Result is VALID",
        "system_name": "CloudL4x1_TRT",
        "tensorrt_version": "8.6.0",
        "test_mode": "PerformanceOnly",
    }
    golden = Sample(
        metric="throughput",
        value=858.759,
        unit="samples per second",
        metadata=metadata,
    )
    sample = samples[0]
    self.assertSamplesEqualUpToTimestamp(golden, sample)


if __name__ == "__main__":
  unittest.main()

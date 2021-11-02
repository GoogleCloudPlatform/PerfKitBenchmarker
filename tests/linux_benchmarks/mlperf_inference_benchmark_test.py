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
        os.path.dirname(__file__), '..', 'data', 'mlperf_inference_output.txt')
    with open(path) as fp:
      self.contents = fp.read()

  def testTrainResults(self):
    samples = mlperf_inference_benchmark.MakeSamplesFromOutput(
        {'version': 'v1.1'}, self.contents)
    metadata = {
        'mlperf 50.00 percentile latency (ns)': '40533329',
        'mlperf 90.00 percentile latency (ns)': '51387550',
        'mlperf 95.00 percentile latency (ns)': '54956149',
        'mlperf 97.00 percentile latency (ns)': '57792422',
        'mlperf 99.00 percentile latency (ns)': '82056764',
        'mlperf 99.90 percentile latency (ns)': '543294654940',
        'mlperf Completed samples per second': '3102.49',
        'mlperf Max latency (ns)': '605456500256',
        'mlperf Mean latency (ns)': '3037717062',
        'mlperf Min duration satisfied': 'Yes',
        'mlperf Min latency (ns)': '4126840',
        'mlperf Min queries satisfied': 'Yes',
        'mlperf Mode': 'PerformanceOnly',
        'mlperf Performance constraints satisfied': 'Yes',
        'mlperf Result is': 'VALID',
        'mlperf SUT name': 'BERT SERVER',
        'mlperf Scenario': 'Server',
        'mlperf Scheduled samples per second': '3102.76',
        'mlperf accuracy_level': '99%',
        'mlperf accuracy_log_probability': '0',
        'mlperf accuracy_log_rng_seed': '0',
        'mlperf accuracy_log_sampling_target': '0',
        'mlperf active_sms': '60',
        'mlperf benchmark': 'Benchmark.BERT',
        'mlperf bert_opt_seqlen': '384',
        'mlperf coalesced_tensor': 'True',
        'mlperf config_name': 'A100-SXM4-40GBx1_bert_Server',
        'mlperf config_ver': 'custom_k_99_MaxP',
        'mlperf cpu_freq': 'None',
        'mlperf enable_interleaved': 'False',
        'mlperf gpu_batch_size': '64',
        'mlperf gpu_copy_streams': '1',
        'mlperf gpu_inference_streams': '2',
        'mlperf gpu_num_bundles': '2',
        'mlperf graphs_max_seqlen': '200',
        'mlperf inference_server': 'custom',
        'mlperf input_dtype': 'int32',
        'mlperf input_format': 'linear',
        'mlperf log_dir': '/work/build/logs/2021.10.27-20.51.11',
        'mlperf max_async_queries': '0',
        'mlperf max_duration (ms)': '0',
        'mlperf max_query_count': '0',
        'mlperf min_duration (ms)': '600000',
        'mlperf min_query_count': '270336',
        'mlperf optimization_level': 'plugin-enabled',
        'mlperf performance_issue_same': '0',
        'mlperf performance_issue_same_index': '0',
        'mlperf performance_issue_unique': '0',
        'mlperf performance_sample_count': '10833',
        'mlperf power_limit': 'None',
        'mlperf precision': 'int8',
        'mlperf print_timestamps': '0',
        'mlperf qsl_rng_seed': '1624344308455410291',
        'mlperf sample_index_rng_seed': '517984244576520566',
        'mlperf samples_per_query': '1',
        'mlperf scenario': 'Scenario.Server',
        'mlperf schedule_rng_seed': '10051496985653635065',
        'mlperf server_num_issue_query_threads': '1',
        'mlperf server_target_qps': '3100',
        'mlperf soft_drop': '0.99',
        'mlperf system': 'A100-SXM4-40GBx1',
        'mlperf system_id': 'A100-SXM4-40GBx1',
        'mlperf target_latency (ns)': '130000000',
        'mlperf tensor_path':
            '${PREPROCESSED_DATA_DIR}/squad_tokenized/input_ids.npy,'
            '${PREPROCESSED_DATA_DIR}/squad_tokenized/segment_ids.npy,'
            '${PREPROCESSED_DATA_DIR}/squad_tokenized/input_mask.npy',
        'mlperf use_cpu': 'False',
        'mlperf use_graphs': 'True',
        'version': 'v1.1'
    }
    golden = Sample(
        metric='throughput', value=3102.76, unit='samples/s', metadata=metadata)
    self.assertSamplesEqualUpToTimestamp(golden, samples[0])


if __name__ == '__main__':
  unittest.main()

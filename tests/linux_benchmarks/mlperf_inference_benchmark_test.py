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
        os.path.dirname(__file__), '..', 'data',
        'bert_inference_performance_output.txt')
    with open(path) as fp:
      self.bert_performance_contents = fp.read()

    path = os.path.join(
        os.path.dirname(__file__), '..', 'data',
        'bert_inference_accuracy_output.txt')
    with open(path) as fp:
      self.bert_accuracy_contents = fp.read()

    path = os.path.join(
        os.path.dirname(__file__), '..', 'data',
        'dlrm_inference_performance_output.txt')
    with open(path) as fp:
      self.dlrm_performance_contents = fp.read()

    path = os.path.join(
        os.path.dirname(__file__), '..', 'data',
        'dlrm_inference_accuracy_output.txt')
    with open(path) as fp:
      self.dlrm_accuracy_contents = fp.read()

  def testTrainResults(self):
    samples = mlperf_inference_benchmark.MakePerformanceSamplesFromOutput(
        {'version': 'v1.1'}, self.bert_performance_contents)
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
        'mlperf benchmark': 'Benchmark.BERT',
        'mlperf coalesced_tensor': 'True',
        'mlperf config_name': 'A100-SXM4-40GBx1_bert_Server',
        'mlperf config_ver': 'custom_k_99_MaxP',
        'mlperf cpu_freq': 'None',
        'mlperf gpu_batch_size': '64',
        'mlperf gpu_copy_streams': '1',
        'mlperf gpu_inference_streams': '2',
        'mlperf gpu_num_bundles': '2',
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
        'mlperf server_target_qps': '3100',
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

    samples = mlperf_inference_benchmark.MakeAccuracySamplesFromOutput(
        {'version': 'v1.1'}, self.bert_accuracy_contents)
    metadata = {
        'mlperf benchmark': 'Benchmark.BERT',
        'mlperf coalesced_tensor': 'True',
        'mlperf gpu_batch_size': '64',
        'mlperf gpu_copy_streams': '1',
        'mlperf gpu_inference_streams': '2',
        'mlperf input_dtype': 'int32',
        'mlperf input_format': 'linear',
        'mlperf precision': 'int8',
        'mlperf scenario': 'Scenario.Server',
        'mlperf server_target_qps': '3100',
        'mlperf system': 'A100-SXM4-40GBx1',
        'mlperf tensor_path':
            '${PREPROCESSED_DATA_DIR}/squad_tokenized/input_ids.npy,'
            '${PREPROCESSED_DATA_DIR}/squad_tokenized/segment_ids.npy,'
            '${PREPROCESSED_DATA_DIR}/squad_tokenized/input_mask.npy',
        'mlperf use_graphs': 'True',
        'mlperf config_name': 'A100-SXM4-40GBx1_bert_Server',
        'mlperf config_ver': 'custom_k_99_MaxP',
        'mlperf accuracy_level': '99%',
        'mlperf optimization_level': 'plugin-enabled',
        'mlperf inference_server': 'custom',
        'mlperf system_id': 'A100-SXM4-40GBx1',
        'mlperf use_cpu': 'False',
        'mlperf power_limit': 'None',
        'mlperf cpu_freq': 'None',
        'mlperf test_mode': 'AccuracyOnly',
        'mlperf fast': 'True',
        'mlperf gpu_num_bundles': '2',
        'mlperf log_dir': '/work/build/logs/2021.11.09-05.18.28',
        'Threshold': 89.965,
        'version': 'v1.1'
    }
    golden = Sample(
        metric='accuracy', value=90.376, unit='%', metadata=metadata)
    self.assertSamplesEqualUpToTimestamp(golden, samples[0])

    samples = mlperf_inference_benchmark.MakePerformanceSamplesFromOutput(
        {'version': 'v1.1'}, self.dlrm_performance_contents)
    metadata = {
        'mlperf benchmark': 'Benchmark.DLRM',
        'mlperf coalesced_tensor': 'True',
        'mlperf gpu_batch_size': '262100',
        'mlperf gpu_copy_streams': '1',
        'mlperf gpu_inference_streams': '1',
        'mlperf input_dtype': 'int8',
        'mlperf input_format': 'chw4',
        'mlperf precision': 'int8',
        'mlperf scenario': 'Scenario.Server',
        'mlperf server_target_qps': '2100000',
        'mlperf system': 'A100-SXM4-40GBx8',
        'mlperf tensor_path': '${PREPROCESSED_DATA_DIR}/criteo/full_recalib/'
                              'numeric_int8_chw4.npy,'
                              '${PREPROCESSED_DATA_DIR}/criteo/full_recalib/'
                              'categorical_int32.npy',
        'mlperf use_graphs': 'False',
        'mlperf config_name': 'A100-SXM4-40GBx8_dlrm_Server',
        'mlperf config_ver': 'custom_k_99_MaxP',
        'mlperf accuracy_level': '99%',
        'mlperf optimization_level': 'plugin-enabled',
        'mlperf inference_server': 'custom',
        'mlperf system_id': 'A100-SXM4-40GBx8',
        'mlperf use_cpu': 'False',
        'mlperf power_limit': 'None',
        'mlperf cpu_freq': 'None',
        'mlperf gpu_num_bundles': '2',
        'mlperf log_dir': '/work/build/logs/2021.11.13-04.12.53',
        'mlperf SUT name': 'DLRM SERVER',
        'mlperf Scenario': 'Server',
        'mlperf Mode': 'PerformanceOnly',
        'mlperf Scheduled samples per second': '2102380.29',
        'mlperf Result is': 'VALID',
        'mlperf Performance constraints satisfied': 'Yes',
        'mlperf Min duration satisfied': 'Yes',
        'mlperf Min queries satisfied': 'Yes',
        'mlperf Completed samples per second': '2102359.14',
        'mlperf Min latency (ns)': '159697',
        'mlperf Max latency (ns)': '12452412',
        'mlperf Mean latency (ns)': '1375416',
        'mlperf 50.00 percentile latency (ns)': '1285505',
        'mlperf 90.00 percentile latency (ns)': '1984044',
        'mlperf 95.00 percentile latency (ns)': '2319343',
        'mlperf 97.00 percentile latency (ns)': '2568660',
        'mlperf 99.00 percentile latency (ns)': '3507998',
        'mlperf 99.90 percentile latency (ns)': '5628323',
        'mlperf samples_per_query': '1',
        'mlperf target_latency (ns)': '30000000',
        'mlperf max_async_queries': '0',
        'mlperf min_duration (ms)': '60000',
        'mlperf max_duration (ms)': '0',
        'mlperf min_query_count': '1',
        'mlperf max_query_count': '0',
        'mlperf qsl_rng_seed': '1624344308455410291',
        'mlperf sample_index_rng_seed': '517984244576520566',
        'mlperf schedule_rng_seed': '10051496985653635065',
        'mlperf accuracy_log_rng_seed': '0',
        'mlperf accuracy_log_probability': '0',
        'mlperf accuracy_log_sampling_target': '0',
        'mlperf print_timestamps': '0',
        'mlperf performance_issue_unique': '0',
        'mlperf performance_issue_same': '0',
        'mlperf performance_issue_same_index': '0',
        'mlperf performance_sample_count': '204800',
        'version': 'v1.1'
    }
    golden = Sample(
        metric='throughput',
        value=2102380.0,
        unit='samples/s',
        metadata=metadata)
    self.assertSamplesEqualUpToTimestamp(golden, samples[0])

    samples = mlperf_inference_benchmark.MakeAccuracySamplesFromOutput(
        {'version': 'v1.1'}, self.dlrm_accuracy_contents)
    metadata = {
        'Threshold': 79.448,
        'mlperf accuracy_level': '99%',
        'mlperf benchmark': 'Benchmark.DLRM',
        'mlperf coalesced_tensor': 'True',
        'mlperf config_name': 'A100-SXM4-40GBx8_dlrm_Server',
        'mlperf config_ver': 'custom_k_99_MaxP',
        'mlperf cpu_freq': 'None',
        'mlperf fast': 'True',
        'mlperf gpu_batch_size': '262100',
        'mlperf gpu_copy_streams': '1',
        'mlperf gpu_inference_streams': '1',
        'mlperf gpu_num_bundles': '2',
        'mlperf inference_server': 'custom',
        'mlperf input_dtype': 'int8',
        'mlperf input_format': 'chw4',
        'mlperf log_dir': '/work/build/logs/2021.11.13-06.24.26',
        'mlperf optimization_level': 'plugin-enabled',
        'mlperf power_limit': 'None',
        'mlperf precision': 'int8',
        'mlperf scenario': 'Scenario.Server',
        'mlperf server_target_qps': '2100000',
        'mlperf system': 'A100-SXM4-40GBx8',
        'mlperf system_id': 'A100-SXM4-40GBx8',
        'mlperf tensor_path': '${PREPROCESSED_DATA_DIR}/criteo/full_recalib/'
                              'numeric_int8_chw4.npy,'
                              '${PREPROCESSED_DATA_DIR}/criteo/full_recalib/'
                              'categorical_int32.npy',
        'mlperf test_mode': 'AccuracyOnly',
        'mlperf use_cpu': 'False',
        'mlperf use_graphs': 'False',
        'version': 'v1.1'
    }
    golden = Sample(
        metric='accuracy', value=80.185, unit='%', metadata=metadata)
    self.assertSamplesEqualUpToTimestamp(golden, samples[0])
    print(samples[0])


if __name__ == '__main__':
  unittest.main()

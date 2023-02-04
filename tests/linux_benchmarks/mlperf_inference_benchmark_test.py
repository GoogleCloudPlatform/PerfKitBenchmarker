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
        'mlperf_inference_log_detail.txt')
    with open(path) as fp:
      self.performance_contents = fp.read()

    path = os.path.join(
        os.path.dirname(__file__), '..', 'data',
        'bert_inference_accuracy_output.txt')
    with open(path) as fp:
      self.bert_accuracy_contents = fp.read()

    path = os.path.join(
        os.path.dirname(__file__), '..', 'data',
        'dlrm_inference_accuracy_output.txt')
    with open(path) as fp:
      self.dlrm_accuracy_contents = fp.read()

  def testTrainResults(self):
    samples = mlperf_inference_benchmark.MakePerformanceSamplesFromOutput(
        {'version': 'v1.1'}, self.performance_contents)
    metadata = {
        'loadgen_version': '1.1 @ ed7044310a',
        'loadgen_build_date_local': '2022-04-12T14:16:07.377200',
        'loadgen_build_date_utc': '2022-04-12T14:16:07.377209',
        'loadgen_git_commit_date': '2021-08-11T17:36:26+01:00',
        'loadgen_git_status_message': '',
        'test_datetime': '2022-04-12T14:37:18Z',
        'sut_name': 'BERT SERVER',
        'qsl_name': 'BERT QSL',
        'qsl_reported_total_count': 10833,
        'qsl_reported_performance_count': 10833,
        'requested_scenario': 'Server',
        'requested_test_mode': 'PerformanceOnly',
        'requested_server_target_qps': 360,
        'requested_server_target_latency_ns': 130000000,
        'requested_server_target_latency_percentile': 0.99,
        'requested_server_coalesce_queries': True,
        'requested_server_find_peak_qps_decimals_of_precision': 1,
        'requested_server_find_peak_qps_boundary_step_size': 1,
        'requested_server_max_async_queries': 0,
        'requested_server_num_issue_query_threads': 0,
        'requested_min_duration_ms': 600000,
        'requested_max_duration_ms': 0,
        'requested_min_query_count': 270336,
        'requested_max_query_count': 0,
        'requested_qsl_rng_seed': 1624344308455410291,
        'requested_sample_index_rng_seed': 517984244576520566,
        'requested_schedule_rng_seed': 10051496985653635065,
        'requested_accuracy_log_rng_seed': 0,
        'requested_accuracy_log_probability': 0,
        'requested_accuracy_log_sampling_target': 0,
        'requested_print_timestamps': False,
        'requested_performance_issue_unique': False,
        'requested_performance_issue_same': False,
        'requested_performance_issue_same_index': 0,
        'requested_performance_sample_count_override': 10833,
        'effective_scenario': 'Server',
        'effective_test_mode': 'PerformanceOnly',
        'effective_samples_per_query': 1,
        'effective_target_qps': 360,
        'effective_target_latency_ns': 130000000,
        'effective_target_latency_percentile': 0.99,
        'effective_max_async_queries': 0,
        'effective_target_duration_ms': 600000,
        'effective_min_duration_ms': 600000,
        'effective_max_duration_ms': 0,
        'effective_min_query_count': 270336,
        'effective_max_query_count': 0,
        'effective_min_sample_count': 270336,
        'effective_qsl_rng_seed': 1624344308455410291,
        'effective_sample_index_rng_seed': 517984244576520566,
        'effective_schedule_rng_seed': 10051496985653635065,
        'effective_accuracy_log_rng_seed': 0,
        'effective_accuracy_log_probability': 0,
        'effective_accuracy_log_sampling_target': 0,
        'effective_print_timestamps': False,
        'effective_performance_issue_unique': False,
        'effective_performance_issue_same': False,
        'effective_performance_issue_same_index': 0,
        'effective_performance_sample_count': 10833,
        'generic_message': 'Starting performance mode',
        'generated_query_count': 270336,
        'generated_samples_per_query': 1,
        'generated_query_duration': 751081766900,
        'logger_swap_request_slots_retry_count': 0,
        'logger_swap_request_slots_retry_retry_count': 0,
        'logger_swap_request_slots_retry_reencounter_count': 0,
        'logger_start_reading_entries_retry_count': 0,
        'logger_tls_total_log_cas_fail_count': 0,
        'logger_tls_total_swap_buffers_slot_retry_count': 0,
        'power_begin': '04-12-2022 14:37:18.983',
        'power_end': '04-12-2022 14:51:28.239',
        'result_validity': 'INVALID',
        'result_perf_constraints_met': False,
        'result_min_duration_met': True,
        'result_min_queries_met': True,
        'result_invalid_reason': 'Reduce target QPS to improve latency. ',
        'result_scheduled_samples_per_sec': 359.93,
        'result_completed_samples_per_sec': 321.067,
        'result_min_latency_ns': 6669619,
        'result_max_latency_ns': 846285311653,
        'result_mean_latency_ns': 48257222252,
        'result_50.00_percentile_latency_ns': 45675229779,
        'result_90.00_percentile_latency_ns': 82432475255,
        'result_95.00_percentile_latency_ns': 87117424239,
        'result_97.00_percentile_latency_ns': 89161842381,
        'result_99.00_percentile_latency_ns': 90813602755,
        'result_99.90_percentile_latency_ns': 735809900637,
        'version': 'v1.1'
    }
    golden = Sample(
        metric='throughput',
        value=321.067,
        unit='samples per second',
        metadata=metadata)
    sample = samples[0]
    sample.metadata.pop('loaded_qsl_set')
    sample.metadata.pop('loadgen_git_log_message')
    sample.metadata.pop('loadgen_file_sha1')
    self.assertSamplesEqualUpToTimestamp(golden, sample)

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
        'mlperf log_dir': '/work/build/logs/2021.11.09-05.18.28',
        'Threshold': 89.965,
        'version': 'v1.1'
    }
    golden = Sample(
        metric='accuracy', value=90.376, unit='%', metadata=metadata)
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
        'mlperf gpu_batch_size': '262100',
        'mlperf gpu_copy_streams': '1',
        'mlperf gpu_inference_streams': '1',
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


if __name__ == '__main__':
  unittest.main()

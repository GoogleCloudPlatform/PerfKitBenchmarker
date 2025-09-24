import copy
import datetime
import json
import os
import unittest
from unittest import mock
import zoneinfo

from perfkitbenchmarker import container_service
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks import kubernetes_ai_inference_benchmark
from perfkitbenchmarker.resources.kubernetes import wg_serving_inference_server
from perfkitbenchmarker.resources.kubernetes import wg_serving_inference_server_spec as k8s_spec
from tests import pkb_common_test_case

ZoneInfo = zoneinfo.ZoneInfo
tz_string = os.environ.get('TZ', 'America/Los_Angeles')
tz = ZoneInfo(tz_string)
_SAMPLE_RESULT_DATA_BASE = {
    'metrics': {'metric1': 10.0, 'request_rate': 1.0},
    'dimensions': {
        'model_id': 'model_a',
        'tokenizer_id': 'tokenizer_a',
        'backend': 'vllm',
    },
    'config': {'start_time': {'seconds': 1000000000}},
}

_SAMPLE_RESULT_DATA_ANOTHER = {
    'metrics': {'metric2': 20.0, 'request_rate': 2.0},
    'dimensions': {
        'model_id': 'model_b',
        'tokenizer_id': 'tokenizer_b',
        'backend': 'tgi',
    },
    'config': {'start_time': {'seconds': 2000000000}},
}


class KubernetesAiInferenceBenchmarkTest(
    pkb_common_test_case.PkbCommonTestCase
):

  def setUp(self):
    super().setUp()
    self.mock_listdir = self.enter_context(
        mock.patch.object(
            kubernetes_ai_inference_benchmark.os, 'listdir', autospec=True
        )
    )
    self.mock_open = self.enter_context(
        mock.patch.object(
            kubernetes_ai_inference_benchmark,
            'open',
            new_callable=mock.mock_open,
        )
    )
    self.mock_log_exception = self.enter_context(
        mock.patch.object(
            kubernetes_ai_inference_benchmark.logging,
            'exception',
            autospec=True,
        )
    )

  def _create_mock_k8s_server(self, hpa_enabled=False):
    mock_k8s_spec = mock.create_autospec(
        k8s_spec.WGServingInferenceServerConfigSpec, instance=True
    )
    mock_k8s_spec.model_server = 'mock_model_server'
    mock_k8s_spec.hpa_enabled = hpa_enabled
    mock_k8s_spec.name = 'mock_server_name'
    mock_k8s_spec.app_selector = 'vllm_inference_server'
    mock_k8s_spec.catalog_components = ''
    mock_k8s_server = mock.create_autospec(
        wg_serving_inference_server.WGServingInferenceServer,
        instance=True,
    )
    mock_k8s_server.spec = mock_k8s_spec
    mock_k8s_server.model_id = 'mock_model'
    mock_k8s_server.tokenizer_id = 'mock_tokenizer'
    mock_k8s_server.pod_names = []
    mock_k8s_server.cluster = mock.create_autospec(
        container_service.KubernetesCluster, instance=True
    )
    fixed_datetime = datetime.datetime(2025, 7, 18, 10, 0, 0)
    fixed_timestamp = fixed_datetime.timestamp()
    mock_k8s_server.model_load_timestamp = fixed_timestamp
    mock_k8s_server.GetResourceMetadata.return_value = {
        'hpa_enabled': hpa_enabled
    }
    mock_k8s_server.timezone = tz_string
    return mock_k8s_server

  def test_prepare_result_success(self):
    self.mock_listdir.return_value = ['result1.json', 'result2.json']
    data1_str = json.dumps(_SAMPLE_RESULT_DATA_BASE)
    data2_str = json.dumps(_SAMPLE_RESULT_DATA_ANOTHER)
    self.mock_open.side_effect = [
        mock.mock_open(read_data=data1_str).return_value,
        mock.mock_open(read_data=data2_str).return_value,
    ]
    mock_k8s_server = self._create_mock_k8s_server(hpa_enabled=False)
    mock_k8s_server.GetResourceMetadata.return_value = {
        'model': 'mock_model',
        'tokenizer_id': 'mock_tokenizer',
        'inference_server': 'mock_model_server',
        'hpa_enabled': False,
    }
    expected_samples = [
        sample.Sample(
            metric='metric1',
            value=10.0,
            unit='',
            metadata={
                'request_rate': 1.0,
                'model': 'mock_model',
                'tokenizer_id': 'mock_tokenizer',
                'inference_server': 'mock_model_server',
                'hpa_enabled': False,
            },
            timestamp=1000000000.0,
        ),
        sample.Sample(
            metric='request_rate',
            value=1.0,
            unit='',
            metadata={
                'request_rate': 1.0,
                'model': 'mock_model',
                'tokenizer_id': 'mock_tokenizer',
                'inference_server': 'mock_model_server',
                'hpa_enabled': False,
            },
            timestamp=1000000000.0,
        ),
        sample.Sample(
            metric='metric2',
            value=20.0,
            unit='',
            metadata={
                'request_rate': 2.0,
                'model': 'mock_model',
                'tokenizer_id': 'mock_tokenizer',
                'inference_server': 'mock_model_server',
                'hpa_enabled': False,
            },
            timestamp=2000000000.0,
        ),
        sample.Sample(
            metric='request_rate',
            value=2.0,
            unit='',
            metadata={
                'request_rate': 2.0,
                'model': 'mock_model',
                'tokenizer_id': 'mock_tokenizer',
                'inference_server': 'mock_model_server',
                'hpa_enabled': False,
            },
            timestamp=2000000000.0,
        ),
    ]

    results = kubernetes_ai_inference_benchmark.CollectBenchmarkResult(
        mock_k8s_server.GetResourceMetadata(), 'dummy_path'
    )
    self.assertCountEqual(results, expected_samples)

    expected_calls = [
        mock.call(
            os.path.join('dummy_path', 'result1.json'), 'r', encoding='utf-8'
        ),
        mock.call(
            os.path.join('dummy_path', 'result2.json'), 'r', encoding='utf-8'
        ),
    ]
    self.mock_open.assert_has_calls(expected_calls, any_order=True)

  def test_prepare_result_hpa_enabled(self):
    self.mock_listdir.return_value = ['result1.json']
    data1_str = json.dumps(_SAMPLE_RESULT_DATA_BASE)
    self.mock_open.return_value = mock.mock_open(
        read_data=data1_str
    ).return_value
    mock_k8s_server = self._create_mock_k8s_server(hpa_enabled=True)
    results = kubernetes_ai_inference_benchmark.CollectBenchmarkResult(
        mock_k8s_server.GetResourceMetadata(), 'dummy_path'
    )
    self.assertTrue(results[0].metadata['hpa_enabled'])
    self.assertTrue(results[1].metadata['hpa_enabled'])

  def test_prepare_result_empty_directory(self):
    self.mock_listdir.return_value = []
    mock_k8s_server = self._create_mock_k8s_server()
    results = kubernetes_ai_inference_benchmark.CollectBenchmarkResult(
        mock_k8s_server.GetResourceMetadata(), 'dummy_path'
    )
    self.assertEmpty(results)

  def test_prepare_result_no_json_files(self):
    self.mock_listdir.return_value = ['result.txt', 'another_file.log']
    mock_k8s_server = self._create_mock_k8s_server()
    results = kubernetes_ai_inference_benchmark.CollectBenchmarkResult(
        mock_k8s_server.GetResourceMetadata(), 'dummy_path'
    )
    self.assertEmpty(results)
    self.mock_open.assert_not_called()

  def test_prepare_result_malformed_json(self):
    self.mock_listdir.return_value = ['malformed.json']
    self.mock_open.return_value.read.return_value = 'not valid json {'
    mock_k8s_server = self._create_mock_k8s_server()
    with self.assertRaises(json.JSONDecodeError):
      kubernetes_ai_inference_benchmark.CollectBenchmarkResult(
          mock_k8s_server.GetResourceMetadata(), 'dummy_path'
      )

  def test_prepare_result_missing_keys(self):
    self.mock_listdir.return_value = ['missing_keys.json']
    data_missing_metrics = copy.deepcopy(_SAMPLE_RESULT_DATA_BASE)
    del data_missing_metrics['metrics']
    self.mock_open.return_value.read.return_value = json.dumps(
        data_missing_metrics
    )
    mock_k8s_server = self._create_mock_k8s_server()
    with self.assertRaises(KeyError):
      kubernetes_ai_inference_benchmark.CollectBenchmarkResult(
          mock_k8s_server, 'dummy_path'
      )

  def test_prepare_result_skips_weighted_json(self):
    self.mock_listdir.return_value = ['result1.json', 'result1.weighted.json']
    data1_str = json.dumps(_SAMPLE_RESULT_DATA_BASE)
    self.mock_open.return_value.read.return_value = data1_str
    mock_k8s_server = self._create_mock_k8s_server()
    results = kubernetes_ai_inference_benchmark.CollectBenchmarkResult(
        mock_k8s_server.GetResourceMetadata(), 'dummy_path'
    )
    self.assertLen(results, 2)
    self.assertEqual(results[0].metric, 'metric1')
    self.mock_open.assert_called_once_with(
        os.path.join('dummy_path', 'result1.json'), 'r', encoding='utf-8'
    )

  def test_prepare_result_timestamp_conversion(self):
    self.mock_listdir.return_value = ['time_test.json']

    data_with_specific_time = copy.deepcopy(_SAMPLE_RESULT_DATA_BASE)
    timestamp_seconds = 1000000000
    data_with_specific_time['config']['start_time'][
        'seconds'
    ] = timestamp_seconds

    self.mock_open.return_value.read.return_value = json.dumps(
        data_with_specific_time
    )
    mock_k8s_server = self._create_mock_k8s_server()
    results = kubernetes_ai_inference_benchmark.CollectBenchmarkResult(
        mock_k8s_server.GetResourceMetadata(), 'dummy_path'
    )
    self.assertLen(results, 2)

    expected_timestamp = datetime.datetime.fromtimestamp(
        timestamp_seconds, tz=datetime.timezone.utc
    ).timestamp()

    self.assertEqual(results[0].timestamp, expected_timestamp)
    self.assertEqual(results[1].timestamp, expected_timestamp)

  def test_format_utc_time_stamp_to_local_time(self):
    mock_k8s_server = self._create_mock_k8s_server()
    # 2025-07-18 22:17:01 UTC, result will be local time (Los Angeles)
    # in tz specified.
    utc_timestamp = datetime.datetime(
        2025, 7, 18, 22, 17, 1, tzinfo=datetime.timezone.utc
    ).timestamp()
    local_timestamp = (
        kubernetes_ai_inference_benchmark._FormatUTCTimeStampToLocalTime(
            mock_k8s_server, utc_timestamp
        )
    )
    utc_dt = datetime.datetime.fromtimestamp(
        utc_timestamp, tz=datetime.timezone.utc
    )
    tz_local = zoneinfo.ZoneInfo(mock_k8s_server.timezone)
    local_dt = utc_dt.astimezone(tz_local)
    dt_str_format = local_dt.strftime('%H:%M:%S')
    self.assertEqual(local_timestamp, dt_str_format)

  def test_is_using_tpu(self):
    # TPU component format: v<version>e-<rows>x<cols>
    # Examples of TPU components:
    # v6e-2x4
    # v5e-2x4

    self.assertTrue(
        kubernetes_ai_inference_benchmark._IsUsingTPU(
            'v6e-2x4,gcsfuse,spot'
        )
    )
    self.assertFalse(
        kubernetes_ai_inference_benchmark._IsUsingTPU(
            '8-H100-80GB,gcsfuse,spot'
        )
    )

  def test_tpu_get_model_load_time(self):
    logs = """22:17:30 Found weights from local: /data/models/vllm_models/llama3-8b-hf
    22:17:32 Precompile select_hidden_states --> num_tokens=256 | num_reqs=32
    22:17:52 Compilation finished in 0.17 [secs].
    22:19:00 Starting vLLM API server 0 on http://0.0.0.0:8000
    """
    startup_event = container_service.KubernetesEvent(
        resource=container_service.KubernetesEventResource(
            kind='Pod',
            name='pod1',
        ),
        reason='Started',
        message='Started container inference-server',
        timestamp=datetime.datetime(
            2025, 7, 18, 22, 17, 1, tzinfo=tz
        ).timestamp(),
    )
    mock_k8s_server = self._create_mock_k8s_server(hpa_enabled=False)
    mock_k8s_server.app_selector = 'vllm_inference_server'
    mock_k8s_server.spec.catalog_components = 'v6e-2x4,gcsfuse,spot'
    mock_k8s_server.cluster.GetResourceMetadataByName.return_value = [
        'pod1',
    ]
    mock_k8s_server.cluster.GetEvents.return_value = [startup_event]
    mock_k8s_server.GetStartupLogsFromPod.side_effect = [
        logs,
    ]
    results = kubernetes_ai_inference_benchmark.GetVLLMModelLoadTime(
        mock_k8s_server
    )
    fake_timestamp = mock_k8s_server.model_load_timestamp
    metadata = {
        'hpa_enabled': False,
        'num_pods': 1,
    }
    expected_results = [
        sample.Sample(
            'ai_inference_container_init_time',
            29.0,
            'seconds',
            metadata,
            fake_timestamp,
        ),
        sample.Sample(
            'ai_inference_storage_model_load_time',
            22.0,
            'seconds',
            metadata,
            fake_timestamp,
        ),
        sample.Sample(
            'ai_inference_post_model_load_to_start_time',
            68.0,
            'seconds',
            metadata,
            fake_timestamp,
        ),
    ]
    self.assertEqual(results, expected_results)

  def test_get_model_load_time_single_pod(self):
    first_call_logs = """22:17:30 Starting to load model /data/models/llama3-8b-hf...
    22:17:52 Model loading took 14.9596 GiB and 21.619846 seconds
    22:19:00 Starting vLLM API server on http://0.0.0.0:8000
    """
    startup_event = container_service.KubernetesEvent(
        resource=container_service.KubernetesEventResource(
            kind='Pod',
            name='pod1',
        ),
        reason='Started',
        message='Started container inference-server',
        timestamp=datetime.datetime(
            2025, 7, 18, 22, 17, 1, tzinfo=tz
        ).timestamp(),
    )
    mock_k8s_server = self._create_mock_k8s_server(hpa_enabled=False)
    mock_k8s_server.app_selector = 'vllm_inference_server'
    mock_k8s_server.cluster.GetResourceMetadataByName.return_value = [
        'pod1',
    ]
    mock_k8s_server.cluster.GetEvents.return_value = [startup_event]
    mock_k8s_server.GetStartupLogsFromPod.side_effect = [
        first_call_logs,
    ]
    results = kubernetes_ai_inference_benchmark.GetVLLMModelLoadTime(
        mock_k8s_server
    )
    fake_timestamp = mock_k8s_server.model_load_timestamp
    metadata = {
        'hpa_enabled': False,
        'num_pods': 1,
    }
    expected_results = [
        sample.Sample(
            'ai_inference_container_init_time',
            29.0,
            'seconds',
            metadata,
            fake_timestamp,
        ),
        sample.Sample(
            'ai_inference_storage_model_load_time',
            22.0,
            'seconds',
            metadata,
            fake_timestamp,
        ),
        sample.Sample(
            'ai_inference_post_model_load_to_start_time',
            68.0,
            'seconds',
            metadata,
            fake_timestamp,
        ),
    ]
    self.assertEqual(results, expected_results)

  def test_get_model_load_time_multiple_pods(self):
    first_call_logs = """22:17:01 Automatically detected platform cuda
    22:17:30 Starting to load model /data/models/llama3-8b-hf...
    22:17:52 Model loading took 14.9596 GiB and 21.619846 seconds
    22:19:00 Starting vLLM API server on http://0.0.0.0:8000
    """
    second_call_logs = """22:17:05 Automatically detected platform cuda
    22:17:15 Starting to load model /data/models/llama3-8b-hf...
    22:17:30 Model loading took 14.9596 GiB and 21.619846 seconds
    22:19:45 Starting vLLM API server on http://0.0.0.0:8000
    """
    third_call_logs = """22:17:10 Automatically detected platform cuda
    22:17:30 Starting to load model /data/models/llama3-8b-hf...
    22:18:10 Model loading took 14.9596 GiB and 21.619846 seconds
    22:22:25 Starting vLLM API server on http://0.0.0.0:8000
    """
    startup_events = [
        container_service.KubernetesEvent(
            resource=container_service.KubernetesEventResource(
                kind='Pod',
                name='pod1',
            ),
            reason='Started',
            message='Started container inference-server',
            timestamp=datetime.datetime(
                2025, 7, 18, 22, 17, 1, tzinfo=tz
            ).timestamp(),
        ),
        container_service.KubernetesEvent(
            resource=container_service.KubernetesEventResource(
                kind='Pod',
                name='pod2',
            ),
            reason='Started',
            message='Started container inference-server',
            timestamp=datetime.datetime(
                2025, 7, 18, 22, 17, 5, tzinfo=tz
            ).timestamp(),
        ),
        container_service.KubernetesEvent(
            resource=container_service.KubernetesEventResource(
                kind='Pod',
                name='pod3',
            ),
            reason='Started',
            message='Started container inference-server',
            timestamp=datetime.datetime(
                2025, 7, 18, 22, 17, 10, tzinfo=tz
            ).timestamp(),
        ),
    ]

    mock_k8s_server = self._create_mock_k8s_server(hpa_enabled=True)
    mock_k8s_server.app_selector = 'vllm_inference_server'
    mock_k8s_server.cluster.GetResourceMetadataByName.return_value = [
        'pod1',
        'pod2',
        'pod3',
    ]
    mock_k8s_server.cluster.GetEvents.return_value = [
        startup_events[0],
        startup_events[1],
        startup_events[2],
    ]
    mock_k8s_server.GetStartupLogsFromPod.side_effect = [
        first_call_logs,
        second_call_logs,
        third_call_logs,
    ]
    results = kubernetes_ai_inference_benchmark.GetVLLMModelLoadTime(
        mock_k8s_server
    )
    fake_timestamp = mock_k8s_server.model_load_timestamp
    metadata = {
        'hpa_enabled': True,
        'num_pods': 3,
    }
    expected_results = [
        sample.Sample(
            'avg_ai_inference_container_init_time',
            19.667,  # avg: (29 + 10 + 20) / 3
            'seconds',
            metadata,
            fake_timestamp,
        ),
        sample.Sample(
            'avg_ai_inference_storage_model_load_time',
            25.667,  # avg: (22 + 15 + 40) / 3
            'seconds',
            metadata,
            fake_timestamp,
        ),
        sample.Sample(
            'avg_ai_inference_post_model_load_to_start_time',
            152.667,  # avg: (68 + 135 + 255) / 3
            'seconds',
            metadata,
            fake_timestamp,
        ),
    ]
    self.assertEqual(results, expected_results)


if __name__ == '__main__':
  unittest.main()

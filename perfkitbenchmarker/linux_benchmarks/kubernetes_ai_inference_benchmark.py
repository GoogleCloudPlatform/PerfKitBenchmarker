"""Benchmark to E2E Kubernetes AI Inference Benchmarks."""

import datetime
import json
import logging
import os
import re
import statistics
from typing import Any, Optional, Tuple
import zoneinfo

from absl import flags
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import container_service
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.resources import kubernetes_inference_server
from perfkitbenchmarker.resources.kubernetes import wg_serving_inference_server as k8s_server


BENCHMARK_NAME = 'kubernetes_ai_inference'
BENCHMARK_CONFIG = """
kubernetes_ai_inference:
  description:
    Benchmark for Kubernetes AI Inference.
  container_cluster:
    cloud: GCP
    type: Autopilot
    vm_count: 1
    vm_spec: *default_dual_core
    inference_server:
      model_server: vllm
      model_name: llama3-8b
      catalog_provider: gke
      catalog_components: 1-L4
      hpa_max_replicas: 10
      extra_deployment_args:
        container-image: vllm/vllm-openai:v0.8.5

      # Example of static inference server config
      # static_inference_server:
      #   endpoint: llama3-8b-vllm-service
      #   port: 8000
      #   model_id: meta-llama/Meta-Llama-3-8B
"""
_TIMESTAMP_PATTERN = r'\d{2}:\d{2}:\d{2}'

FLAGS = flags.FLAGS


_HF_TOKEN = flags.DEFINE_string(
    'k8s_ai_inference_hf_token',
    None,
    'Access Token of HuggingFace Hub, it could be either token or uri to the'
    ' secret in object storage (e.g. gs://bucket/path/to/token)',
)

_REQUEST_RATE = flags.DEFINE_string(
    'k8s_ai_inference_request_rate',
    '1,2,4,6,8',
    'Request rate to use for the benchmark.',
)

_HPA_ENABLED = flags.DEFINE_bool(
    'k8s_ai_inference_hpa_enabled',
    False,
    'Whether to enable HPA for the benchmark.',
)

_EXTRA_LPG_ARGS = flags.DEFINE_multi_string(
    'k8s_ai_inference_extra_lpg_args',
    [],
    'Extra args to pass to the latency profile generator job. example:'
    ' --k8s_ai_inference_extra_lpg_args=KEY_OF_ARG=VALUE_OF_ARG',
)

flags.DEFINE_string(
    'wg_serving_repo_url',
    'https://github.com/kubernetes-sigs/wg-serving',
    'Repository URL for WG-Serving used by serving_catalog_cli.',
)

flags.DEFINE_string(
    'wg_serving_repo_branch',
    'main',
    'Git branch of the WG-Serving repository.',
)

lpg_extra_args = {}


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  """Load and return benchmark config.

  Args:
      user_config: user supplied configuration (flags and config file)

  Returns:
      loaded benchmark configuration
  """
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if _HF_TOKEN.present:
    config['container_cluster']['inference_server'][
        'hf_token'
    ] = _HF_TOKEN.value

  for arg in _EXTRA_LPG_ARGS.value:
    key, value = arg.strip().split('=', maxsplit=1)
    lpg_extra_args[key.lstrip('-')] = value

  return config


def Prepare(_):
  """Prepare a cluster to run the inference benchmark."""


def Run(
    benchmark_spec: bm_spec.BenchmarkSpec,
) -> list[sample.Sample]:
  """Run the benchmark.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of result samples collected from the json files.
  """
  cluster: container_service.KubernetesCluster = (
      benchmark_spec.container_cluster
  )
  server: k8s_server.WGServingInferenceServer = cluster.inference_server
  if not server or not isinstance(server, k8s_server.WGServingInferenceServer):
    raise ValueError('Inference server is not initialized in the cluster.')

  metrics = []

  if _HPA_ENABLED.value:
    if server.is_remote:
      raise ValueError(
          'HPA is not supported on remote inference servers. Please use'
          ' benchmark managed inference server.'
      )

    server.EnableHPA()
    output_path = vm_util.GetTempDir() + '/inference_benchmark_result_hpa'
    RunInferenceBenchmark(cluster, server.GetCallableServer(), output_path)
    server.StopHPAPolling()

    metrics += _CollectBenchmarkResult(server, output_path)
    metrics += server.GetHPASamples()
  else:
    metrics += server.GetPodStartupSamples()
    output_path = vm_util.GetTempDir() + '/inference_benchmark_result'
    RunInferenceBenchmark(cluster, server.GetCallableServer(), output_path)
    metrics += _CollectBenchmarkResult(server, output_path)

  return metrics


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec):
  """Cleanup resources to their original state.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  del benchmark_spec


def _CollectBenchmarkResult(
    server: k8s_server.WGServingInferenceServer,
    result_path: str,
) -> list[sample.Sample]:
  """Collect the result of the inference benchmark.

  Args:
    server: The inference server resource.
    result_path: The path to the folder containing the result json files.

  Returns:
    A list of result samples collected from the json files.
  """
  results = CollectBenchmarkResult(server.GetResourceMetadata(), result_path)
  if 'vllm' in server.spec.model_server:
    model_load_metrics = GetVLLMModelLoadTime(server)
    results += model_load_metrics

  return results


def CollectBenchmarkResult(
    metadata: dict[str, Any],
    result_path: str,
) -> list[sample.Sample]:
  """Collect the result of the inference benchmark.

  Args:
    metadata: The metadata for each sample.
    result_path: The path to the folder containing the result json files.

  Returns:
    A list of result samples collected from the json files.
  """
  results = []
  for filename in os.listdir(result_path):
    if not filename.endswith('.json'):
      continue
    if filename.endswith('weighted.json'):
      continue

    file_path = os.path.join(result_path, filename)
    try:
      with open(file_path, 'r', encoding='utf-8') as f:
        result = json.load(f)
        metrics = result['metrics']

        sample_metadata = {
            'request_rate': metrics['request_rate'],
            **metadata,
        }
        timestamp = datetime.datetime.fromtimestamp(
            result['config']['start_time']['seconds']
        ).timestamp()

        for metric in metrics:
          if not metrics[metric]:
            continue
          unit = 'ms' if 'ms' in metric else ''
          results.append(
              sample.Sample(
                  metric, metrics[metric], unit, sample_metadata, timestamp
              )
          )
    except (json.JSONDecodeError, FileNotFoundError, KeyError) as e:
      logging.exception('Failed to read %s: %s', filename, e)
      raise e
  return results


def RunInferenceBenchmark(
    cluster: container_service.KubernetesCluster,
    server: kubernetes_inference_server.InferenceServerEndpoint,
    output_path: str,
) -> None:
  """Run the inference benchmark.

  Args:
    cluster: The Kubernetes cluster resource.
    server: The inference server resource.
    output_path: The path to the folder containing the result json files.

  Raises:
    RuntimeError: If the latency profile generator job fails.
  """
  if hasattr(server, 'model_id_from_path') and server.model_id_from_path:
    model_id = server.model_id_from_path
  else:
    model_id = server.model_id
  created_resources = cluster.ApplyManifest(
      'container/kubernetes_ai_inference/latency-profile-generator.yaml.j2',
      models=model_id,
      tokenizer=server.tokenizer_id,
      inference_server=server.service_name,
      inference_server_port=server.service_port,
      backend=server.backend,
      request_rate=_REQUEST_RATE.value,
      image_repo=k8s_server.FLAG_IMAGE_REPO.value,
      env_vars=lpg_extra_args,
  )
  job_resource = next(it for it in created_resources if it.startswith('job'))
  _, job_name = job_resource.split('/', maxsplit=1)

  condition = cluster.WaitForResourceForMultiConditions(
      f'job/{job_name}',
      ['jsonpath={.status.ready}=1', 'condition=Failed'],
      timeout=7200,
  )

  if 'Failed' in condition:
    raise RuntimeError('Latency profile generator job failed.')

  pod_name = cluster.RetryableGetPodNameFromJob(job_name)
  os.makedirs(output_path, exist_ok=True)
  cluster.CopyFilesFromPod(pod_name, '/benchmark_result', output_path)

  cluster.DeleteResource(f'job/{job_name}')


def GetVLLMModelLoadTime(
    server: k8s_server.WGServingInferenceServer,
) -> list[sample.Sample]:
  """Get the model load time from the logs."""
  init_time_name = 'ai_inference_container_init_time'
  model_load_time_name = 'ai_inference_storage_model_load_time'
  application_start_time_name = 'ai_inference_post_model_load_to_start_time'
  if server.model_load_timestamp is not None:
    time = server.model_load_timestamp
  else:
    now = datetime.datetime.now()
    time = now.timestamp()
  pods = server.cluster.GetResourceMetadataByName(
      'pods',
      f'app={server.app_selector}',
      output_format='name',
      output_formatter=lambda res: res.splitlines(),
  )
  if len(pods) > 1:
    init_time_name = 'avg_ai_inference_container_init_time'
    model_load_time_name = 'avg_ai_inference_storage_model_load_time'
    application_start_time_name = (
        'avg_ai_inference_post_model_load_to_start_time'
    )
  init_times = []
  model_load_times = []
  application_start_times = []
  for pod in pods:
    if pod.startswith('pod/'):
      pod_name = pod[4:]
    else:
      pod_name = pod
    result_stdout = server.GetStartupLogsFromPod(pod_name)
    if _IsUsingTPU(server.spec.catalog_components):
      init_time, model_load_time, application_start_time = (
          _ParseTPUModelLoadTimeMetrics(server, result_stdout, pod_name)
      )
    else:
      init_time, model_load_time, application_start_time = (
          _ParseModelLoadTimeMetrics(server, result_stdout, pod_name)
      )
    init_times.append(init_time)
    model_load_times.append(model_load_time)
    application_start_times.append(application_start_time)
  metadata = server.GetResourceMetadata()
  metadata.update({
      'num_pods': len(pods),
  })
  return [
      sample.Sample(
          init_time_name,
          round(statistics.mean(init_times), 3),
          'seconds',
          metadata,
          time,
      ),
      sample.Sample(
          model_load_time_name,
          round(statistics.mean(model_load_times), 3),
          'seconds',
          metadata,
          time,
      ),
      sample.Sample(
          application_start_time_name,
          round(statistics.mean(application_start_times), 3),
          'seconds',
          metadata,
          time,
      ),
  ]


def _ParseTPUModelLoadTimeMetrics(
    server: k8s_server.WGServingInferenceServer,
    result_stdout: str,
    pod_name: str,
) -> Tuple[float, float, float]:
  """Parse the model load time metrics from the logs."""
  model_load_start_timestamp = None
  model_load_end_timestamp = None
  vllm_start_timestamp = None
  log_lines = result_stdout.splitlines()
  events = server.cluster.GetEvents()
  startup_event = None
  for event in events:
    if (
        event.resource.kind == 'Pod'
        and event.resource.name == pod_name
        and 'Started container inference-server' in event.message
    ):
      startup_event = event
  if startup_event is None:
    raise ValueError(
        f'No events found for pod {pod_name} with message "Started container'
        ' inference-server".'
    )
  container_init_timestamp = _FormatUTCTimeStampToLocalTime(
      server,
      startup_event.timestamp,
  )
  for line in log_lines:
    # Check model load start timestamp
    if (
        'Downloading weights from HF' in line
        or 'Found weights from local' in line
    ) and model_load_start_timestamp is None:
      model_load_start_timestamp = _ParseInferenceServerTimeStamp(line)
    # Check model load end timestamp
    if 'Compilation finished in' in line:
      model_load_end_timestamp = _ParseInferenceServerTimeStamp(line)
    # Check when overall container starts
    if 'Starting vLLM API server' in line:
      vllm_start_timestamp = _ParseInferenceServerTimeStamp(line)
      break
  if container_init_timestamp is None:
    raise ValueError('Container init timestamp is not found in the logs.')
  if model_load_start_timestamp is None:
    raise ValueError('Model load start timestamp is not found in the logs.')
  if model_load_end_timestamp is None:
    raise ValueError('Model load end timestamp is not found in the logs.')
  if vllm_start_timestamp is None:
    raise ValueError('VLLM start timestamp is not found in the logs.')
  init_time = GetTimeDifference(
      container_init_timestamp, model_load_start_timestamp
  )
  model_load_time = GetTimeDifference(
      model_load_start_timestamp, model_load_end_timestamp
  )
  application_start_time = GetTimeDifference(
      model_load_end_timestamp, vllm_start_timestamp
  )
  return init_time, model_load_time, application_start_time


def _ParseModelLoadTimeMetrics(
    server: k8s_server.WGServingInferenceServer,
    result_stdout: str,
    pod_name: str,
) -> Tuple[float, float, float]:
  """Parse the model load time metrics from the logs."""
  model_load_start_timestamp = None
  model_load_end_timestamp = None
  vllm_start_timestamp = None
  log_lines = result_stdout.splitlines()
  events = server.cluster.GetEvents()
  startup_event = None
  for event in events:
    if (
        event.resource.kind == 'Pod'
        and event.resource.name == pod_name
        and 'Started container inference-server' in event.message
    ):
      startup_event = event
  if startup_event is None:
    raise ValueError(
        f'No events found for pod {pod_name} with message "Started container'
        ' inference-server".'
    )
  container_init_timestamp = _FormatUTCTimeStampToLocalTime(
      server,
      startup_event.timestamp,
  )
  for line in log_lines:
    if 'Starting to load model' in line and model_load_start_timestamp is None:
      model_load_start_timestamp = _ParseInferenceServerTimeStamp(line)
    # Check model load end timestamp
    if 'Model loading took' in line:
      model_load_end_timestamp = _ParseInferenceServerTimeStamp(line)
    # Check when overall container starts
    if 'Starting vLLM API server on' in line:
      vllm_start_timestamp = _ParseInferenceServerTimeStamp(line)
      break
  if container_init_timestamp is None:
    raise ValueError('Container init timestamp is not found in the logs.')
  if model_load_start_timestamp is None:
    raise ValueError('Model load start timestamp is not found in the logs.')
  if model_load_end_timestamp is None:
    raise ValueError('Model load end timestamp is not found in the logs.')
  if vllm_start_timestamp is None:
    raise ValueError('VLLM start timestamp is not found in the logs.')
  init_time = GetTimeDifference(
      container_init_timestamp, model_load_start_timestamp
  )
  model_load_time = GetTimeDifference(
      model_load_start_timestamp, model_load_end_timestamp
  )
  application_start_time = GetTimeDifference(
      model_load_end_timestamp, vllm_start_timestamp
  )
  return init_time, model_load_time, application_start_time


def GetTimeDifference(start_time: str, end_time: str) -> float:
  """Calculates the duration in seconds between a start and end time.

  If end time is before start time, we assume it is on the next day.

  Args:
      start_time: The starting time in "HH:MM:SS" format.
      end_time: The ending time in "HH:MM:SS" format.

  Returns:
      The duration in seconds as an integer.
  """
  time_format = '%H:%M:%S'
  if start_time is None or end_time is None:
    return 0

  start_time = datetime.datetime.strptime(start_time, time_format)
  end_time = datetime.datetime.strptime(end_time, time_format)
  if end_time < start_time:
    end_time += datetime.timedelta(days=1)
  time_difference = end_time - start_time

  return int(time_difference.total_seconds())


def _ParseInferenceServerTimeStamp(log_line: str) -> Optional[str]:
  """Parses a timestamp from a log line.

  Args:
      log_line: The line of log to parse.

  Returns:
      The timestamp string if found, otherwise None.
  """
  match = re.search(_TIMESTAMP_PATTERN, log_line)
  if not match:
    logging.warning('Failed to parse timestamp from log: %s', log_line)
    return None
  return match.group(0)


def _IsUsingTPU(components: str) -> bool:
  """Returns whether the components are using TPU.

  TPU component format: v<version>e-<rows>x<cols>
  Examples of TPU components:
  v6e-2x4
  v5e-2x4

  Args:
    components: The components of the inference server, separated by commas.

  Returns:
    Whether the components are using TPU.
  """
  pattern = re.compile(r'v\d+e-\d+x\d+')
  return bool(pattern.search(components))


def _FormatUTCTimeStampToLocalTime(
    server: k8s_server.WGServingInferenceServer, timestamp: Optional[float]
) -> Optional[str]:
  """Returns the time zone of the pod."""
  if server.timezone is None:
    raise ValueError('Local Pod timezone was not found.')
  timezone = zoneinfo.ZoneInfo(server.timezone)
  utc_dt = datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)
  target_dt = utc_dt.astimezone(timezone)
  return target_dt.strftime('%H:%M:%S')

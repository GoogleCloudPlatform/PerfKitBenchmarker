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
"""Module for Kubernetes wg-serving Inference Server resource."""
from __future__ import annotations
import base64
import collections
import concurrent.futures
import dataclasses
import datetime
import logging
import threading
from typing import Any, Callable, Dict, Optional
from absl import flags
from perfkitbenchmarker import container_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import object_storage_service
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.resources import kubernetes_inference_server
from perfkitbenchmarker.resources.kubernetes import wg_serving_inference_server_spec
import yaml


FLAGS = flags.FLAGS
# Temporary path for generated inference server manifest
# it should match the output path defined in
# data/container/kubernetes_ai_inference/serving_catalog_cli.yaml.j2
_OUTPUT_PATH = '/output/output.yaml'

FLAG_IMAGE_REPO = flags.DEFINE_string(
    'k8s_inference_server_image_repo',
    'us-docker.pkg.dev/p3rf-gke/public',
    'The image repo path where the container images are stored (e.g.'
    ' gcr.io/gke-release)',
)

FLAG_GCS_BUCKET = flags.DEFINE_string(
    'k8s_inference_server_gcs_bucket',
    None,
    'The GCS bucket that has model data for inference server to use.',
)

WG_SERVING_REPO_URL = flags.DEFINE_string(
    'wg_serving_repo_url',
    'https://github.com/kubernetes-sigs/wg-serving',
    'URL of the WG Serving repository.',
)

WG_SERVING_REPO_BRANCH = flags.DEFINE_string(
    'wg_serving_repo_branch',
    'main',
    'Branch of the WG Serving repository.',
)


@dataclasses.dataclass
class PodStartupMetrics:
  """Dataclass for pod startup metrics."""

  pod_name: str
  metadata: Dict[str, Any]
  pod_created_timestamp: float
  pod_scheduled_timestamp: float
  container_started_timestamp: float
  workload_ready_timestamp: float
  pod_startup_logs: str


class BaseWGServingInferenceServer(
    kubernetes_inference_server.BaseKubernetesInferenceServer
):
  """Base class for WG-Serving Inference Server resource."""

  def __getstate__(self):
    state = self.__dict__.copy()
    del state['_monitor_executor']
    del state['_monitor_future_map']
    return state

  deployment_metadata: Optional[Dict[str, Any]]
  service_name: Optional[str]
  model_id: Optional[str]
  timezone: Optional[str]
  model_id_from_path: Optional[str]
  tokenizer_id: Optional[str]
  service_port: Optional[int]
  app_selector: Optional[str]
  spec: wg_serving_inference_server_spec.WGServingInferenceServerConfigSpec

  def __init__(
      self,
      spec: wg_serving_inference_server_spec.WGServingInferenceServerConfigSpec,
      cluster: container_service.KubernetesCluster,
  ):
    super().__init__(spec, cluster)
    self.model_load_timestamp = None
    self.deployment_metadata = None
    self.max_replicas: int = 1
    self._monitor_executor: Optional[concurrent.futures.ThreadPoolExecutor] = (
        None
    )
    self._monitor_future_map: Dict[
        str, concurrent.futures.Future[Optional[PodStartupMetrics]]
    ] = {}
    self.is_remote: bool = False
    self.user_managed = False
    self.timezone = None

    if spec.static_inference_server is not None:
      self._InitializeUserManagedStateIfSpecified(spec.static_inference_server)

  def GetStartupLogsFromPod(self, pod_name: str) -> str:
    """Returns the inference server logs from the pod."""
    if self._monitor_future_map[pod_name]:
      pod_info = self._monitor_future_map[pod_name].result()
      if pod_info:
        return pod_info.pod_startup_logs
    raise ValueError(f'Pod {pod_name} was not found in monitor future map.')

  def GetPodTimeZone(self, pod_name: str) -> str:
    """Returns the time zone of the pod."""
    log_cmd = [
        'exec',
        '-it',
        pod_name,
        '--',
        'cat',
        '/etc/timezone',
    ]
    stdout, _, _ = container_service.RunKubectlCommand(log_cmd)
    stdout_split = stdout.splitlines()
    return stdout_split[0]

  def GetResourceMetadata(self) -> Dict[str, Any]:
    """Returns the resource metadata."""
    result = super().GetResourceMetadata()
    result.update({
        'model': getattr(self, 'model_id', 'unknown'),
        'tokenizer_id': getattr(self, 'tokenizer_id', 'unknown'),
        'model_server': self.spec.model_server,
    })

    return result

  def GetCallableServer(
      self,
  ) -> kubernetes_inference_server.InferenceServerEndpoint:
    """Returns the callable server."""
    return kubernetes_inference_server.InferenceServerEndpoint(
        deployment_metadata=self.deployment_metadata,
        service_name=self.service_name,
        model_id=self.model_id,
        backend=self.spec.model_server,
        tokenizer_id=self.tokenizer_id,
        service_port=str(self.service_port),
        model_id_from_path=self.model_id_from_path,
    )

  def _RefreshDeploymentMetadata(self) -> None:
    """Refreshes the deployment metadata."""
    if self.is_remote:
      return
    if not self.app_selector:
      raise ValueError('app_selector is not set.')
    self.deployment_metadata = next((
        deployment
        for deployment in self.cluster.GetResourceMetadataByName(
            'deployments',
            f'app={self.app_selector}',
            should_pre_log=False,
            suppress_logging=True,
        ).get('items', [])
        if deployment
    ))

  def _InitializeUserManagedStateIfSpecified(
      self,
      static_inference_server: wg_serving_inference_server_spec.WGServingStaticInferenceServerConfigSpec,
  ) -> None:
    """Initializes the state if user-managed resources are specified."""
    logging.info(
        'Configuring resource using static_inference_server: %s',
        static_inference_server,
    )

    self.service_name, self.service_port, self.model_id, self.tokenizer_id = (
        static_inference_server.endpoint,
        static_inference_server.port,
        static_inference_server.model_id,
        static_inference_server.tokenizer_id,
    )

    try:
      service_metadata = self.cluster.GetResourceMetadataByName(
          f'service/{self.service_name}'
      )
      if not service_metadata:
        raise KeyError(
            f'User-managed service {self.service_name} not found in cluster.'
        )
      self.app_selector = (
          service_metadata.get('spec', {}).get('selector', {}).get('app')
      )
      if not self.app_selector:
        raise KeyError(
            'No app selector found in user-managed service'
            f' {self.service_name}.'
        )
      self._RefreshDeploymentMetadata()
      logging.info(
          'Successfully attached to user-managed inference server %s',
          self.service_name,
      )
    except (KeyError, IndexError, errors.VmUtil.IssueCommandError) as e:
      if not self.model_id:
        raise errors.Config.MissingOption(
            'model_id and tokenizer_id are required for remote inference server'
        )
      if not self.tokenizer_id:
        self.tokenizer_id = self.model_id
      self.is_remote = True
      self.user_managed = True
      logging.warning(
          'Failed to found inference server in cluster, assuming it is'
          ' remote. %s',
          e,
      )

  def _MonitorParseTimestamp(
      self, timestamp_str: Optional[str]
  ) -> Optional[float]:
    """Parses a timestamp string to a float unix timestamp."""
    if not timestamp_str:
      return None
    try:
      dt = datetime.datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
      return dt.timestamp()
    except ValueError:
      logging.warning('Could not parse timestamp: %s', timestamp_str)
      return None

  def _MonitorPodStartup(
      self, pod_name: str, pod_metadata: Dict[str, Any]
  ) -> Optional[PodStartupMetrics]:
    """Waits for a single pod to be ready and extracts its metrics."""
    try:
      if not any(
          condition.get('type') == 'Ready'
          for condition in pod_metadata.get('status', {}).get('conditions', [])
      ):
        logging.info('Waiting for pod %s to be Ready.', pod_name)
        self.cluster.WaitForResource(
            f'pod/{pod_name}', 'Ready', timeout=self.spec.deployment_timeout
        )
        pod_metadata = self.cluster.GetResourceMetadataByName(
            f'pod/{pod_name}', should_pre_log=False, suppress_logging=True
        )

      pod_created_timestamp = self._MonitorParseTimestamp(
          pod_metadata.get('metadata', {}).get('creationTimestamp')
      )
      if not pod_created_timestamp:
        raise ValueError(
            "Missing critical timestamp 'pod_created_timestamp' for pod"
            f' {pod_name}'
        )
      pod_scheduled_timestamp = None
      workload_ready_timestamp = None
      for condition in pod_metadata.get('status', {}).get('conditions', []):
        if condition.get('type') == 'PodScheduled':
          pod_scheduled_timestamp = self._MonitorParseTimestamp(
              condition.get('lastTransitionTime')
          )
        if condition.get('type') == 'Ready':
          workload_ready_timestamp = self._MonitorParseTimestamp(
              condition.get('lastTransitionTime')
          )
      if not pod_scheduled_timestamp:
        raise ValueError(
            "Missing critical timestamp 'pod_scheduled_timestamp' for pod"
            f' {pod_name}'
        )
      if not workload_ready_timestamp:
        raise ValueError(
            "Missing critical timestamp 'workload_ready_timestamp' for pod"
            f' {pod_name}'
        )

      container_started_timestamp = next(
          (
              self._MonitorParseTimestamp(
                  container_status.get('state', {})
                  .get('running', {})
                  .get('startedAt')
              )
              for container_status in pod_metadata.get('status', {}).get(
                  'containerStatuses', []
              )
              if container_status.get('name') == self.WORKLOAD_NAME
          ),
          None,
      )
      if not container_started_timestamp:
        raise ValueError(
            "Missing critical timestamp 'container_started_timestamp' for pod"
            f' {pod_name}'
        )
      pod_startup_logs = self.GetInferenceServerLogsFromPod(f'pod/{pod_name}')
      if self.timezone is None:
        self.timezone = self.GetPodTimeZone(pod_name)

      # Collect node / instance metadata (best-effort)
      startup_metadata = {}
      node_name = pod_metadata.get('spec', {}).get('nodeName')
      if node_name:
        startup_metadata['node_name'] = node_name
        try:
          node_metadata = self.cluster.GetResourceMetadataByName(
              f'node/{node_name}', should_pre_log=False, suppress_logging=True
          )
          node_labels = node_metadata.get('metadata', {}).get('labels', {}) or {}
          instance_type = (
              node_labels.get('node.kubernetes.io/instance-type')
              or node_labels.get('beta.kubernetes.io/instance-type')
          )
          if instance_type:
            startup_metadata['node_instance_type'] = instance_type
          instance_family = (
              node_labels.get('karpenter.k8s.aws/instance-family')
              or node_labels.get('eks.amazonaws.com/instance-family')
          )
          if not instance_family and instance_type:
            instance_family = instance_type.split('.', 1)[0]
          if instance_family:
            startup_metadata['node_instance_family'] = instance_family
          instance_size = node_labels.get('karpenter.k8s.aws/instance-size')
          if instance_size:
            startup_metadata['node_instance_size'] = instance_size
          gpu_product = node_labels.get('nvidia.com/gpu.product')
          if gpu_product:
            startup_metadata['gpu_product'] = gpu_product
        except Exception as e:  # best-effort only
          logging.info(
              'Failed to fetch node metadata for %s: %s', node_name, e
          )
      logging.info('Successfully collected metrics for pod %s.', pod_name)
      return PodStartupMetrics(
          pod_name=pod_name,
          metadata=startup_metadata,
          pod_startup_logs=pod_startup_logs,
          pod_created_timestamp=pod_created_timestamp,
          pod_scheduled_timestamp=pod_scheduled_timestamp,
          container_started_timestamp=container_started_timestamp,
          workload_ready_timestamp=workload_ready_timestamp,
      )

    except errors.VmUtil.IssueCommandTimeoutError:
      logging.warning(
          'Pod %s did not become Ready for metric collection.', pod_name
      )
      return None
    except ValueError as e:
      logging.warning('Failed to collect metrics for pod %s: %s', pod_name, e)
      return None

  def MonitorPods(self) -> None:
    """Submits tasks to monitor pods."""
    if self.is_remote or not self._monitor_executor:
      raise errors.Resource.CreationError(
          'Pod monitoring is not supported for this inference server. Make sure'
          ' that the inference server is not remote or user-managed.'
      )
    pod_list_metadata = self.cluster.GetResourceMetadataByName(
        'pods',
        f'app={self.app_selector}',
        should_pre_log=False,
        suppress_logging=True,
    )
    if not pod_list_metadata or not pod_list_metadata.get('items'):
      logging.warning('No pods found for metric collection.')
      return
    initial_pod_items = pod_list_metadata['items']
    for pod_item in initial_pod_items:
      pod_name = pod_item.get('metadata', {}).get('name')
      initial_pod_metadata = pod_item
      if not pod_name:
        logging.warning('Could not find pod name in metadata: %s', pod_item)
        continue
      if pod_name in self._monitor_future_map:
        continue
      self._monitor_future_map[pod_name] = self._monitor_executor.submit(
          self._MonitorPodStartup,
          pod_name,
          initial_pod_metadata,
      )
    logging.info(
        'Submitted %s tasks to monitor inference server pods.',
        len(self._monitor_future_map),
    )

  def _CollectStartupMonitorMetrics(self) -> dict[str, PodStartupMetrics]:
    """Collects pod's startup metrics from monitor."""
    collected_metrics_map = {}
    for _, future in self._monitor_future_map.items():
      result = future.result()
      if result:
        collected_metrics_map[result.pod_name] = result
    return collected_metrics_map

  def GetPodStartupSamples(self) -> list[sample.Sample]:
    """Collects and returns pod startup metrics as a list of samples.

    Returns:
        list[sample.Sample]: A list of samples, each representing a
        pod startup metric.
    """
    if self.is_remote:
      return []

    metrics_to_collect_map: dict[str, Callable[[PodStartupMetrics], float]] = {
        'startup_latency': (
            lambda m: m.workload_ready_timestamp - m.pod_created_timestamp
        ),
        'node_readiness': (
            lambda m: m.pod_scheduled_timestamp - m.pod_created_timestamp
        ),
        'pod_startup': (
            lambda m: m.container_started_timestamp - m.pod_scheduled_timestamp
        ),
        'started_to_ready': (
            lambda m: m.workload_ready_timestamp - m.container_started_timestamp
        ),
    }

    samples = []
    metadata = self.GetResourceMetadata()
    startup_metrics_map = self._CollectStartupMonitorMetrics()
    for pod_name, pod_startup_data in startup_metrics_map.items():
      sample_metadata = {
          'pod_name': pod_name,
          **metadata,
          **pod_startup_data.metadata,
      }
      for (
          sample_metric_name,
          metric_calculator,
      ) in metrics_to_collect_map.items():
        samples.append(
            sample.Sample(
                sample_metric_name,
                metric_calculator(pod_startup_data),
                's',
                sample_metadata,
                pod_startup_data.pod_created_timestamp,
            )
        )
    return samples

  def GetInferenceServerLogsFromPod(self, pod_name: str) -> str:
    """Returns the inference server logs from the pod."""
    log_cmd = [
        'logs',
        pod_name,
        '-c',
        'inference-server',
    ]
    stdout, _, _ = container_service.RunKubectlCommand(log_cmd)
    return stdout

  def GetInferenceServerDeploymentName(self) -> str:
    """Returns the name of the inference server deployment."""
    if not self.deployment_metadata:
      raise errors.Resource.CreationError(
          'Deployment metadata is required to get inference server details.'
      )
    return self.deployment_metadata['metadata']['name']

  def _PostCreate(self) -> None:
    """Post-creation steps for the Kubernetes Inference Server."""
    if self.is_remote or not self.app_selector or not self.deployment_metadata:
      logging.warning(
          'Initial pod startup metrics cannot be collected because the server'
          ' is remote or essential identifiers (app_selector, deployment_name)'
          ' are missing.'
      )
      return
    if not self._monitor_executor:
      self._monitor_executor = concurrent.futures.ThreadPoolExecutor(
          max_workers=self.max_replicas,
          thread_name_prefix='pod-metric-poller',
      )
      self._monitor_future_map = {}
    self.MonitorPods()

  def _PreDelete(self) -> None:
    """Pre-deletion steps for the Kubernetes Inference Server."""
    if self._monitor_executor:
      self._monitor_executor.shutdown(wait=False)
      self._monitor_executor = None

  def _Exists(self) -> bool:
    """Returns true if the inference server deployment exists and is ready."""
    if self.deleted:
      return False
    if self.is_remote:
      return True
    return bool(self.deployment_metadata)

  @property
  def replica(self) -> int:
    """Returns the number of replicas of the inference server."""
    self._RefreshDeploymentMetadata()
    if not self.deployment_metadata:
      return 0
    return self.deployment_metadata.get('status', {}).get('replicas', 0)


class WGServingInferenceServer(BaseWGServingInferenceServer):
  """Implementation of Kubernetes WG Serving Inference Server.

  It manages the deployment of the inference server whose manifest is generated
  by wg-serving catalog CLI.
  """

  INFERENCE_SERVER_TYPE = (
      wg_serving_inference_server_spec.REGISTERED_INFERENCE_SERVER_TYPE
  )
  WORKLOAD_NAME = 'inference-server'

  def __getstate__(self):
    state = super().__getstate__()
    del state['_hpa_poller_executor']
    del state['_hpa_polling_stop_event']

    return state

  def __init__(
      self,
      spec: wg_serving_inference_server_spec.WGServingInferenceServerConfigSpec,
      cluster: container_service.KubernetesCluster,
  ):
    super().__init__(spec, cluster)

    if not spec:
      raise errors.Resource.CreationError(
          'DefaultInferenceServerConfigSpec is required for'
          ' DefaultKubernetesInferenceServer.'
      )
    self.huggingface_token = self.spec.hf_token
    self.max_replicas = self.spec.hpa_max_replicas
    self.hpa_name = 'hpa/k8s-ai-inference-benchmark'
    self._hpa_polling_stop_event = None
    self._hpa_poller_executor = None
    self._hpa_enabled = False
    self._hpa_scale_up_time_series = []
    self._hpa_scale_up_time_series_lock = threading.Lock()
    self.accelerator_component = self._GetAcceleratorComponent()
    self.accelerator_type = self._GetAcceleratorType()
    self.accelerator_count = self._GetAcceleratorCount()
    self.created_resources = []

  def GetResourceMetadata(self) -> dict[str, Any]:
    metadata = super().GetResourceMetadata()
    storage_type = self.GetStorageType()
    metadata.update({
        'catalog_components': self.spec.catalog_components,
        'hpa_enabled': self._hpa_enabled,
        'storage_type': storage_type,
        'accelerator_component': self.accelerator_component,
        'accelerator_type': self.accelerator_type,
        'accelerator_count': self.accelerator_count,
    })
    if self.spec.runtime_class_name:
      metadata['runtime_class_name'] = self.spec.runtime_class_name
    if FLAGS.cloud == 'AWS':
      metadata['aws_spot_instances'] = bool(FLAGS.aws_spot_instances)
    return metadata

  def GetStorageType(self) -> str:
    """Returns the storage type of the inference server."""
    if 'gcsfuse' in self.spec.catalog_components:
      return 'gcsfuse'
    return 'huggingface'

  def _GetAcceleratorComponent(self) -> str:
    """Returns the accelerator component of the inference server."""
    components = self.spec.catalog_components.split(',')
    for component in components:
      lowered = component.lower()
      for gpu in virtual_machine.VALID_GPU_TYPES:
        if gpu in lowered:
          return component
    return 'unknown'

  def _GetAcceleratorType(self) -> str:
    """Returns the accelerator type of the inference server."""
    if self.accelerator_component == 'unknown':
      return 'unknown'
    if 'v6e' in self.accelerator_component:
      return 'v6e'
    return self.accelerator_component.split('-')[1]

  def _GetAcceleratorCount(self) -> int:
    """Returns the accelerator count of the inference server."""
    if self.accelerator_component == 'unknown':
      return 0
    if 'v6e' in self.accelerator_component:
      xpart = self.accelerator_component.split('-')[1]
      xnumbers = xpart.split('x')
      total = 1
      for xnumber in xnumbers:
        total *= int(xnumber)
      return total
    return int(self.accelerator_component.split('-')[0])

  def _InjectDefaultHuggingfaceToken(self) -> None:
    """Injects HuggingFace token into the cluster."""
    if not self.huggingface_token.startswith('hf_'):
      logging.info(
          'Fetching token from storage service at uri: %s',
          self.huggingface_token,
      )
      secret_file_path = vm_util.PrependTempDir('hf_token.secret')
      storage_service = object_storage_service.GetObjectStorageClass(
          FLAGS.cloud
      )()
      storage_service.PrepareService(self.cluster.region)
      storage_service.Copy(self.huggingface_token, secret_file_path)

      with open(secret_file_path, mode='r+') as secret_file:
        self.huggingface_token = secret_file.read()
        secret_file.seek(0)
        secret_file.truncate()

    created_resources = list(
        self.cluster.ApplyManifest(
            'container/kubernetes_ai_inference/huggingface_token_secret.yaml.j2',
            encode_token=base64.b64encode(
                self.huggingface_token.encode('utf-8')
            ).decode('utf-8'),
            should_log_file=False,
        )
    )
    self.created_resources.extend(created_resources)

  def _GetInferenceServerManifest(self) -> str:
    """Generates and retrieves the inference server manifest content."""
    provider = self.spec.cloud.lower()
    if provider == 'gcp':
      provider = 'gke'
    generate_args = {
        'kind': 'core/deployment',
        'model-server': self.spec.model_server,
        'model': self.spec.model_name,
        'provider': provider,
        'components': self.spec.catalog_components,
        **self.spec.extra_deployment_args,
    }
    created_resources = list(
        self.cluster.ApplyManifest(
            'container/kubernetes_ai_inference/serving_catalog_cli.yaml.j2',
            image_repo=FLAG_IMAGE_REPO.value,
            wg_serving_repo_url=WG_SERVING_REPO_URL.value,
            wg_serving_repo_branch=WG_SERVING_REPO_BRANCH.value,
            generate_args=' '.join(
                [f'--{k} {v}' for k, v in generate_args.items()]
            ),
        )
    )

    job_resource = next(it for it in created_resources if it.startswith('job'))
    _, job_name = job_resource.split('/', maxsplit=1)
    try:
      pod_name = self.cluster.RetryableGetPodNameFromJob(job_name)

      self.cluster.WaitForResource(f'pod/{pod_name}', 'Ready', timeout=600)
      inference_server_manifest = self.cluster.GetFileContentFromPod(
          pod_name, _OUTPUT_PATH
      )

      # Add runtime class name to the deployment spec if specified.
      if self.spec.runtime_class_name:
        docs = []
        for doc in yaml.safe_load_all(inference_server_manifest):
          if doc.get('kind') == 'Deployment':
            doc['spec']['template']['spec'][
                'runtimeClassName'
            ] = self.spec.runtime_class_name
          docs.append(doc)
        inference_server_manifest = yaml.dump_all(docs)

      logging.info('Cleaned up manifest generation job %s', job_name)
      return inference_server_manifest
    finally:
      self.cluster.DeleteResource(f'job/{job_name}', ignore_not_found=True)

  def _ProvisionGPUNodePool(self):
    """Provisions cloud-specific GPU node pool for inference workloads."""
    if FLAGS.cloud == 'AWS':
      use_spot = bool(FLAGS.aws_spot_instances)
      self.cluster.ApplyManifest(
          'container/kubernetes_ai_inference/aws-gpu-nodepool.yaml.j2',
          gpu_nodepool_name='gpu',
          gpu_consolidate_after='1h',
          gpu_consolidation_policy='WhenEmpty',
          karpenter_nodeclass_name='default',  # must exist already
          gpu_capacity_types=['spot'] if use_spot else ['on-demand'],
          gpu_arch=['amd64'],
          gpu_instance_families=['g6','p5'],
          gpu_taint_key='nvidia.com/gpu',
      )
    elif FLAGS.cloud == 'Azure':
      self.cluster.ApplyManifest(
          'container/kubernetes_ai_inference/azure-gpu-nodepool.yaml.j2',
          gpu_capacity_types=['on-demand'],
          gpu_sku_name=[self.accelerator_type],
      )

  def _ParseInferenceServerDeploymentMetadata(self) -> None:
    """Parses deployment metadata to get server details and stores them."""
    try:
      if not self.deployment_metadata:
        raise errors.Resource.CreationError(
            'Deployment metadata is required to parse inference server details.'
        )
      containers = (
          self.deployment_metadata.get('spec', {})
          .get('template', {})
          .get('spec', {})
          .get('containers', [])
      )
      inference_server_container = next((
          c
          for c in containers
          if isinstance(c, dict) and c.get('name') == self.WORKLOAD_NAME
      ))
      self.tokenizer_id = self.model_id = next((
          env_var.get('value')
          for env_var in inference_server_container.get('env', [])
          if env_var.get('name') == 'MODEL_ID'
      ))
      if self.spec.extra_deployment_args:
        model_path = self.spec.extra_deployment_args.get('model-path', None)
        if model_path:
          self.model_id_from_path = '/data/models/' + model_path
      if not hasattr(self, 'model_id_from_path') or not self.model_id_from_path:
        self.model_id_from_path = None
    except (KeyError, IndexError, TypeError, StopIteration) as e:
      logging.exception(
          'Error parsing MODEL_ID from deployment %s, Please ensure the'
          ' deployment is deployed with the correct manifest: %s',
          self.deployment_metadata,
          e,
      )
      raise errors.Resource.CreationError(
          'Failed to parse MODEL_ID from deployment'
          f' {self.deployment_metadata}: {e}'
      )

  def _ParseAndStoreInferenceServerDetails(self, manifest_content: str) -> None:
    """Parses manifest to get server details and stores them."""
    try:
      loaded_manifest_docs = list(yaml.safe_load_all(manifest_content))
      if not loaded_manifest_docs:
        raise errors.Resource.CreationError(
            'Generated manifest content is empty.'
        )

      for resource_config in loaded_manifest_docs:
        kind = resource_config.get('kind')
        metadata = resource_config.get('metadata', {})
        spec_data = resource_config.get('spec', {})

        if kind == 'Service':
          self.service_name = metadata.get('name')
          self.service_port = next(
              (port['port'] for port in spec_data.get('ports', []) if port),
              None,
          )
          self.app_selector = spec_data.get('selector', {}).get('app')
        elif kind == 'Deployment':
          self.deployment_metadata = resource_config
          self._ParseInferenceServerDeploymentMetadata()

      if not all([
          self.service_name,
          self.service_port is not None,
          self.deployment_metadata,
          self.model_id,
          self.app_selector,
      ]):
        logging.error(
            'Failed to parse all server details: service_name=%s,'
            ' service_port=%s, deployment_name=%s, model_id=%s,'
            ' app_selector=%s',
            self.service_name,
            self.service_port,
            self.deployment_metadata,
            self.model_id,
            self.app_selector,
        )
        raise errors.Resource.CreationError(
            'Could not parse essential server details from the generated'
            ' manifest.'
        )
    except (
        KeyError,
        IndexError,
        TypeError,
        yaml.YAMLError,
        StopIteration,
    ) as e:
      raise errors.Resource.CreationError(
          f'Failed to parse inference server manifest: {e}'
      )

  def _HandlerHPAScaleEvent(
      self,
      on_hpa_scale_event: Callable[[int, int, str], None],
      current_replicas: int,
      previous_replicas: int,
      transition_time: str,
  ):
    """Handles HPA scale up event."""
    if current_replicas > previous_replicas:
      self.MonitorPods()
      timestamp = datetime.datetime.fromisoformat(transition_time).timestamp()
      with self._hpa_scale_up_time_series_lock:
        self._hpa_scale_up_time_series.append(timestamp)

    if on_hpa_scale_event:
      on_hpa_scale_event(current_replicas, previous_replicas, transition_time)

  def _PollForHPAEvents(
      self,
      initial_replicas: int,
      stop_event: threading.Event,
      on_hpa_scale_event: Callable[[int, int, int], None],
  ):
    """Polls for HPA scale events."""
    last_replicas = initial_replicas
    last_transition = None
    while not stop_event.is_set():
      try:
        hpa = self.cluster.GetResourceMetadataByName(
            self.hpa_name, should_pre_log=False, suppress_logging=True
        )

        status = hpa.get('status', {})
        current_replica = status.get('currentReplicas', 0)
        transition_time = status.get('lastScaleTime', None)

        if (
            current_replica != last_replicas
            and transition_time != last_transition
        ):
          logging.info(
              'HPA scale event detected: %s -> %s',
              last_replicas,
              current_replica,
          )
          if self._hpa_poller_executor:
            self._hpa_poller_executor.submit(
                self._HandlerHPAScaleEvent,
                on_hpa_scale_event,
                current_replica,
                last_replicas,
                transition_time,
            )
          last_replicas = current_replica
          last_transition = transition_time
      except errors.VmUtil.IssueCommandTimeoutError as e:
        logging.exception(
            'Timeout when polling for HPA events, it may be caused by IAM'
            ' permission issue. Please check the log and your IAM bindings for'
            ' more details.'
        )
        raise e
      stop_event.wait(10)

  def StopHPAPolling(self):
    """Stops the HPA scale event poller."""
    if self._hpa_polling_stop_event:
      self._hpa_polling_stop_event.set()
    if self._hpa_poller_executor:
      self._hpa_poller_executor.shutdown(wait=False)
      self._hpa_poller_executor = None
    logging.info('HPA polling stopped.')

  def EnableHPA(
      self, on_hpa_scale_event: Optional[Callable[[int, int, int], None]] = None
  ) -> None:
    """Enable HPA for the inference server deployment."""
    if self.is_remote or not self.deployment_metadata:
      raise errors.Resource.CreationError(
          'Cannot enable HPA for remote or user-managed inference server.'
      )

    try:
      adapter_resources = list(
          self.cluster.ApplyManifest(
              'container/kubernetes_ai_inference/custom-metrics-stackdriver-adapter.yaml'
          )
      )
      self.created_resources.extend(adapter_resources)
      for deployment in filter(
          lambda it: str(it).startswith('deployment'), adapter_resources
      ):
        self.cluster.WaitForResource(
            f'{deployment}',
            'available',
            namespace='custom-metrics',
            timeout=600,
        )
    except errors.VmUtil.IssueCommandTimeoutError as e:
      logging.exception(
          'Timeout when enabling HPA, it may be caused by IAM permission issue.'
          ' Please check the log and your IAM bindings for more details.'
      )
      raise e

    self.cluster.DeleteResource(self.hpa_name, ignore_not_found=True)
    try:
      created_resources = list(
          self.cluster.ApplyManifest(
              'container/kubernetes_ai_inference/hpa.custom_metric.yaml.j2',
              hpa_min_replicas=self.spec.hpa_min_replicas,
              hpa_max_replicas=self.spec.hpa_max_replicas,
              custom_metric_name=self.spec.hpa_custom_metric_name,
              hpa_target_value=self.spec.hpa_target_value,
              deployment_name=self.deployment_metadata['metadata']['name'],
              hpa_stabilization_window_seconds=self.spec.deployment_timeout
              + 120,
          )
      )
      self.created_resources.extend(created_resources)
      for hpa in filter(
          lambda it: str(it).startswith('horizontalpodautoscaler.autoscaling'),
          created_resources,
      ):
        self.cluster.WaitForResource(f'{hpa}', 'ScalingActive', timeout=60)

      if not self._hpa_poller_executor:
        self._hpa_poller_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=2, thread_name_prefix='hpa-poller-worker'
        )
        self._hpa_polling_stop_event = threading.Event()
        self._hpa_poller_executor.submit(
            self._PollForHPAEvents,
            self.replica,
            self._hpa_polling_stop_event,
            on_hpa_scale_event,
        )
      else:
        raise errors.Resource.CreationError(
            'Cannot enable multiple HPA for the same inference server.'
        )
      self._hpa_enabled = True

    except errors.VmUtil.IssueCommandTimeoutError as e:
      logging.exception(
          'Timeout when enabling HPA, it may be caused by the custom metric not'
          ' being available. Please check the log and your metrics for more'
          ' details.'
      )
      raise e

  def _Create(self) -> None:
    """Creates the Default Kubernetes Inference Server."""
    if self.is_remote:
      logging.info(
          'PKB is using a remote deployment %s and service %s. K8s operations'
          ' like HPA and pod monitoring will be skipped.',
          self.deployment_metadata,
          self.service_name,
      )
      return

    if self.deployment_metadata and self.service_name:
      logging.info(
          'PKB is using a pre-existing deployment %s and service %s.',
          self.deployment_metadata,
          self.service_name,
      )
      self._ParseInferenceServerDeploymentMetadata()
      return

    if 'gcsfuse' in self.spec.catalog_components:
      self._ApplyGCSFusePVC()

    self._ProvisionGPUNodePool()

    logging.info('Creating new inference server')
    self._InjectDefaultHuggingfaceToken()

    # Define Safeguard policy error patterns
    safeguard_policy_errors = [
        'admission webhook "validation.gatekeeper.sh" denied',
        'azurepolicy-k8sazurev',
    ]

    # Apply the manifest with retry logic,
    # waiting for the Safeguard policy relaxation to take effect.
    @vm_util.Retry(
        poll_interval=30, retryable_exceptions=errors.Resource.CreationError
    )
    def _apply_manifest_with_retry():
      """Apply inference server manifest with retry logic."""
      logging.info('Applying inference server manifest')
      try:
        return self._GetInferenceServerManifest()
      except Exception as e:
        # Only retry Safeguard policy errors, fail fast on others
        if any(pattern in str(e) for pattern in safeguard_policy_errors):
          raise errors.Resource.CreationError(str(e))
        raise

    inference_server_manifest = _apply_manifest_with_retry()

    self._ParseAndStoreInferenceServerDetails(inference_server_manifest)
    with vm_util.NamedTemporaryFile(
        mode='w', prefix='inference-server-manifest', suffix='.yaml'
    ) as f:
      f.write(inference_server_manifest)
      f.flush()
      created_resources = list(
          self.cluster.ApplyManifest(f.name, should_log_file=False)
      )
      self.created_resources.extend(created_resources)

    deployment_name = self.deployment_metadata['metadata']['name']
    try:
      self.cluster.WaitForResource(
          f'deployment/{deployment_name}',
          'available',
          timeout=self.spec.deployment_timeout,
      )
    except errors.VmUtil.IssueCommandError as e:
      pods = self.cluster.GetResourceMetadataByName(
          'pods',
          f'app={self.app_selector}',
          output_format='name',
          output_formatter=lambda res: res.splitlines(),
      )
      events = self.cluster.GetEvents()
      quota_failure = False
      for pod in pods:
        pod_name = pod.split('/')[1]
        status_cmd = [
            'get',
            'pod',
            pod.split('/')[1],
            '-o',
            'jsonpath={.status.phase}',
        ]
        status, _, _ = container_service.RunKubectlCommand(status_cmd)
        if 'Pending' not in status:
          continue
        for event in events:
          if (
              event.resource.kind == 'Pod'
              and event.resource.name == pod_name
              and 'GCE out of resources' in event.message
          ):
            quota_failure = True
            break
      if 'timed out waiting for the condition' in str(e) and quota_failure:
        raise errors.Benchmarks.QuotaFailure(
            f'TIMED OUT: Deployment {deployment_name} did not become available'
            f' within {self.spec.deployment_timeout} seconds. This can be due'
            ' to issues like resource exhaustion, but can also be due to image'
            f' pull errors, or pod scheduling problems. Original error: {e}'
        ) from e
      else:
        raise e

  def _ApplyGCSFusePVC(self):
    """Apply the PV & PVC to the environment."""
    if not FLAG_GCS_BUCKET.value:
      raise errors.Resource.CreationError(
          'GCS bucket is required to apply GCSFuse PVC.'
      )
    self.cluster.ApplyManifest(
        'container/kubernetes_ai_inference/gcsfuse_pv_pvc.yaml.j2',
        gcs_bucket=FLAG_GCS_BUCKET.value,
    )
    logging.info('Successfully applied GCSFuse PVC.')

  def _CollectStartupMonitorMetrics(self) -> dict[str, PodStartupMetrics]:
    collected_metrics_map = super()._CollectStartupMonitorMetrics()
    scale_up_times = sorted(self._hpa_scale_up_time_series, reverse=True)
    for _, metrics in collected_metrics_map.items():
      if metrics.pod_created_timestamp and scale_up_times:
        scale_up_time = next(
            (
                ts
                for ts in scale_up_times
                if ts <= metrics.pod_created_timestamp
            ),
            None,
        )
        if scale_up_time:
          metrics.metadata['scale_up_timestamp'] = scale_up_time
        else:
          # Pod created before HPA scale up, HPA is not enabled.
          metrics.metadata['hpa_enabled'] = False

    return collected_metrics_map

  def GetHPASamples(self) -> list[sample.Sample]:
    """Collect the HPA metrics of the inference server.

    Returns:
      A list of result samples collected.
    """

    scale_to_latencies = collections.defaultdict(list)
    samples = self.GetPodStartupSamples()

    for metric in samples:
      if metric.metric == 'startup_latency':
        if scale_up_time := metric.metadata.get('scale_up_timestamp'):
          scale_to_latencies[scale_up_time].append(
              metric.value + metric.timestamp - scale_up_time
          )

    metadata = self.GetResourceMetadata()
    for scale_up_time, latencies in scale_to_latencies.items():
      sample_metadata = {'pods_count': len(latencies), **metadata}
      samples.extend(
          sample.Sample(metric, value, 's', sample_metadata, scale_up_time)
          for metric, value in {
              'avg_time_from_scale_to_ready': sum(latencies) / len(latencies),
              'max_time_from_scale_to_ready': max(latencies),
              'min_time_from_scale_to_ready': min(latencies),
          }.items()
      )

    return samples

  def _Delete(self) -> None:
    """Deletes the Default Kubernetes Inference Server."""
    if self.is_remote:
      logging.info(
          'User-managed deployment %s and service %s will not be deleted.'
      )
      self.deleted = True
      return

    self.StopHPAPolling()
    for resource in reversed(self.created_resources):
      self.cluster.DeleteResource(
          resource, ignore_not_found=True, raise_on_timeout=False
      )
    self.deployment_metadata = None

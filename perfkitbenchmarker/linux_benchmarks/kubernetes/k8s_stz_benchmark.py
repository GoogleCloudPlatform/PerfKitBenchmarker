# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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
"""GKE Scale-to-Zero (STZ) vs KEDA benchmark."""

import time
from typing import Any

from absl import flags
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.azure import flags as azure_flags
from perfkitbenchmarker.providers.gcp import util
from perfkitbenchmarker.resources.container_service import kubernetes_commands


class ReplicasNotReadyError(Exception):
  """Exception raised when kubernetes replicas are not yet ready."""


BENCHMARK_NAME = 'k8s_stz'
BENCHMARK_CONFIG = """
k8s_stz:
  description: Benchmarks GKE Scale-to-Zero vs KEDA.
  container_cluster:
    cloud: GCP
    type: Kubernetes
    vm_spec:
      GCP:
        machine_type: n2-standard-4
      AWS:
        machine_type: m6i.xlarge
      Azure:
        machine_type: Standard_D4s_v5
  vm_groups:
    client_vms:
      os_type: ubuntu2204
      vm_spec:
        GCP:
          machine_type: n2-standard-4
        AWS:
          machine_type: m6i.xlarge
        Azure:
          machine_type: Standard_D4s_v5
      vm_count: 1
"""

FLAGS = flags.FLAGS

_DEPLOY_KEDA = flags.DEFINE_boolean(
    'k8s_stz_deploy_keda',
    False,
    'Whether to deploy KEDA operator instead of native HPA.',
)
_TARGET_SCALE = flags.DEFINE_integer(
    'k8s_stz_target_scale',
    50,
    'Target replica count for high scale load phase.',
)
_CAPACITY_BUFFER_TYPE = flags.DEFINE_string(
    'k8s_stz_capacity_buffer_type',
    '',
    'Type of capacity buffer to use (e.g., active, standby).',
)
_WORKLOAD_GENERATOR = flags.DEFINE_enum(
    'k8s_stz_workload_generator',
    'http',
    ['http', 'pubsub', 'kafka'],
    'Workload generator to use for scaling events.',
)


def _CreatePubSubInfra(topic_name: str, sub_name: str) -> None:
  """Creates the Pub/Sub topic and subscription."""
  create_topic_cmd = util.GcloudCommand(
      None, 'pubsub', 'topics', 'create', topic_name
  )
  create_topic_cmd.Issue()

  create_sub_cmd = util.GcloudCommand(
      None, 'pubsub', 'subscriptions', 'create', sub_name
  )
  create_sub_cmd.flags['topic'] = topic_name
  create_sub_cmd.Issue()


def _DeletePubSubInfra(topic_name: str, sub_name: str) -> None:
  """Cleans up the Pub/Sub resources."""
  del_sub_cmd = util.GcloudCommand(
      None, 'pubsub', 'subscriptions', 'delete', sub_name
  )
  del_sub_cmd.Issue(raise_on_failure=False)

  del_topic_cmd = util.GcloudCommand(
      None, 'pubsub', 'topics', 'delete', topic_name
  )
  del_topic_cmd.Issue(raise_on_failure=False)


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  """Loads the dynamic YAML configuration."""
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if _CAPACITY_BUFFER_TYPE.value and _DEPLOY_KEDA.value:
    raise errors.Setup.InvalidConfigurationError(
        'k8s_stz_capacity_buffer_type and k8s_stz_deploy_keda are mutually '
        'exclusive.'
    )
  return config


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Installs dependencies and prepares the cluster."""
  benchmark_spec.always_call_cleanup = True

  # Deploy the baseline application workload
  kubernetes_commands.ApplyManifest(
      'kubernetes_stz/http_server.yaml.j2',
      benchmark_name=BENCHMARK_NAME.replace('_', '-'),
  )

  if _CAPACITY_BUFFER_TYPE.value == 'active':
    kubernetes_commands.ApplyManifest('kubernetes_stz/active_buffer.yaml.j2')
  elif _CAPACITY_BUFFER_TYPE.value == 'standby':
    kubernetes_commands.ApplyManifest('kubernetes_stz/standby_buffer.yaml.j2')

  if _WORKLOAD_GENERATOR.value == 'pubsub':
    topic_name = f'pkb-stz-topic-{FLAGS.run_uri}'
    sub_name = f'pkb-stz-sub-{FLAGS.run_uri}'
    _CreatePubSubInfra(topic_name, sub_name)

  use_keda = _DEPLOY_KEDA.value or azure_flags.AZURE_AKS_ENABLE_KEDA.value

  if not use_keda:
    # Deploy native HPA if KEDA is not being used
    kubernetes_commands.ApplyManifest(
        'kubernetes_stz/hpa_stz.yaml.j2',
        benchmark_name=BENCHMARK_NAME.replace('_', '-'),
        target_scale=_TARGET_SCALE.value,
    )

  if use_keda:
    if not azure_flags.AZURE_AKS_ENABLE_KEDA.value:
      # Deploy OSS KEDA operator only if Managed KEDA is not in use
      kubernetes_commands.ApplyManifest('kubernetes_stz/keda_operator.yaml')
      kubernetes_commands.WaitForRollout(
          'deployment/keda-operator', namespace='keda'
      )

    # Always deploy the ScaledObject when using KEDA
    kubernetes_commands.ApplyManifest(
        'kubernetes_stz/scaled_object.yaml.j2',
        benchmark_name=BENCHMARK_NAME.replace('_', '-'),
        target_scale=_TARGET_SCALE.value,
    )


def _ParseOutput(
    output_string: str, base_metadata: dict[str, Any]
) -> list[sample.Sample]:
  """Parses output into PKB samples.

  Args:
    output_string: Raw string output from benchmark execution.
    base_metadata: Dictionary of base metadata to attach to samples.

  Returns:
    List of sample.Sample objects.
  """
  samples = []
  lines = output_string.splitlines()
  for line in lines:
    if 'sfz_latency_ms' not in line:
      continue
    parts = line.split('=')
    if len(parts) == 2:
      value = float(parts[1].strip())
      samples.append(sample.Sample('SFZ_Latency', value, 'ms', base_metadata))
  return samples


def _RunBenchmarkCommand(client_vm: Any) -> str:
  """Runs the benchmark command to measure SFZ latency."""
  topic_name = f'pkb-stz-topic-{FLAGS.run_uri}'

  if _WORKLOAD_GENERATOR.value == 'pubsub':
    # Publish messages from the client VM to trigger scaling
    publish_cmd = (
        'for i in {1..10}; do '
        f'gcloud pubsub topics publish {topic_name} --message="Load $i"; '
        'done'
    )
    client_vm.RemoteCommand(publish_cmd, ignore_failure=False)
  elif _WORKLOAD_GENERATOR.value == 'http':
    # Stub for HTTP load generation
    pass
  elif _WORKLOAD_GENERATOR.value == 'kafka':
    # Stub for Kafka load generation
    pass

  # Measurement logic: Poll kubernetes_commands until readyReplicas > 0
  start_time = time.time()

  @vm_util.Retry(
      poll_interval=2,
      timeout=300,
      retryable_exceptions=(ReplicasNotReadyError,),
  )
  def _CheckReplicas():
    cmd = [
        'get',
        'deployment',
        BENCHMARK_NAME.replace('_', '-'),
        '-o',
        'jsonpath={.status.readyReplicas}',
    ]
    stdout, _, retcode = kubernetes_commands.kubectl.RunKubectlCommand(
        cmd, raise_on_failure=False
    )
    if retcode == 0 and stdout.strip() and stdout.strip().isdigit():
      if int(stdout.strip()) > 0:
        return True
    raise ReplicasNotReadyError('No ready replicas yet.')

  _CheckReplicas()
  end_time = time.time()
  latency_ms = (end_time - start_time) * 1000.0
  return f'sfz_latency_ms={latency_ms}'


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Triggers the load, monitors execution, and parses output."""
  client_vm = benchmark_spec.vm_groups['client_vms'][0]

  # Ensure base_metadata is always passed down
  base_metadata = {
      'deploy_keda': _DEPLOY_KEDA.value,
      'target_scale': _TARGET_SCALE.value,
      'capacity_buffer': _CAPACITY_BUFFER_TYPE.value,
      'workload_generator': _WORKLOAD_GENERATOR.value,
      'azure_managed_keda': azure_flags.AZURE_AKS_ENABLE_KEDA.value,
  }

  output_string = _RunBenchmarkCommand(client_vm)

  samples = _ParseOutput(output_string, base_metadata)
  return samples


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Cleans up resources idempotently."""

  client_vms = benchmark_spec.vm_groups['client_vms']
  for vm in client_vms:
    vm.RemoteCommand('rm -rf /tmp/k8s_stz_test_data', ignore_failure=True)
    vm.RemoteCommand('pkill -f k8s_stz_daemon', ignore_failure=True)

  if _WORKLOAD_GENERATOR.value == 'pubsub':
    topic_name = f'pkb-stz-topic-{FLAGS.run_uri}'
    sub_name = f'pkb-stz-sub-{FLAGS.run_uri}'
    _DeletePubSubInfra(topic_name, sub_name)

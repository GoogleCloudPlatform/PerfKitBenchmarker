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
"""Measures agent-sandbox controller claim provisioning latency under load.

Installs the agent-sandbox controller stack (Prepare) and submits SandboxClaim
custom resources at a target QPS from the PKB host, measuring time to the claim
reporting a Ready status condition (Run). Cluster and node pools are provisioned
by PerfKitBenchmarker from BENCHMARK_CONFIG.
"""

from absl import flags

from perfkitbenchmarker import configs
from perfkitbenchmarker.linux_packages import agent_sandbox
from perfkitbenchmarker.linux_packages import agent_sandbox_loadgen
from perfkitbenchmarker.linux_packages import agent_sandbox_metrics

BENCHMARK_NAME = 'agent_sandbox'

BENCHMARK_CONFIG = """
agent_sandbox:
  description: >
    Measure agent-sandbox controller claim provisioning latency under load.
  container_cluster:
    cloud: GCP
    type: Kubernetes
    vm_count: 1
    vm_spec:
      GCP:
        machine_type: c4-standard-4
        zone: us-central1-a
      AWS:
        machine_type: m8i.xlarge
        zone: us-east-1a
    nodepools:
      sandbox:
        vm_count: 4
        vm_spec:
          GCP:
            machine_type: c4-standard-16
            zone: us-central1-a
          AWS:
            machine_type: m8i.4xlarge
            zone: us-east-1a
        node_labels:
          sandbox.gke.io/runtime: runsc
        node_taints:
          - sandbox.gke.io/runtime=runsc:NoSchedule
"""

# Load shape.
_QPS = flags.DEFINE_float(
    'agent_sandbox_qps',
    10.0,
    'Target SandboxClaim submission rate (claims per second).',
)
_DURATION = flags.DEFINE_float(
    'agent_sandbox_duration',
    60.0,
    'Submission window in seconds. Combined with qps to derive total.',
)
_TOTAL = flags.DEFINE_integer(
    'agent_sandbox_total',
    None,
    'Total claims to submit. If set, overrides duration derivation.',
)
_MAX_CONCURRENT = flags.DEFINE_integer(
    'agent_sandbox_max_concurrent', 200, 'Maximum in-flight claims.'
)
_READY_TIMEOUT = flags.DEFINE_integer(
    'agent_sandbox_ready_timeout', 180, 'Per-claim ready timeout in seconds.'
)
_WORKLOAD_DURATION = flags.DEFINE_integer(
    'agent_sandbox_workload_duration',
    0,
    'Seconds to run a CPU busy-loop inside each sandbox after it becomes '
    'Ready, before releasing it. 0 (default) releases immediately (pure '
    'provisioning benchmark). >0 holds the sandbox, so peak_concurrency '
    'reflects simultaneously-alive sandboxes and exec_duration_s / '
    'total_lifecycle_s metrics are emitted.',
)
_CLAIM_TTL = flags.DEFINE_integer(
    'agent_sandbox_claim_ttl_seconds',
    120,
    'Server-side TTL (ttlSecondsAfterFinished) set on each SandboxClaim so the'
    ' controller auto-deletes orphaned claims if the driver is hard-killed.'
    ' 0 disables the TTL.',
)
# Sandbox stack.
_NAMESPACE = flags.DEFINE_string(
    'agent_sandbox_namespace',
    'default',
    'Namespace in which SandboxClaims are created.',
)
_TEMPLATE = flags.DEFINE_string(
    'agent_sandbox_template', 'python-runtime-template', 'SandboxTemplate name.'
)
_RUNTIME_CLASS = flags.DEFINE_string(
    'agent_sandbox_runtime_class', 'runsc', 'RuntimeClass for sandbox pods.'
)
_WARMPOOL = flags.DEFINE_string(
    'agent_sandbox_warmpool',
    'python-sandbox-warmpool',
    'SandboxWarmPool name. Empty disables the warm pool reference.',
)
_WARMPOOL_REPLICAS = flags.DEFINE_integer(
    'agent_sandbox_warmpool_replicas',
    0,
    'SandboxWarmPool size to provision in Prepare.',
)
_CONTROLLER_REF = flags.DEFINE_string(
    'agent_sandbox_controller_ref',
    '32c4f231a116f76eb707fe34510b8143d61268ae',
    'agent-sandbox release ref (tag or SHA) for CRD, RBAC, and controller '
    'manifests.',
)
_CONTROLLER_IMAGE = flags.DEFINE_string(
    'agent_sandbox_controller_image',
    'us-central1-docker.pkg.dev/k8s-staging-images/agent-sandbox/'
    'agent-sandbox-controller:v20260527-v0.4.6-31-gd43447b-main',
    'Controller container image.',
)

# Cloud selection uses PKB's standard global --cloud flag (GCP provisions GKE,
# AWS provisions EKS). Both run the identical controller install, load
# generator, and metrics path. The flag is read in GetConfig to select the
# per-cloud vm_spec block and is applied to the cluster spec automatically by
# ContainerClusterSpec._ApplyFlags.

# Node pool sizing.
_GENERAL_POOL_MACHINE_TYPE = flags.DEFINE_string(
    'agent_sandbox_general_pool_machine_type',
    None,
    'Machine type for the general (default) node pool. None uses the '
    'per-cloud default from the benchmark config.',
)
_GENERAL_POOL_NODES = flags.DEFINE_integer(
    'agent_sandbox_general_pool_nodes',
    1,
    'Node count for the general (default) node pool.',
)
_SANDBOX_POOL_MACHINE_TYPE = flags.DEFINE_string(
    'agent_sandbox_sandbox_pool_machine_type',
    None,
    'Machine type for the sandbox node pool. None uses the per-cloud '
    'default from the benchmark config.',
)
_SANDBOX_POOL_NODES = flags.DEFINE_integer(
    'agent_sandbox_sandbox_pool_nodes',
    4,
    'Node count for the sandbox node pool.',
)
_SANDBOX_POOL_MAX_PODS = flags.DEFINE_integer(
    'agent_sandbox_sandbox_pool_max_pods_per_node',
    None,
    'Max pods per node for the sandbox node pool. None uses the cluster'
    ' default.',
)

# Controller tuning. None means use the controller binary defaults.
_CTRL_CLAIM_WORKERS = flags.DEFINE_integer(
    'agent_sandbox_controller_claim_workers',
    None,
    'Controller --sandbox-claim-concurrent-workers value.',
)
_CTRL_SANDBOX_WORKERS = flags.DEFINE_integer(
    'agent_sandbox_controller_sandbox_workers',
    None,
    'Controller --sandbox-concurrent-workers value.',
)
_CTRL_WARMPOOL_WORKERS = flags.DEFINE_integer(
    'agent_sandbox_controller_warmpool_workers',
    None,
    'Controller --sandbox-warm-pool-concurrent-workers value.',
)
_CTRL_WARMPOOL_MAX_BATCH_SIZE = flags.DEFINE_integer(
    'agent_sandbox_controller_warmpool_max_batch_size',
    None,
    'Controller --sandbox-warm-pool-max-batch-size value.',
)
_CTRL_KUBE_API_BURST = flags.DEFINE_integer(
    'agent_sandbox_controller_kube_api_burst',
    None,
    'Controller --kube-api-burst value.',
)
_CTRL_KUBE_API_QPS = flags.DEFINE_integer(
    'agent_sandbox_controller_kube_api_qps',
    None,
    'Controller --kube-api-qps value.',
)
_CTRL_ENABLE_TRACING = flags.DEFINE_boolean(
    'agent_sandbox_controller_enable_tracing',
    False,
    'Enable controller OpenTelemetry tracing.',
)
_CTRL_OTEL_ENDPOINT = flags.DEFINE_string(
    'agent_sandbox_controller_otel_endpoint',
    None,
    'OTLP exporter endpoint when tracing is enabled.',
)
_CTRL_LEADER_ELECT = flags.DEFINE_boolean(
    'agent_sandbox_controller_leader_elect',
    False,
    'Whether the controller runs with leader election enabled.',
)

FLAGS = flags.FLAGS


def GetConfig(user_config):
  """Loads the benchmark config and merges user overrides."""
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  cluster = config['container_cluster']
  cluster['cloud'] = FLAGS.cloud
  cloud = cluster['cloud']

  # Wire general (default) pool sizing. Machine type falls back to the
  # per-cloud config default when the flag is unset.
  cluster['vm_count'] = _GENERAL_POOL_NODES.value
  if _GENERAL_POOL_MACHINE_TYPE.value is not None:
    cluster['vm_spec'][cloud]['machine_type'] = _GENERAL_POOL_MACHINE_TYPE.value
  # ContainerClusterSpec does not accept max_pods_per_node at the top level;
  # the key is only valid on NodepoolSpec. General-pool max-pods is therefore
  # not supported via this flag path and is silently ignored here.

  # Wire sandbox pool sizing.
  sandbox = cluster['nodepools']['sandbox']
  sandbox['vm_count'] = _SANDBOX_POOL_NODES.value
  if _SANDBOX_POOL_MACHINE_TYPE.value is not None:
    sandbox['vm_spec'][cloud]['machine_type'] = _SANDBOX_POOL_MACHINE_TYPE.value
  if _SANDBOX_POOL_MAX_PODS.value is not None:
    sandbox['max_pods_per_node'] = _SANDBOX_POOL_MAX_PODS.value

  return config


def Prepare(benchmark_spec):
  """Installs the agent-sandbox controller stack on the provisioned cluster."""
  del benchmark_spec  # Cluster is provisioned by PKB; install via kubectl.
  tuning: dict[str, object] = {
      'enable_tracing': _CTRL_ENABLE_TRACING.value,
      'leader_elect': _CTRL_LEADER_ELECT.value,
  }
  for key, flag in (
      ('claim_workers', _CTRL_CLAIM_WORKERS),
      ('sandbox_workers', _CTRL_SANDBOX_WORKERS),
      ('warmpool_workers', _CTRL_WARMPOOL_WORKERS),
      ('warmpool_max_batch_size', _CTRL_WARMPOOL_MAX_BATCH_SIZE),
      ('kube_api_burst', _CTRL_KUBE_API_BURST),
      ('kube_api_qps', _CTRL_KUBE_API_QPS),
      ('otel_endpoint', _CTRL_OTEL_ENDPOINT),
  ):
    if flag.value is not None:
      tuning[key] = flag.value
  agent_sandbox.install_stack(
      controller_ref=_CONTROLLER_REF.value,
      controller_image=_CONTROLLER_IMAGE.value,
      runtime_class=_RUNTIME_CLASS.value,
      template_name=_TEMPLATE.value,
      warmpool_name=_WARMPOOL.value,
      warmpool_replicas=_WARMPOOL_REPLICAS.value,
      controller_tuning=tuning,
  )


def Run(benchmark_spec):
  """Submits claims at the target rate and returns latency samples."""
  shape = agent_sandbox_loadgen.resolve_run_shape(
      qps=_QPS.value,
      duration=_DURATION.value if _TOTAL.value is None else None,
      total=_TOTAL.value,
  )
  driver = agent_sandbox_loadgen.ClaimDriver(
      namespace=_NAMESPACE.value,
      template_name=_TEMPLATE.value,
      warmpool_name=_WARMPOOL.value or None,
      claim_ttl_seconds=_CLAIM_TTL.value,
      max_concurrent=_MAX_CONCURRENT.value,
  )
  workload_duration = _WORKLOAD_DURATION.value
  executor = None
  if workload_duration > 0:
    exec_pool_size = _MAX_CONCURRENT.value + 50
    executor = agent_sandbox_loadgen.StreamExecExecutor(
        core_v1_api=agent_sandbox_loadgen._make_core_v1_api(exec_pool_size),
        custom_objects_api=agent_sandbox_loadgen._make_custom_objects_api(
            exec_pool_size
        ),
        namespace=_NAMESPACE.value,
        workload_duration=workload_duration,
    )
  generator = agent_sandbox_loadgen.LoadGenerator(
      driver,
      ready_timeout=_READY_TIMEOUT.value,
      max_concurrent=_MAX_CONCURRENT.value,
      workload_executor=executor,
      workload_duration=workload_duration,
  )
  records = generator.run(shape)

  metadata = {
      'target_qps': shape.qps,
      'duration': shape.duration,
      'total_claims': shape.total,
      'warmpool_replicas': _WARMPOOL_REPLICAS.value,
      'template': _TEMPLATE.value,
      'runtime_class': _RUNTIME_CLASS.value,
  }
  cluster = getattr(benchmark_spec, 'container_cluster', None)
  if cluster is not None:
    metadata.update(cluster.GetResourceMetadata())
  return agent_sandbox_metrics.build_samples(
      records, generator.peak_concurrency, metadata
  )


def Cleanup(benchmark_spec):
  """Best-effort deletion of any SandboxClaims left in the namespace."""
  del benchmark_spec
  from perfkitbenchmarker.resources.container_service import kubectl

  kubectl.RunKubectlCommand(
      [
          'delete',
          'sandboxclaims',
          '--all',
          '-n',
          _NAMESPACE.value,
          '--ignore-not-found',
      ],
      raise_on_failure=False,
  )

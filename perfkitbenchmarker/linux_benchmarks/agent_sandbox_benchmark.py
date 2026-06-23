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
"""Benchmarks agent sandbox SandboxClaim provisioning latency.

Submits SandboxClaim custom resources at a target QPS against the agent
sandbox installed on the cluster (via benchmark_spec.agent_sandbox) and
reports claim-to-Ready latency percentiles, throughput, and peak concurrency.
"""

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import stages
from perfkitbenchmarker.linux_benchmarks import agent_sandbox_loadgen
from perfkitbenchmarker.linux_benchmarks import agent_sandbox_metrics
from perfkitbenchmarker.resources.kubernetes import k8s_agent_sandbox

FLAGS = flags.FLAGS

_QPS = flags.DEFINE_float(
    'agent_sandbox_qps', 10.0,
    'Target SandboxClaim submission rate (claims per second).')
_DURATION = flags.DEFINE_float(
    'agent_sandbox_duration', 60.0,
    'Submission window in seconds. Combined with qps to derive total.')
_TOTAL = flags.DEFINE_integer(
    'agent_sandbox_total', None,
    'Total claims to submit. If set, overrides duration derivation.')
_MAX_CONCURRENT = flags.DEFINE_integer(
    'agent_sandbox_max_concurrent', 200, 'Maximum in-flight claims.')
_READY_TIMEOUT = flags.DEFINE_integer(
    'agent_sandbox_ready_timeout', 180, 'Per-claim ready timeout in seconds.')
_WORKLOAD_DURATION = flags.DEFINE_integer(
    'agent_sandbox_workload_duration', 0,
    'Seconds to run a CPU busy-loop inside each sandbox after it becomes '
    'Ready, before releasing it. 0 (default) releases immediately (pure '
    'provisioning benchmark). >0 holds the sandbox so peak_concurrency '
    'reflects simultaneously-alive sandboxes and exec/lifecycle metrics emit.')
_CLAIM_TTL = flags.DEFINE_integer(
    'agent_sandbox_claim_ttl_seconds', 120,
    'Server-side TTL set on each SandboxClaim so the controller auto-deletes '
    'orphaned claims if the driver is hard-killed. 0 disables the TTL.')

BENCHMARK_NAME = 'agent_sandbox'
BENCHMARK_CONFIG = """
agent_sandbox:
  description: >
    Submit SandboxClaims at a target QPS and measure provisioning latency on
    the agent-sandbox stack.
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
        node_taints:
          - sandbox.gke.io/runtime=runsc:NoSchedule
  agent_sandbox:
    type: Kubernetes
"""


def GetConfig(user_config):
  """Loads the benchmark config and merges user overrides."""
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  config['container_cluster']['cloud'] = FLAGS.cloud
  return config


def Prepare(benchmark_spec):
  """Installs the controller, sandbox template, and warm pool.

  CRDs, RBAC, and gVisor are installed during provision. The controller stack
  is installed here so it can be re-applied against an existing cluster with
  `--run_stage=prepare` to iterate on controller settings without recreating
  the cluster. On a prepare-stage resume the benchmark spec was pickled at
  provision, so the config is rebuilt from the current flags; re-run with your
  full flag set plus whatever you are changing.
  """
  sandbox = benchmark_spec.agent_sandbox
  if sandbox is None:
    raise errors.Benchmarks.PrepareException(
        'agent_sandbox must be configured for this benchmark')
  if stages.PROVISION not in FLAGS.run_stage:
    sandbox.RefreshSpecFromFlags()
  sandbox.InstallWorkload()


def Run(benchmark_spec):
  """Submits claims at the target rate and returns latency samples."""
  spec = benchmark_spec.agent_sandbox.spec
  shape = agent_sandbox_loadgen.resolve_run_shape(
      qps=_QPS.value,
      duration=_DURATION.value if _TOTAL.value is None else None,
      total=_TOTAL.value)
  driver = agent_sandbox_loadgen.ClaimDriver(
      namespace=spec.namespace,
      template_name=k8s_agent_sandbox.SANDBOX_NAME,
      warmpool_name=k8s_agent_sandbox.SANDBOX_NAME,
      claim_ttl_seconds=_CLAIM_TTL.value,
      max_concurrent=_MAX_CONCURRENT.value)
  workload_duration = _WORKLOAD_DURATION.value
  executor = None
  if workload_duration > 0:
    pool_size = _MAX_CONCURRENT.value + 50
    executor = agent_sandbox_loadgen.StreamExecExecutor(
        core_v1_api=agent_sandbox_loadgen._make_core_v1_api(pool_size),
        custom_objects_api=agent_sandbox_loadgen._make_custom_objects_api(
            pool_size),
        namespace=spec.namespace,
        workload_duration=workload_duration)
  generator = agent_sandbox_loadgen.LoadGenerator(
      driver, ready_timeout=_READY_TIMEOUT.value,
      max_concurrent=_MAX_CONCURRENT.value,
      workload_executor=executor, workload_duration=workload_duration)
  records = generator.run(shape)
  metadata = {
      'target_qps': shape.qps,
      'duration': shape.duration,
      'total_claims': shape.total,
      'warmpool_replicas': spec.sandbox_warmpool.replicas,
      'runtime_class': spec.sandbox_template.runtime_class,
  }
  metadata.update(benchmark_spec.container_cluster.GetResourceMetadata())
  return agent_sandbox_metrics.build_samples(
      records, generator.peak_concurrency, metadata)


def Cleanup(benchmark_spec):
  """No-op: cluster teardown reclaims the agent sandbox stack."""
  del benchmark_spec

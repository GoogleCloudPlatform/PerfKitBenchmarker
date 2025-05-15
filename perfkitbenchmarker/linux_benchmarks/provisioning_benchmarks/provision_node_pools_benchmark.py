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
"""Measures the performance of node pool provisioning.

The test creates two batches of jobs, each job running on a single node in a
separate node pool. Second batch is created after the first batch is running.
The test measures the time it takes to provision both batches,
from the moment jobs are created to the moment they are all running.

Batch default sizes:
- First batch (init-batch): 0->100
- Second batch (test-batch): 100->120

The test doesn't compensate the time for master VM resizes on GKE. For default
settings master VM resize should be triggered twice when waiting for the first
batch of 100 jobs to be running (go/gke-kcp-resize).

Node pool creation is triggered by workload separation based on node selectors
and tolerations. This strategy works on GKE clusters with NAP enabled. Details:
https://cloud.google.com/kubernetes-engine/docs/how-to/workload-separation#separate-workloads-autopilot

Potential optimization (deliberately omitted) for GKE:
- Use smaller machine types using Custom Compute Classes
- Resize master VMs before running tests
"""
import logging
import time
from typing import List
from absl import flags
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import container_service
from perfkitbenchmarker import sample

INIT_BATCH_SIZE = flags.DEFINE_integer(
    "provision_node_pools_init_batch",
    100,
    "Number of node pools to create in the initial batch",
)
TEST_BATCH_SIZE = flags.DEFINE_integer(
    "provision_node_pools_test_batch",
    20,
    "Number of node pools to create in the test batch after the initial batch",
)
USE_GPU = flags.DEFINE_boolean(
    "provision_node_pools_with_gpu",
    False,
    "Whether the node pools should use GPUs. Requires setting flag"
    "with GKE gpu limit --gke_max_accelerator='type=nvidia-tesla-t4,count=120'",
)

BENCHMARK_NAME = "provision_node_pools"
BENCHMARK_CONFIG = """
provision_node_pools:
  description: Measure the performance of node pools auto provisioning.
  container_cluster:
    cloud: GCP
    type: Kubernetes
    vm_spec: *default_dual_core
  flags:
    # Below flags are required to enable node autoprovisioning on GKE standard clusters
    # (100 init_batch + 20 test_batch + 10 buffer) * 4 cores
    gke_max_cpu: 520
    # (100 init_batch + 20 test_batch + 10 buffer) * 16 GB
    gke_max_memory: 2080
"""

INIT_BATCH_NAME = "init-batch"
TEST_BATCH_NAME = "test-batch"
JOB_MANIFEST_TEMPLATE = "provision_node_pools/job_manifest.yaml.j2"


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(_: bm_spec.BenchmarkSpec) -> None:
  pass


def _CreateJobsAndWait(
    cluster: container_service.KubernetesCluster, batch_name: str, jobs: int
) -> None:
  """Creates and waits for jobs to be running."""
  logging.info(
      "Creating batch '%s' of %d jobs, each job running in a separate node in a"
      " separate node pools",
      batch_name,
      jobs,
  )
  apply_start = time.monotonic()
  for i in range(jobs):
    cluster.ApplyManifest(
        JOB_MANIFEST_TEMPLATE,
        batch=batch_name,
        gpu=USE_GPU.value,
        cloud=cluster.CLOUD,
        id="{:03d}".format(i + 1),
    )
  logging.info(
      "Created %d jobs in batch '%s' in %d seconds. Waiting for all jobs to be"
      " running",
      jobs,
      batch_name,
      time.monotonic() - apply_start,
  )
  start = time.monotonic()
  container_service.RunKubectlCommand(
      command=[
          "wait",
          "pod",
          "-l",
          "batch=%s" % batch_name,
          "--for",
          "jsonpath={.status.phase}=Running",
          # wait up to 2 min per node pool + 45 min for master resizes
          # synchronous NAP in GKE takes ~1 min per node pool
          "--timeout",
          "%ds" % ((jobs * 2 + 45) * 60),
      ],
      timeout=None,
  )
  logging.info(
      "All %d jobs in batch '%s' are running. Wait time: %d seconds.",
      jobs,
      batch_name,
      time.monotonic() - start,
  )


def _AssertNodes(
    cluster: container_service.KubernetesCluster,
    expected_nodes: int,
) -> None:
  """Asserts expected number of nodes in the cluster."""
  nodes = len(cluster.GetNodeNames())
  allowed_diff = 1
  if abs(nodes - expected_nodes) > allowed_diff:
    raise ValueError(
        "Cluster has %d nodes, but expected %d (+/- %d)"
        % (nodes, expected_nodes, allowed_diff)
    )
  logging.info("Cluster has %d nodes", nodes)


def _AssertNodePools(
    cluster: container_service.KubernetesCluster,
    expected_node_pools: int,
) -> None:
  """Asserts expected number of node pools in the cluster."""
  node_pools = len(cluster.GetNodePoolNames())
  allowed_diff = 1
  if abs(node_pools - expected_node_pools) > allowed_diff:
    raise ValueError(
        "Cluster has %d node pools, but expected %d (+/- %d)"
        % (node_pools, expected_node_pools, allowed_diff)
    )
  logging.info("Cluster has %d node pools", node_pools)


def _CreateNodePools(
    cluster: container_service.KubernetesCluster,
    batch_name: str,
    node_pools_to_add: int,
) -> List[sample.Sample]:
  """Creates node pools and measures the time it takes to provision them."""
  nodes_before = len(cluster.GetNodeNames())
  nodes_pools_before = len(cluster.GetNodePoolNames())
  start = time.monotonic()
  _CreateJobsAndWait(cluster, batch_name, node_pools_to_add)
  elapsed = time.monotonic() - start
  _AssertNodes(cluster, nodes_before + node_pools_to_add)
  _AssertNodePools(cluster, nodes_pools_before + node_pools_to_add)
  metadata = {"node_pools_created": node_pools_to_add}
  metric_batch_name = batch_name.replace("-", "_")
  return [
      sample.Sample(
          "%s_provisioning_time" % metric_batch_name,
          elapsed,
          "seconds",
          metadata,
      ),
      sample.Sample(
          "%s_provisioning_time_per_node_pool" % metric_batch_name,
          elapsed / node_pools_to_add,
          "seconds",
          metadata,
      ),
  ]


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Runs the node pools provisioning benchmark."""
  cluster = benchmark_spec.container_cluster
  samples = []
  start = time.monotonic()
  if INIT_BATCH_SIZE.value > 0:
    samples += _CreateNodePools(cluster, INIT_BATCH_NAME, INIT_BATCH_SIZE.value)
  samples += _CreateNodePools(cluster, TEST_BATCH_NAME, TEST_BATCH_SIZE.value)
  elapsed = time.monotonic() - start
  total_node_pools = INIT_BATCH_SIZE.value + TEST_BATCH_SIZE.value
  metadata = {
      "node_pools_init_batch": INIT_BATCH_SIZE.value,
      "node_pools_test_batch": TEST_BATCH_SIZE.value,
      "node_pools_total": total_node_pools,
  }
  samples += [sample.Sample("total_time", elapsed, "seconds", metadata)]
  samples += [
      sample.Sample(
          "total_time_per_node_pool",
          elapsed / total_node_pools,
          "seconds",
          metadata,
      ),
  ]
  return samples


def Cleanup(_) -> None:
  pass

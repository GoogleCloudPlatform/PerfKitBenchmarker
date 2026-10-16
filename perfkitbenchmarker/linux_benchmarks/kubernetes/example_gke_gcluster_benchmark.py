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
"""Simple example of GKE clusters provisioned by gcluster.

This example shows how to use GKE clusters provisioned by gcluster
(https://github.com/GoogleCloudPlatform/cluster-toolkit). This is useful for
"complex" GKE cluster architectures, particularly for AI/ML which require
specific nodepool and networking configurations that are difficult to model with
vanilla PKB.

When run, this benchmark:
1. Uses gcluster to build the cluster defined in
data/cluster/gke/gke_default.yaml.j2. gcluster manages the lifecycle
of the cluster, nodepools, networking, IAM, etc.
2. Requests and configures the kubeconfig
3. Runs kubectl get nodes to verify cluster reachability.
4. Tears down the cluster
"""

import logging
from typing import Any
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker.resources.container_service import kubectl

BENCHMARK_NAME = 'example_gke_gcluster'
BENCHMARK_CONFIG = """
example_gke_gcluster:
  description: Example demonstrating how to use GKE clusters provisioned by gcluster.
  vm_groups: {}
  container_cluster:
    cloud: GCP
    type: gcluster
    vm_count: 1
    vm_spec:
      GCP:
        zone: us-central1-a
        # region: us-central1
"""


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  """Loads and returns the benchmark config."""
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(_: bm_spec.BenchmarkSpec) -> None:
  """Prepares the container cluster."""
  # PKB's Provision phase automatically handles the cluster creation before this
  # is called.
  pass


def Run(_: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Run a simple command against the cluster."""
  logging.info('Running kubectl get nodes to verify cluster reachability...')

  # The cluster is fully provisioned by the time Run() is called.
  # We use the built-in RunKubectlCommand helper which runs locally.
  stdout, stderr, retcode = kubectl.RunKubectlCommand(['get', 'nodes'])

  if retcode != 0:
    raise RuntimeError(
        'Cluster test failed during kubectl get nodes:'
        f'\\nSTDOUT: {stdout}\\nSTDERR: {stderr}'
    )

  logging.info('Cluster is reachable! Node list:\\n%s', stdout)

  return [
      sample.Sample('Kubectl Get Nodes Success', 1, 'count', {'stdout': stdout})
  ]


def Cleanup(_: bm_spec.BenchmarkSpec) -> None:
  """Cleans up resources."""
  # PKB's Teardown phase automatically deletes the cluster after this is called.
  pass

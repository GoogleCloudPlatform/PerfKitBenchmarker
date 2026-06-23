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
"""Stub benchmark that provisions the Kubernetes agent sandbox resource.

The agent sandbox is installed via the top-level agent_sandbox config block,
wired through benchmark_spec.ConstructAgentSandbox. The load generator and
metrics land in a follow-up change; Run currently returns no samples.
"""

from absl import flags
from perfkitbenchmarker import configs
# Imported so the K8sAgentSandbox resource and its config spec register.
from perfkitbenchmarker.resources.kubernetes import k8s_agent_sandbox  # pylint: disable=unused-import

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'agent_sandbox'
BENCHMARK_CONFIG = """
agent_sandbox:
  description: >
    Provision the agent-sandbox stack on a Kubernetes cluster. Load generation
    and metrics are added in a follow-up change.
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
  agent_sandbox:
    type: Kubernetes
"""


def GetConfig(user_config):
  """Loads the benchmark config and merges user overrides."""
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  config['container_cluster']['cloud'] = FLAGS.cloud
  return config


def Prepare(benchmark_spec):
  """No-op: the agent sandbox is provisioned via benchmark_spec."""
  del benchmark_spec


def Run(benchmark_spec):
  """Returns no samples yet. Load generation lands in a follow-up change."""
  del benchmark_spec
  return []  # TODO(followup): run the load generator and return samples.


def Cleanup(benchmark_spec):
  """No-op: cluster teardown reclaims the agent sandbox stack."""
  del benchmark_spec

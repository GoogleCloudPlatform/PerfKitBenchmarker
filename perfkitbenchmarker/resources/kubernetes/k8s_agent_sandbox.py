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
"""Kubernetes implementation of an agent sandbox.

Installs the open-source kubernetes-sigs/agent-sandbox stack (CRDs, RBAC,
controller, gVisor runtime class, SandboxTemplate, and SandboxWarmPool).
"""

from perfkitbenchmarker.resources import agent_sandbox
from perfkitbenchmarker.resources import agent_sandbox_spec


class K8sAgentSandbox(agent_sandbox.BaseAgentSandbox):
  """Installs the open-source kubernetes-sigs/agent-sandbox stack."""

  SANDBOX_TYPE = agent_sandbox_spec.DEFAULT_SANDBOX_TYPE

  def _Create(self):
    """Installs the OSS agent sandbox stack onto the cluster."""
    raise NotImplementedError(
        'OSS agent sandbox install is not yet implemented.'
    )

  def _Delete(self):
    """Removes the OSS agent sandbox stack from the cluster."""
    raise NotImplementedError(
        'OSS agent sandbox teardown is not yet implemented.'
    )

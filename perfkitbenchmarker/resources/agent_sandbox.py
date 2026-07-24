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
"""Base class for an agent sandbox resource installed on a Kubernetes cluster.

An agent sandbox is a stack (CRDs, controller, runtime class, warm pools)
installed onto a Kubernetes cluster. Concrete subclasses provide the install
logic, for example the open-source kubernetes-sigs/agent-sandbox stack or a
cloud-managed offering. This benchmark is loosely modeled on the kubernetes
inference server pattern and is attached to a cluster as cluster.agent_sandbox.
"""

from __future__ import annotations

from typing import Optional

from perfkitbenchmarker import errors
from perfkitbenchmarker import resource as pkb_resource


class BaseAgentSandbox(pkb_resource.BaseResource):
  """Base class for an agent sandbox resource."""

  RESOURCE_TYPE = 'BaseAgentSandbox'
  REQUIRED_ATTRS = ['SANDBOX_TYPE']

  def __init__(self, spec, cluster):
    super().__init__()
    self.spec = spec
    self.cluster = cluster
    if self.cluster is None:
      raise errors.Resource.CreationError(
          'A kubernetes cluster is required for an agent sandbox resource.'
      )

  def InstallWorkload(self):
    """Installs the controller and workload components."""
    raise NotImplementedError()


def GetAgentSandbox(spec, cluster) -> Optional[BaseAgentSandbox]:
  """Returns an agent sandbox resource for the given spec, or None."""
  if not spec:
    return None
  agent_sandbox_class: type[BaseAgentSandbox] = pkb_resource.GetResourceClass(
      BaseAgentSandbox, SANDBOX_TYPE=spec.type
  )
  return agent_sandbox_class(spec, cluster)

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

This module models the configuration surface for the open-source
kubernetes-sigs/agent-sandbox stack. The on-cluster installation orchestration
(gVisor, controller, sandbox template, and warm pool) is implemented in the
follow-up benchmark change.
"""

from absl import logging

from perfkitbenchmarker.resources import agent_sandbox
from perfkitbenchmarker.resources import agent_sandbox_spec
from perfkitbenchmarker.resources.kubernetes import k8s_agent_sandbox_spec  # pylint: disable=unused-import


class K8sAgentSandbox(agent_sandbox.BaseAgentSandbox):
  """Models the configuration surface for the kubernetes-sigs/agent-sandbox stack."""

  SANDBOX_TYPE = agent_sandbox_spec.DEFAULT_SANDBOX_TYPE

  def _Create(self) -> None:
    """Registers the agent sandbox; on-cluster installation is deferred.

    This resource currently models only the configuration surface. The
    on-cluster installation orchestration (gVisor, controller, sandbox
    template, and warm pool) and its manifests are implemented in the
    follow-up benchmark change.
    """
    logging.info(
        'Agent sandbox configuration registered; on-cluster installation is '
        'deferred to a follow-up change.'
    )

  def _Delete(self):
    """No-op: the ephemeral cluster teardown reclaims the sandbox stack."""
    pass

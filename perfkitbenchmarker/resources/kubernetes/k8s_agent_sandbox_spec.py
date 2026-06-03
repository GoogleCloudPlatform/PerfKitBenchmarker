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
"""Spec for the Kubernetes agent sandbox."""

from perfkitbenchmarker.resources import agent_sandbox_spec


class K8sAgentSandboxConfigSpec(agent_sandbox_spec.BaseAgentSandboxConfigSpec):
  """Config spec for the Kubernetes agent sandbox.

  Skeleton only. Stack and controller-tuning options are added in a follow-up.
  """

  SANDBOX_TYPE = agent_sandbox_spec.DEFAULT_SANDBOX_TYPE

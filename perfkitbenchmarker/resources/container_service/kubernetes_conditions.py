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
"""Classes related to Kubernetes resource status conditions."""

from collections import abc
import dataclasses
import json
import logging
from typing import Any
from dateutil import parser
from perfkitbenchmarker.resources.container_service import kubectl


def ConvertToEpochTime(timestamp: str) -> int:
  """Converts a timestamp to epoch time."""
  # Example: 2024-11-08T23:44:36Z
  return int(parser.parse(timestamp).timestamp())


@dataclasses.dataclass
class KubernetesStatusCondition:
  """Stores the information of a Kubernetes resource status condition."""

  resource_type: str
  resource_name: str
  epoch_time: int
  event: str

  @classmethod
  def FromJsonPathResult(
      cls, resource_type: str, resource_name: str, condition: dict[str, Any]
  ) -> 'KubernetesStatusCondition':
    """Parses the json result of kubectl get."""
    str_time = condition['lastTransitionTime']
    return cls(
        resource_type,
        resource_name,
        epoch_time=ConvertToEpochTime(str_time),
        event=condition['type'],
    )

  @classmethod
  def IsValid(cls, condition: dict[str, Any]) -> bool:
    """Returns true if the resource condition is valid."""
    return (
        'lastTransitionTime' in condition
        and condition['lastTransitionTime']
        and 'type' in condition
        and condition['type']
    )


def GetStatusConditionsForResourceType(
    resource_type: str,
    resources_to_ignore: abc.Set[str] = frozenset(),
    suppress_logging: bool = False,
) -> list[KubernetesStatusCondition]:
  """Returns the status conditions for a resource type.

  Args:
    resource_type: The type of the resource to get the status conditions for.
    resources_to_ignore: A set of resource names to ignore.
    suppress_logging: Whether to suppress logging.

  Returns:
    A list of status condition.
  """
  # Use full JSON output to avoid invalid JSON when manually building from
  # jsonpath with many resources or on connection reset (truncated output).
  # Avoid logging huge JSON: kubernetes_scale uses num_replicas;
  # kubernetes_node_scale uses kubernetes_scale_num_nodes for the
  # same code path (get pod/node -o json).
  stdout, _, _ = kubectl.RunKubectlCommand(
      ['get', resource_type, '-o', 'json'],
      timeout=60 * 5,  # 5 minutes for large clusters (e.g. 1000 pods)
      suppress_logging=suppress_logging,
  )
  data = json.loads(stdout)
  name_to_conditions = {}
  for item in data.get('items', []):
    name = item.get('metadata', {}).get('name')
    conditions = item.get('status', {}).get('conditions')
    if name is not None and conditions is not None:
      name_to_conditions[name] = conditions

  for key in resources_to_ignore:
    name_to_conditions.pop(key, None)

  results = []
  failures = []
  for name in name_to_conditions:
    for conditions in name_to_conditions[name]:
      if not KubernetesStatusCondition.IsValid(conditions):
        failures.append(conditions)
        continue
      results.append(
          KubernetesStatusCondition.FromJsonPathResult(
              resource_type, name, conditions
          )
      )

  if failures:
    unique_failures = set(frozenset(f.items()) for f in failures)
    logging.warning(
        'Failed to parse %d K8s conditions, with %d unique failures. Printing'
        ' the first 5.',
        len(failures),
        len(unique_failures),
    )
    for failure in list(unique_failures)[:5]:
      logging.warning('Failed to parse the condition: %s', failure)

  return results

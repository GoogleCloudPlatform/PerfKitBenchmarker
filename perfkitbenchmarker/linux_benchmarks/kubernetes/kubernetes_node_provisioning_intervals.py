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
"""Pure metric core for Kubernetes node-provisioning latency.

Parses Pod and Node JSON into records, joins them by node name, and computes
four cumulative provisioning intervals anchored at `pod.created` using
API-server timestamps:
  - `node_registered_s` (`node.created - pod.created`): VM provision + kubelet
    registration latency.
  - `node_ready_s` (`node.ready - pod.created`): Time until kubelet + CNI report
    `NodeReady=True`.
  - `pod_scheduled_s` (`pod.scheduled - pod.created`): Time until the scheduler
    binds the pod to the node (on GPU nodes, this waits until
    `nvidia-device-plugin` advertises `nvidia.com/gpu` in
    `Node.status.allocatable`).
  - `pod_ready_s` (`pod.ready - pod.created`): End-to-end time until the pod's
    container is Ready on the newly provisioned node.

Doesn't use kubectl so the math is unit-testable without a cluster.
Missing prerequisites or pre-existing warm nodes yield None rather than
negative or misleading intervals.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
import dataclasses
from typing import Any, Optional

from dateutil import parser as _date_parser
from perfkitbenchmarker import sample


def parse_epoch_seconds(timestamp: str) -> float:
  """RFC3339 timestamp -> float epoch seconds."""
  return _date_parser.parse(timestamp).timestamp()


def condition_time(
    conditions: list[dict[str, Any]], cond_type: str, status: str = 'True'
) -> Optional[float]:
  """Epoch seconds of the first condition matching type+status, else None."""
  for c in conditions:
    if c.get('type') == cond_type and c.get('status') == status:
      ts = c.get('lastTransitionTime')
      if ts:
        return parse_epoch_seconds(ts)
  return None


def _created(item: dict[str, Any]) -> float:
  """Extract creation timestamp from metadata."""
  return parse_epoch_seconds(item['metadata']['creationTimestamp'])


def _conditions(item: dict[str, Any]) -> list[dict[str, Any]]:
  """Extract conditions from status, defaulting to empty list."""
  return (item.get('status') or {}).get('conditions') or []


@dataclasses.dataclass(frozen=True)
class PodRecord:
  """Pod provisioning record with relevant condition timestamps."""

  name: str
  created: float
  node_name: Optional[str]
  unschedulable: Optional[float]
  scheduled: Optional[float]
  containers_ready: Optional[float]
  ready: Optional[float]

  @classmethod
  def from_item(cls, item: dict[str, Any]) -> PodRecord:
    """Create PodRecord from Kubernetes pod JSON object."""
    c = _conditions(item)
    return cls(
        name=item['metadata']['name'],
        created=_created(item),
        node_name=(item.get('spec') or {}).get('nodeName'),
        unschedulable=condition_time(c, 'PodScheduled', status='False'),
        scheduled=condition_time(c, 'PodScheduled', status='True'),
        containers_ready=condition_time(c, 'ContainersReady'),
        ready=condition_time(c, 'Ready'),
    )


@dataclasses.dataclass(frozen=True)
class NodeRecord:
  """Node provisioning record with creation and ready timestamps."""

  name: str
  created: float
  ready: Optional[float]

  @classmethod
  def from_item(cls, item: dict[str, Any]) -> NodeRecord:
    """Create NodeRecord from Kubernetes node JSON object."""
    return cls(
        name=item['metadata']['name'],
        created=_created(item),
        ready=condition_time(_conditions(item), 'Ready'),
    )


def parse_items(
    resource_json: dict[str, Any], factory: Callable[[dict[str, Any]], Any]
) -> list[Any]:
  """Parse items from a Kubernetes resource JSON blob using the given factory."""
  return [factory(item) for item in resource_json.get('items') or []]


def _sub(a: Optional[float], b: Optional[float]) -> Optional[float]:
  """Subtract b from a, returning None if either is None."""
  if a is None or b is None:
    return None
  return a - b


@dataclasses.dataclass(frozen=True)
class NodeProvisioningIntervals:
  """Per-pod cumulative provisioning interval measurements from `pod.created`."""

  pod_name: str
  node_name: Optional[str]

  node_registered_s: Optional[float]
  node_ready_s: Optional[float]
  pod_scheduled_s: Optional[float]
  pod_ready_s: Optional[float]


def compute_intervals(
    pods: Sequence[PodRecord],
    nodes: Sequence[NodeRecord],
) -> list[NodeProvisioningIntervals]:
  """Joins pod -> node by node name and computes cumulative intervals.

  Args:
    pods: Sequence of PodRecord instances.
    nodes: Sequence of NodeRecord instances.

  Returns:
    List of NodeProvisioningIntervals. If a pod is scheduled onto a pre-existing
    warm node (`node.created < pod.created`), `node_registered_s` and
    `node_ready_s` are set to None so warm-node scheduling is never conflated
    with cold node provisioning.
  """
  nodes_by_name = {n.name: n for n in nodes}

  out: list[NodeProvisioningIntervals] = []
  for pod in pods:
    node = nodes_by_name.get(pod.node_name) if pod.node_name else None
    is_newly_provisioned_node = node is not None and node.created >= pod.created

    node_created = node.created if is_newly_provisioned_node and node else None
    node_ready = node.ready if is_newly_provisioned_node and node else None

    out.append(
        NodeProvisioningIntervals(
            pod_name=pod.name,
            node_name=pod.node_name,
            node_registered_s=_sub(node_created, pod.created),
            node_ready_s=_sub(node_ready, pod.created),
            pod_scheduled_s=_sub(pod.scheduled, pod.created),
            pod_ready_s=_sub(pod.ready, pod.created),
        )
    )
  return out


_INTERVAL_FIELDS = tuple(
    f.name
    for f in dataclasses.fields(NodeProvisioningIntervals)
    if f.name.endswith('_s')
)
_PERCENTILES = (
    sample.Percentile(50),
    sample.Percentile(90),
    sample.Percentile(99),
)


def to_samples(
    intervals: Sequence[NodeProvisioningIntervals],
    extra_metadata: dict[str, Any],
) -> list[sample.Sample]:
  """Compute percentile, count, and per-pod samples from provisioning intervals."""
  samples: list[sample.Sample] = []
  for field in _INTERVAL_FIELDS:
    values: list[float] = []
    for iv in intervals:
      v = getattr(iv, field, None)
      if v is not None:
        values.append(v)
    if not values:
      continue
    samples.append(
        sample.Sample(
            f'{field}_count', len(values), 'count', dict(extra_metadata)
        )
    )
    base_sample = sample.Sample(f'{field}_', 0, 'seconds', dict(extra_metadata))
    samples.extend(
        sample.SummarizePercentiles(values, base_sample, _PERCENTILES)
    )
    for iv in intervals:
      v = getattr(iv, field, None)
      if v is not None:
        md = dict(extra_metadata)
        md['pod_name'] = iv.pod_name
        if iv.node_name:
          md['node_name'] = iv.node_name
        samples.append(sample.Sample(field, v, 'seconds', md))
  return samples

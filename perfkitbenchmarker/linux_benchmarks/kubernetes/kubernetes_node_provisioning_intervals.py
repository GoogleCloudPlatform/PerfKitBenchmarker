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

Parses Pod, Node, and Event JSON into records, joins them by node name, and
computes cumulative intervals, waterfall sub-phase spans, deduplicated per-node
provisioning statistics, and burst-level makespan / milestone metrics.

Four core API-server-stamped intervals anchored at `pod.created`:
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

Waterfall sub-phase spans:
  - `node_boot_to_ready_s` (`node.ready - node.created`): Node bootstrap & CNI.
  - `node_ready_to_pod_scheduled_s` (`pod.scheduled - node.ready`): Scheduler
    binding and device-plugin registration lag on newly provisioned nodes.
  - `pod_scheduled_to_ready_s` (`pod.ready - pod.scheduled`): Image pull and
    container startup latency after scheduling.

Doesn't use kubectl directly so all math is unit-testable without a cluster.
Missing prerequisites or pre-existing warm nodes yield None rather than
negative or misleading intervals.
"""

from __future__ import annotations

import collections
from collections.abc import Callable, Sequence
import dataclasses
import logging
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
  scale_up_triggered: Optional[float] = None
  image_pulling: Optional[float] = None
  image_pulled: Optional[float] = None

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
  """Node provisioning record with creation, readiness, and machine info."""

  name: str
  created: float
  ready: Optional[float]
  machine_type: Optional[str] = None
  nodepool: Optional[str] = None

  @classmethod
  def from_item(cls, item: dict[str, Any]) -> NodeRecord:
    """Create NodeRecord from Kubernetes node JSON object."""
    labels = (item.get('metadata') or {}).get('labels') or {}
    machine_type = labels.get('node.kubernetes.io/instance-type') or labels.get(
        'beta.kubernetes.io/instance-type'
    )
    nodepool = (
        labels.get('cloud.google.com/gke-nodepool')
        or labels.get('karpenter.sh/nodepool')
        or labels.get('pkb_nodepool')
    )
    return cls(
        name=item['metadata']['name'],
        created=_created(item),
        ready=condition_time(_conditions(item), 'Ready'),
        machine_type=machine_type,
        nodepool=nodepool,
    )


def parse_items(
    resource_json: dict[str, Any], factory: Callable[[dict[str, Any]], Any]
) -> list[Any]:
  """Parse items from a Kubernetes resource JSON blob using the given factory."""
  return [factory(item) for item in resource_json.get('items') or []]


def _extract_event_timestamp(event: dict[str, Any]) -> Optional[float]:
  """Extracts epoch seconds from a Kubernetes Event object."""
  for field in ('firstTimestamp', 'eventTime', 'lastTimestamp'):
    val = event.get(field)
    if val:
      try:
        return parse_epoch_seconds(val)
      except (ValueError, TypeError):
        continue
  return None


def enrich_pods_from_events(
    pods: Sequence[PodRecord],
    events_json: dict[str, Any],
) -> list[PodRecord]:
  """Recovers overwritten conditions & timestamps from Kubernetes Events.

  When Kubernetes transitions `PodScheduled` from False to True, the False
  condition is overwritten in-place in `pod.status.conditions`. However, the
  corresponding `FailedScheduling` event persists in the event log. Similarly,
  Cluster Autoscaler emits `TriggeredScaleUp` and Karpenter emits `Nominated`
  events, and kubelet emits `Pulling` / `Pulled` image events.

  Args:
    pods: Sequence of PodRecord instances.
    events_json: Parsed JSON dict from `kubectl get events -o json`.

  Returns:
    List of PodRecord instances enriched with event timestamps.
  """
  pods_by_name = {p.name: p for p in pods}
  earliest_event: dict[tuple[str, str], float] = {}

  for event in events_json.get('items') or []:
    obj = event.get('involvedObject') or {}
    if obj.get('kind') != 'Pod':
      continue
    pod_name = obj.get('name')
    if not pod_name or pod_name not in pods_by_name:
      continue
    reason = event.get('reason')
    if not reason:
      continue
    ts = _extract_event_timestamp(event)
    if ts is None:
      continue
    key = (pod_name, reason)
    if key not in earliest_event or ts < earliest_event[key]:
      earliest_event[key] = ts

  enriched: list[PodRecord] = []
  for pod in pods:
    unschedulable = pod.unschedulable
    if unschedulable is None:
      unschedulable = earliest_event.get((pod.name, 'FailedScheduling'))

    scale_up_triggered = pod.scale_up_triggered
    if scale_up_triggered is None:
      scale_up_triggered = earliest_event.get((pod.name, 'TriggeredScaleUp'))
    if scale_up_triggered is None:
      scale_up_triggered = earliest_event.get((pod.name, 'Nominated'))

    image_pulling = pod.image_pulling
    if image_pulling is None:
      image_pulling = earliest_event.get((pod.name, 'Pulling'))
    image_pulled = pod.image_pulled
    if image_pulled is None:
      image_pulled = earliest_event.get((pod.name, 'Pulled'))

    enriched.append(
        dataclasses.replace(
            pod,
            unschedulable=unschedulable,
            scale_up_triggered=scale_up_triggered,
            image_pulling=image_pulling,
            image_pulled=image_pulled,
        )
    )
  return enriched


def _sub(a: Optional[float], b: Optional[float]) -> Optional[float]:
  """Subtract b from a, returning None if either is None."""
  if a is None or b is None:
    return None
  return a - b


def _non_negative_sub(
    a: Optional[float], b: Optional[float]
) -> Optional[float]:
  """Subtract b from a, returning None (and logging) if a < b."""
  diff = _sub(a, b)
  if diff is None:
    return None
  if diff < 0:
    logging.warning(
        'Negative provisioning sub-phase interval (%s - %s = %.3fs); '
        'dropping sample (possible clock or timestamp granularity skew).',
        a,
        b,
        diff,
    )
    return None
  return diff


@dataclasses.dataclass(frozen=True)
class NodeProvisioningIntervals:
  """Per-pod cumulative provisioning interval measurements from `pod.created`."""

  pod_name: str
  node_name: Optional[str]

  node_registered_s: Optional[float]
  node_ready_s: Optional[float]
  pod_scheduled_s: Optional[float]
  pod_ready_s: Optional[float]

  pod_unschedulable_s: Optional[float] = None
  pod_scale_up_triggered_s: Optional[float] = None
  pod_containers_ready_s: Optional[float] = None
  node_boot_to_ready_s: Optional[float] = None
  node_ready_to_pod_scheduled_s: Optional[float] = None
  pod_scheduled_to_ready_s: Optional[float] = None
  image_pull_s: Optional[float] = None
  node_machine_type: Optional[str] = None
  node_pool: Optional[str] = None

  def get_metadata(self) -> dict[str, str]:
    """Returns per-pod and node metadata dictionary for sample emission."""
    md = {'pod_name': self.pod_name}
    if self.node_name:
      md['node_name'] = self.node_name
    if self.node_machine_type:
      md['node_machine_type'] = self.node_machine_type
    if self.node_pool:
      md['node_pool'] = self.node_pool
    return md


def compute_intervals(
    pods: Sequence[PodRecord],
    nodes: Sequence[NodeRecord],
) -> list[NodeProvisioningIntervals]:
  """Joins pod -> node by node name and computes cumulative and sub-phase intervals.

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
    if node is not None and node.created >= pod.created:
      node_created = node.created
      node_ready = node.ready
    else:
      node_created = None
      node_ready = None

    out.append(
        NodeProvisioningIntervals(
            pod_name=pod.name,
            node_name=pod.node_name,
            node_registered_s=_sub(node_created, pod.created),
            node_ready_s=_sub(node_ready, pod.created),
            pod_scheduled_s=_sub(pod.scheduled, pod.created),
            pod_ready_s=_sub(pod.ready, pod.created),
            pod_unschedulable_s=_non_negative_sub(
                pod.unschedulable, pod.created
            ),
            pod_scale_up_triggered_s=_non_negative_sub(
                pod.scale_up_triggered, pod.created
            ),
            pod_containers_ready_s=_non_negative_sub(
                pod.containers_ready, pod.created
            ),
            node_boot_to_ready_s=_non_negative_sub(node_ready, node_created),
            node_ready_to_pod_scheduled_s=_non_negative_sub(
                pod.scheduled, node_ready
            ),
            pod_scheduled_to_ready_s=_non_negative_sub(
                pod.ready, pod.scheduled
            ),
            image_pull_s=_non_negative_sub(pod.image_pulled, pod.image_pulling),
            node_machine_type=node.machine_type if node else None,
            node_pool=node.nodepool if node else None,
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
    pods: Optional[Sequence[PodRecord]] = None,
    nodes: Optional[Sequence[NodeRecord]] = None,
    target_replicas: Optional[int] = None,
    scale_triggered_at: Optional[float] = None,
) -> list[sample.Sample]:
  """Computes percentile, count, and per-pod/node samples from provisioning intervals.

  Args:
    intervals: Sequence of NodeProvisioningIntervals to summarize.
    extra_metadata: Metadata dictionary to attach to every emitted sample.
    pods: Optional sequence of PodRecords (required for pod-anchored
      deduplicated node metrics and fleet milestones).
    nodes: Optional sequence of NodeRecords (required for deduplicated node
      metrics).
    target_replicas: Optional target replica count for milestone percentages.
    scale_triggered_at: Optional scale trigger timestamp for fleet milestones.

  Returns:
    List of sample.Sample objects representing intervals, milestones, and
    counts.
  """
  pods = pods or []
  nodes = nodes or []
  samples: list[sample.Sample] = []

  # 1. Per-pod interval metrics
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
        md = dict(extra_metadata) | iv.get_metadata()
        samples.append(sample.Sample(field, v, 'seconds', md))

  # 2. Deduplicated per-node intervals (bin-packing aware)
  nodes_by_name = {n.name: n for n in nodes}
  pods_by_node: dict[str, list[PodRecord]] = collections.defaultdict(list)
  for p in pods:
    if p.node_name:
      pods_by_node[p.node_name].append(p)

  unique_reg_values: list[float] = []
  unique_ready_values: list[float] = []
  unique_boot_values: list[float] = []
  node_pack_counts: list[int] = []

  for node_name, node_pods in pods_by_node.items():
    node = nodes_by_name.get(node_name)
    min_pod_created = min(p.created for p in node_pods)
    if node is not None and node.created >= min_pod_created:
      node_pack_counts.append(len(node_pods))
      reg = _sub(node.created, min_pod_created)
      if reg is not None:
        unique_reg_values.append(reg)
      rdy = _sub(node.ready, min_pod_created)
      if rdy is not None:
        unique_ready_values.append(rdy)
      boot = _non_negative_sub(node.ready, node.created)
      if boot is not None:
        unique_boot_values.append(boot)

  node_metrics = [
      ('unique_node_registered_s', unique_reg_values),
      ('unique_node_ready_s', unique_ready_values),
      ('unique_node_boot_to_ready_s', unique_boot_values),
  ]
  for metric_name, values in node_metrics:
    if not values:
      continue
    samples.append(
        sample.Sample(
            f'{metric_name}_count', len(values), 'count', dict(extra_metadata)
        )
    )
    base_sample = sample.Sample(
        f'{metric_name}_', 0, 'seconds', dict(extra_metadata)
    )
    samples.extend(
        sample.SummarizePercentiles(values, base_sample, _PERCENTILES)
    )

  if node_pack_counts:
    samples.append(
        sample.Sample(
            'provisioned_node_count',
            len(node_pack_counts),
            'count',
            dict(extra_metadata),
        )
    )
    samples.append(
        sample.Sample(
            'pods_per_node_mean',
            sum(node_pack_counts) / len(node_pack_counts),
            'count',
            dict(extra_metadata),
        )
    )
    samples.append(
        sample.Sample(
            'pods_per_node_max',
            max(node_pack_counts),
            'count',
            dict(extra_metadata),
        )
    )

  # 3. Fleet makespan and readiness milestones
  if pods:
    t0 = (
        scale_triggered_at
        if scale_triggered_at is not None
        else min(p.created for p in pods)
    )
    ready_deltas = sorted(
        p.ready - t0 for p in pods if p.ready is not None and p.ready >= t0
    )
    ready_count = len(ready_deltas)
    expected_count = (
        target_replicas if target_replicas is not None else len(pods)
    )
    unready_count = max(0, expected_count - ready_count)

    samples.append(
        sample.Sample(
            'pods_ready_count', ready_count, 'count', dict(extra_metadata)
        )
    )
    samples.append(
        sample.Sample(
            'pods_unready_count', unready_count, 'count', dict(extra_metadata)
        )
    )

    if ready_deltas:
      samples.append(
          sample.Sample(
              'makespan_s', max(ready_deltas), 'seconds', dict(extra_metadata)
          )
      )
      for pct, metric_name in (
          (0.50, 'time_to_50pct_ready_s'),
          (0.90, 'time_to_90pct_ready_s'),
          (1.00, 'time_to_100pct_ready_s'),
      ):
        threshold = max(1, int(round(pct * expected_count)))
        if ready_count >= threshold:
          samples.append(
              sample.Sample(
                  metric_name,
                  ready_deltas[threshold - 1],
                  'seconds',
                  dict(extra_metadata),
              )
          )

  return samples

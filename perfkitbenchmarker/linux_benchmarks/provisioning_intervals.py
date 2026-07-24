"""Pure metric core for node-provisioning latency.

Parses pod/node/NodeClaim JSON into records, joins them by node name, and
computes per-pod provisioning sub-intervals with sub-second precision. No
kubectl calls here, so the math is unit-testable without a cluster. Missing
prerequisites yield None rather than dropping a metric.
"""

from __future__ import annotations

import dataclasses
from typing import Any, Callable, Optional
from dateutil import parser as _date_parser
import numpy as np
from perfkitbenchmarker import sample


def parse_epoch_seconds(timestamp: str) -> float:
  """RFC3339 timestamp -> float epoch seconds (sub-second preserved)."""
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
  return item.get('status', {}).get('conditions') or []


@dataclasses.dataclass(frozen=True)
class PodRecord:
  """Pod provisioning record with all relevant condition timestamps."""
  name: str
  created: float
  node_name: Optional[str]
  unschedulable: Optional[float]
  scheduled: Optional[float]
  containers_ready: Optional[float]
  ready: Optional[float]

  @classmethod
  def from_item(cls, item: dict[str, Any]) -> 'PodRecord':
    """Create PodRecord from Kubernetes pod JSON object."""
    c = _conditions(item)
    return cls(
        name=item['metadata']['name'],
        created=_created(item),
        node_name=item.get('spec', {}).get('nodeName'),
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
  def from_item(cls, item: dict[str, Any]) -> 'NodeRecord':
    """Create NodeRecord from Kubernetes node JSON object."""
    return cls(
        name=item['metadata']['name'],
        created=_created(item),
        ready=condition_time(_conditions(item), 'Ready'),
    )


@dataclasses.dataclass(frozen=True)
class NodeClaimRecord:
  """NodeClaim provisioning record with all relevant condition timestamps."""
  name: str
  node_name: Optional[str]
  created: float
  launched: Optional[float]
  registered: Optional[float]
  initialized: Optional[float]

  @classmethod
  def from_item(cls, item: dict[str, Any]) -> 'NodeClaimRecord':
    """Create NodeClaimRecord from Kubernetes NodeClaim JSON object."""
    c = _conditions(item)
    return cls(
        name=item['metadata']['name'],
        node_name=item.get('status', {}).get('nodeName'),
        created=_created(item),
        launched=condition_time(c, 'Launched'),
        registered=condition_time(c, 'Registered'),
        initialized=condition_time(c, 'Initialized'),
    )


def parse_items(
    resource_json: dict[str, Any], factory: Callable[[dict[str, Any]], Any]
) -> list[Any]:
  """Parse items from a Kubernetes resource JSON blob using the given factory.

  Args:
    resource_json: Dictionary with 'items' key containing list of Kubernetes
      objects.
    factory: Callable that converts each item dict to a record.

  Returns:
    List of records created by applying factory to each item.
  """
  return [factory(item) for item in resource_json.get('items', [])]


def _sub(a: Optional[float], b: Optional[float]) -> Optional[float]:
  """Subtract b from a, returning None if either is None."""
  if a is None or b is None:
    return None
  return a - b


@dataclasses.dataclass(frozen=True)
class PodIntervals:
  """Per-pod provisioning interval measurements."""
  pod_name: str
  node_name: Optional[str]
  unschedulable_detection_s: Optional[float]
  provisioner_reaction_s: Optional[float]
  provisioner_detection_s: Optional[float]
  karpenter_batch_s: Optional[float]
  nodeclaim_register_s: Optional[float]
  nodeclaim_init_s: Optional[float]
  node_vm_boot_s: Optional[float]
  total_provisioning_s: Optional[float]
  pod_bind_s: Optional[float]
  container_ready_s: Optional[float]
  pod_ready_s: Optional[float]
  pod_to_containers_ready_s: Optional[float]
  pod_to_ready_s: Optional[float]


def compute_intervals(
    pods: list[PodRecord],
    nodes: list[NodeRecord],
    nodeclaims: list[NodeClaimRecord],
) -> list[PodIntervals]:
  """Joins pod -> node -> nodeclaim by node name and computes sub-intervals.

  Args:
    pods: List of PodRecord instances.
    nodes: List of NodeRecord instances.
    nodeclaims: List of NodeClaimRecord instances.

  Returns:
    List of PodIntervals with all interval measurements computed.
  """
  nodes_by_name = {n.name: n for n in nodes}
  claims_by_node = {c.node_name: c for c in nodeclaims if c.node_name}

  out: list[PodIntervals] = []
  for pod in pods:
    node = nodes_by_name.get(pod.node_name) if pod.node_name else None
    claim = claims_by_node.get(pod.node_name) if pod.node_name else None

    node_created = node.created if node else None
    node_ready = node.ready if node else None
    t_provision = claim.created if claim else node_created
    launched = claim.launched if claim else None

    out.append(PodIntervals(
        pod_name=pod.name,
        node_name=pod.node_name,
        unschedulable_detection_s=_sub(pod.unschedulable, pod.created),
        provisioner_reaction_s=_sub(t_provision, pod.unschedulable),
        provisioner_detection_s=_sub(t_provision, pod.created),
        karpenter_batch_s=_sub(launched, t_provision) if claim else None,
        nodeclaim_register_s=(
            _sub(claim.registered, claim.launched) if claim else None),
        nodeclaim_init_s=(
            _sub(claim.initialized, claim.registered) if claim else None),
        node_vm_boot_s=_sub(node_ready, node_created),
        total_provisioning_s=_sub(node_ready, pod.created),
        pod_bind_s=_sub(pod.scheduled, node_ready),
        container_ready_s=_sub(pod.containers_ready, pod.scheduled),
        pod_ready_s=_sub(pod.ready, pod.scheduled),
        pod_to_containers_ready_s=_sub(pod.containers_ready, pod.created),
        pod_to_ready_s=_sub(pod.ready, pod.created),
    ))
  return out


@dataclasses.dataclass(frozen=True)
class PodIntervals2(PodIntervals):
  """PodIntervals extended with device plugin readiness interval."""
  device_plugin_ready_s: Optional[float] = None


def attach_gpu_interval(
    intervals: list[PodIntervals],
    nodes: list[NodeRecord],
    gpu_allocatable_at: dict[str, float],
) -> list[PodIntervals2]:
  """Attach GPU device-plugin-ready interval to pod intervals.

  Args:
    intervals: List of PodIntervals instances.
    nodes: List of NodeRecord instances.
    gpu_allocatable_at: Dict mapping node name to epoch second when
      nvidia.com/gpu appeared in allocatable.

  Returns:
    List of PodIntervals2 with device_plugin_ready_s = T_gpu - T3.
  """
  ready_by_node = {n.name: n.ready for n in nodes}
  out: list[PodIntervals2] = []
  for iv in intervals:
    t_gpu = gpu_allocatable_at.get(iv.node_name) if iv.node_name else None
    t3 = ready_by_node.get(iv.node_name) if iv.node_name else None
    out.append(PodIntervals2(**dataclasses.asdict(iv),
                             device_plugin_ready_s=_sub(t_gpu, t3)))
  return out


_INTERVAL_FIELDS = (
    'unschedulable_detection_s', 'provisioner_reaction_s',
    'provisioner_detection_s', 'karpenter_batch_s', 'nodeclaim_register_s',
    'nodeclaim_init_s', 'node_vm_boot_s', 'total_provisioning_s', 'pod_bind_s',
    'container_ready_s', 'pod_ready_s', 'pod_to_containers_ready_s',
    'pod_to_ready_s', 'device_plugin_ready_s',
)
_PERCENTILES = (('p50', 50), ('p90', 90), ('p99', 99))


def to_samples(
    intervals: list['PodIntervals'], extra_metadata: dict[str, Any]
) -> list[sample.Sample]:
  """Compute percentile and count samples from pod intervals.

  Args:
    intervals: List of PodIntervals instances.
    extra_metadata: Dictionary of extra metadata to attach to all samples.

  Returns:
    List of sample.Sample objects with percentile, count, and per-pod metrics.
    For each interval field with ≥1 non-None value: three percentile samples
    (p50, p90, p99), one count sample, and one raw sample per pod.
    All-None fields are skipped entirely.
  """
  samples: list[sample.Sample] = []
  for field in _INTERVAL_FIELDS:
    values = [getattr(iv, field, None) for iv in intervals
              if getattr(iv, field, None) is not None]
    if not values:
      continue
    samples.append(sample.Sample(
        f'{field}_count', len(values), 'count', dict(extra_metadata)))
    for label, pct in _PERCENTILES:
      samples.append(sample.Sample(
          f'{field}_{label}', float(np.percentile(values, pct)), 'seconds',
          dict(extra_metadata)))
    for iv in intervals:
      v = getattr(iv, field, None)
      if v is not None:
        md = dict(extra_metadata)
        md['k8s_pod_name'] = iv.pod_name
        samples.append(sample.Sample(field, v, 'seconds', md))
  return samples

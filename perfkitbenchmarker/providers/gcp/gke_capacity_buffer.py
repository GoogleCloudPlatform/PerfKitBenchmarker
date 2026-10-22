"""GKE Capacity Buffer resource."""

import json
import logging
import time

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_commands
import yaml

FLAGS = flags.FLAGS


class GkeCapacityBuffer(resource.BaseResource):
  """A resource class for managing GKE Capacity Buffers."""

  def __init__(self, spec):
    super().__init__()
    self.spec = spec
    self.buffer_name = f'pkb-{FLAGS.run_uri}'

  def _Create(self) -> None:
    """Creates the CapacityBuffer using kubectl apply."""
    self.baseline_cpu = 0.0
    if FLAGS.container_cluster_type != 'Auto':
      self.baseline_cpu = self._GetNodeCpu()

    provisioning_strategy = (
        'buffer.gke.io/standby-capacity'
        if FLAGS.gke_capacity_buffer_strategy == 'Standby'
        else 'buffer.x-k8s.io/active-capacity'
    )
    # Autopilot rejects the dummy pod if it doesn't meet minimums for
    # Performance class
    unit_cpu = '1'
    unit_memory = '2Gi'
    if (
        FLAGS.container_cluster_type == 'Auto'
        and FLAGS.gke_capacity_buffer_compute_class == 'Performance'
    ):
      unit_cpu = '2'
      unit_memory = '4Gi'

    compute_class = getattr(FLAGS, 'gke_capacity_buffer_compute_class', None)
    if FLAGS.container_cluster_type != 'Auto':
      self.baseline_cpu = self._GetNodeCpu()

    yaml_docs = kubernetes_commands.ConvertManifestToYamlDicts(
        'capacity_buffer.yaml.j2',
        provisioning_strategy=provisioning_strategy,
        cpu=FLAGS.gke_capacity_buffer_cpu,
        buffer_name=self.buffer_name,
        unit_cpu=unit_cpu,
        unit_memory=unit_memory,
        compute_class=compute_class,
    )
    kubernetes_commands.ApplyYaml(yaml_docs)

  def _Delete(self) -> None:
    """Deletes the CapacityBuffer idempotently."""
    kubernetes_commands.DeleteResource(
        f'capacitybuffer/{self.buffer_name}', ignore_not_found=True
    )
    kubernetes_commands.DeleteResource(
        'podtemplate/buffer-unit-template', ignore_not_found=True
    )

  def _Exists(self) -> bool:
    """Verifies the Custom Resource Definition was accepted by the cluster.

    Returns:
      True if accepted.
    """
    _, _, retcode = kubectl.RunKubectlCommand(
        ['get', 'capacitybuffer', self.buffer_name], raise_on_failure=False
    )
    return retcode == 0

  def _GetNodeCpu(self, required_taint=None, require_ready=True) -> float:
    stdout, _, retcode = kubectl.RunKubectlCommand(
        ['get', 'nodes', '-o', 'json'], raise_on_failure=False
    )
    if retcode != 0:
      return 0.0
    try:
      nodes_json = json.loads(stdout)
    except ValueError:
      return 0.0

    ready_cpu = 0.0
    for node in nodes_json.get('items', []):
      taints = node.get('spec', {}).get('taints', [])
      if required_taint:
        if not any(t.get('key') == required_taint for t in taints):
          continue

      if require_ready:
        conditions = node.get('status', {}).get('conditions', [])
        is_ready = any(
            c.get('type') == 'Ready' and c.get('status') == 'True'
            for c in conditions
        )
        if not is_ready:
          continue

      capacity_cpu = node.get('status', {}).get('capacity', {}).get('cpu', '0')
      if capacity_cpu.endswith('m'):
        ready_cpu += int(capacity_cpu[:-1]) / 1000.0
      else:
        ready_cpu += float(capacity_cpu)
    return ready_cpu

  @vm_util.Retry(
      poll_interval=10,
      max_retries=60,
      retryable_exceptions=(errors.Resource.RetryableCreationError,),
  )
  def _IsReady(self) -> bool:
    """Checks if the CapacityBuffer nodes are provisioned by polling nodes.

    Returns:
      True if provisioned.
    """
    if FLAGS.gke_capacity_buffer_strategy == 'Active':

      # 1. Check if CapacityBuffer is recognized and Provisioning by CA
      stdout, _, retcode = kubectl.RunKubectlCommand(
          ['get', 'capacitybuffer', self.buffer_name, '-o', 'json'],
          raise_on_failure=False,
      )
      if retcode != 0:
        raise errors.Resource.RetryableCreationError(
            'kubectl get capacitybuffer failed')
      try:
        cb = json.loads(stdout)
      except Exception as exc:
        raise errors.Resource.RetryableCreationError(
            'json.loads failed on capacitybuffer') from exc

      conditions = cb.get('status', {}).get('conditions', [])

      for c in conditions:
        if (c.get('type') == 'ReadyForProvisioning' and
            str(c.get('status')) == 'False' and
            c.get('reason') == 'error'):
          raise errors.Resource.CreationError(
              f"CapacityBuffer rejected: {c.get('message')}"
          )
      provisioning = any(c.get('type') == 'Provisioning' and
                         str(c.get('status')) == 'True' for c in conditions)
      if not provisioning:
        raise errors.Resource.RetryableCreationError(
            'CapacityBuffer is not yet Provisioning.')

      # 2. Check CA status to ensure it has finished scaling and all
      # nodes are ready
      stdout, _, retcode = kubectl.RunKubectlCommand(
          ['get', 'configmap', 'cluster-autoscaler-status', '-n',
           'kube-system', '-o', 'json'], raise_on_failure=False
      )
      if retcode != 0:
        raise errors.Resource.RetryableCreationError(
            'Failed to get cluster-autoscaler-status')
      try:
        ca_cm = json.loads(stdout)
        status_yaml = ca_cm.get('data', {}).get('status', '')
        ca_status = yaml.safe_load(status_yaml)
      except Exception as exc:
        raise errors.Resource.RetryableCreationError(
            'Failed to parse cluster-autoscaler-status') from exc

      cw = ca_status.get('clusterWide', {})
      if cw.get('scaleUp', {}).get('status') == 'InProgress':
        raise errors.Resource.RetryableCreationError(
            'CA is still scaling up.')

      unready = (cw.get('nodeCounts', {}).get('registered', {})
                 .get('unready', {}).get('total', 0))
      if unready > 0:
        raise errors.Resource.RetryableCreationError(
            f'Cluster has {unready} unready nodes.')

      return True

    if FLAGS.container_cluster_type == 'Auto':
      # Standby on Autopilot
      logging.info(
          'Sleeping for 5 minutes for Autopilot Standby CapacityBuffer '
          'to allow 2m init and suspension...')
      time.sleep(300)
      return True

    # Standby on Standard clusters
    suspended_cpu = self._GetNodeCpu(
        required_taint='buffer.gke.io/standby-capacity-node-suspended',
        require_ready=False,
    )
    expected_cpu = float(FLAGS.gke_capacity_buffer_cpu)
    if suspended_cpu < expected_cpu:
      raise errors.Resource.RetryableCreationError(
          f'Not enough suspended CPU. Expected at least {expected_cpu}, '
          f'got {suspended_cpu}.'
      )

    if (FLAGS.gke_capacity_buffer_strategy == 'Standby' and
        not getattr(self, '_standby_suspended', False)):
      logging.info(
          'Standby nodes are tainted. Sleeping for 2 minutes to allow '
          'them to suspend.')
      time.sleep(120)
      self._standby_suspended = True

    return True

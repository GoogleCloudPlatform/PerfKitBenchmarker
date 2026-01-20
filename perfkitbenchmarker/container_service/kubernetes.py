# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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
"""Contains classes related to managed kubernetes container services."""

import json
from typing import Any

from absl import flags
from perfkitbenchmarker import container_service
from perfkitbenchmarker import events
from perfkitbenchmarker import kubernetes_helper
from perfkitbenchmarker import vm_util
import requests
import yaml

# pylint: disable=unused-import
# Temporarily hoist kubectl related methods into this namespace
from .kubectl import RETRYABLE_KUBECTL_ERRORS
from .kubectl import RunKubectlCommand
from .kubectl import RunRetryableKubectlCommand
# Temporarily hoist cluster related classes into this namespace
from .kubernetes_cluster import INGRESS_JSONPATH
from .kubernetes_cluster import KubernetesCluster
# Temporarily hoist k8s cluster commands into this namespace
from .kubernetes_commands import KubernetesClusterCommands
# Temporarily hoist event related classes into this namespace
from .kubernetes_events import KubernetesEvent
from .kubernetes_events import KubernetesEventPoller
from .kubernetes_events import KubernetesEventResource
# pylint: enable=unused-import


BenchmarkSpec = Any  # benchmark_spec lib imports this module.


flags.DEFINE_string(
    'kubeconfig',
    None,
    'Path to kubeconfig to be used by kubectl. '
    "If unspecified, it will be set to a file in this run's "
    'temporary directory.',
)

flags.DEFINE_string('kubectl', 'kubectl', 'Path to kubectl tool')

_K8S_INGRESS = """
apiVersion: extensions/v1beta1
kind: Ingress
metadata:
  name: {service_name}-ingress
spec:
  backend:
    serviceName: {service_name}
    servicePort: 8080
"""


class KubernetesPod:
  """Representation of a Kubernetes pod.

  It can be created as a PKB managed resource using KubernetesContainer,
  or created with ApplyManifest and directly constructed.
  """

  def __init__(self, name=None, **kwargs):
    super().__init__(**kwargs)
    assert name
    self.name = name

  def _GetPod(self) -> dict[str, Any]:
    """Gets a representation of the POD and returns it."""
    stdout, _, _ = RunKubectlCommand(['get', 'pod', self.name, '-o', 'yaml'])
    pod = yaml.safe_load(stdout)
    self.ip_address = pod.get('status', {}).get('podIP')
    return pod

  def _ValidatePodHasNotFailed(self, status: dict[str, Any]):
    """Raises an exception if the pod has failed."""
    # Inspect the pod's status to determine if it succeeded, has failed, or is
    # doomed to fail.
    # https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/
    phase = status['phase']
    if phase == 'Succeeded':
      return
    elif phase == 'Failed':
      raise container_service.FatalContainerException(
          f'Pod {self.name} failed:\n{yaml.dump(status)}'
      )
    else:
      for condition in status.get('conditions', []):
        if (
            condition['type'] == 'PodScheduled'
            and condition['status'] == 'False'
            and condition['reason'] == 'Unschedulable'
        ):
          # TODO(pclay): Revisit this when we scale clusters.
          raise container_service.FatalContainerException(
              f"Pod {self.name} failed to schedule:\n{condition['message']}"
          )
      for container_status in status.get('containerStatuses', []):
        waiting_status = container_status['state'].get('waiting', {})
        if waiting_status.get('reason') in [
            'ErrImagePull',
            'ImagePullBackOff',
        ]:
          raise container_service.FatalContainerException(
              f'Failed to find container image for {self.name}:\n'
              + yaml.dump(waiting_status.get('message'))
          )

  def WaitForExit(self, timeout: int | None = None) -> dict[str, Any]:
    """Gets the finished running container."""

    @vm_util.Retry(
        timeout=timeout,
        retryable_exceptions=(container_service.RetriableContainerException,),
    )
    def _WaitForExit():
      pod = self._GetPod()
      status = pod['status']
      self._ValidatePodHasNotFailed(status)
      phase = status['phase']
      if phase == 'Succeeded':
        return pod
      else:
        raise container_service.RetriableContainerException(
            f'Pod phase ({phase}) not in finished phases.'
        )

    return _WaitForExit()

  def GetLogs(self):
    """Returns the logs from the container."""
    stdout, _, _ = RunKubectlCommand(['logs', self.name])
    return stdout


# Order KubernetesPod first so that it's constructor is called first.
class KubernetesContainer(KubernetesPod, container_service.BaseContainer):
  """A KubernetesPod based flavor of Container."""

  def _Create(self):
    """Creates the container."""
    run_cmd = [
        'run',
        self.name,
        '--image=%s' % self.image,
        '--restart=Never',
        # Allow scheduling on ARM nodes.
        '--overrides',
        json.dumps({
            'spec': {
                'tolerations': [{
                    'operator': 'Exists',
                    'key': 'kubernetes.io/arch',
                    'effect': 'NoSchedule',
                }]
            }
        }),
    ]

    limits = []
    if self.cpus:
      limits.append(f'cpu={int(1000 * self.cpus)}m')
    if self.memory:
      limits.append(f'memory={self.memory}Mi')
    if limits:
      run_cmd.append('--limits=' + ','.join(limits))

    if self.command:
      run_cmd.extend(['--command', '--'])
      run_cmd.extend(self.command)
    RunKubectlCommand(run_cmd)

  def _Delete(self):
    """Deletes the container."""
    pass

  def _IsReady(self):
    """Returns true if the container has stopped pending."""
    status = self._GetPod()['status']
    super()._ValidatePodHasNotFailed(status)
    return status['phase'] != 'Pending'


class KubernetesContainerService(container_service.BaseContainerService):
  """A Kubernetes flavor of Container Service."""

  def __init__(self, container_spec, name):
    super().__init__(container_spec)
    self.name = name
    self.port = 8080

  def _Create(self):
    run_cmd = [
        'run',
        self.name,
        '--image=%s' % self.image,
        '--port',
        str(self.port),
    ]

    limits = []
    if self.cpus:
      limits.append(f'cpu={int(1000 * self.cpus)}m')
    if self.memory:
      limits.append(f'memory={self.memory}Mi')
    if limits:
      run_cmd.append('--limits=' + ','.join(limits))

    if self.command:
      run_cmd.extend(['--command', '--'])
      run_cmd.extend(self.command)
    RunKubectlCommand(run_cmd)

    expose_cmd = [
        'expose',
        'deployment',
        self.name,
        '--type',
        'NodePort',
        '--target-port',
        str(self.port),
    ]
    RunKubectlCommand(expose_cmd)
    with vm_util.NamedTemporaryFile() as tf:
      tf.write(_K8S_INGRESS.format(service_name=self.name))
      tf.close()
      kubernetes_helper.CreateFromFile(tf.name)

  def _GetIpAddress(self):
    """Attempts to set the Service's ip address."""
    ingress_name = '%s-ingress' % self.name
    get_cmd = [
        'get',
        'ing',
        ingress_name,
        '-o',
        'jsonpath={.status.loadBalancer.ingress[*].ip}',
    ]
    stdout, _, _ = RunKubectlCommand(get_cmd)
    ip_address = stdout
    if ip_address:
      self.ip_address = ip_address

  def _IsReady(self):
    """Returns True if the Service is ready."""
    if self.ip_address is None:
      self._GetIpAddress()
    if self.ip_address is not None:
      url = 'http://%s' % self.ip_address
      r = requests.get(url)
      if r.status_code == 200:
        return True
    return False

  def _Delete(self):
    """Deletes the service."""
    with vm_util.NamedTemporaryFile() as tf:
      tf.write(_K8S_INGRESS.format(service_name=self.name))
      tf.close()
      kubernetes_helper.DeleteFromFile(tf.name)

    delete_cmd = ['delete', 'deployment', self.name]
    RunKubectlCommand(delete_cmd, raise_on_failure=False)


@events.benchmark_start.connect
def _SetKubeConfig(unused_sender, benchmark_spec: BenchmarkSpec):
  """Sets the value for the kubeconfig flag if it's unspecified."""
  if not flags.FLAGS.kubeconfig:
    flags.FLAGS.kubeconfig = vm_util.PrependTempDir(
        'kubeconfig' + str(benchmark_spec.sequence_number)
    )
    # Store the value for subsequent run stages.
    benchmark_spec.config.flags['kubeconfig'] = flags.FLAGS.kubeconfig

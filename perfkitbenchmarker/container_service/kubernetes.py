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

import calendar
import dataclasses
import datetime
import functools
import ipaddress
import json
import logging
import multiprocessing
from multiprocessing import synchronize
import queue as py_queue
import re
import time
from typing import Any, Callable, Dict, Iterable, Iterator, Optional, Sequence

from absl import flags
from perfkitbenchmarker import container_service
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import events
from perfkitbenchmarker import kubernetes_helper
from perfkitbenchmarker import units
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import container_spec as container_spec_lib
from perfkitbenchmarker.resources import kubernetes_inference_server
from perfkitbenchmarker.sample import Sample
import requests
import yaml

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
RESOURCE_DELETE_SLEEP_SECONDS = 5
RETRYABLE_KUBECTL_ERRORS = [
    (
        '"unable to decode an event from the watch stream: http2: client'
        ' connection lost"'
    ),
    'read: connection reset by peer',
    'Unable to connect to the server: dial tcp',
    'Unable to connect to the server: net/http: TLS handshake timeout',
    # These come up in kubectl exec
    'error dialing backend:',
    'connect: connection timed out',
    'error sending request:',
    '(abnormal closure): unexpected EOF',
]
INGRESS_JSONPATH = '{.status.loadBalancer.ingress[0]}'


def RunKubectlCommand(command: list[str], **kwargs) -> tuple[str, str, int]:
  """Run a kubectl command."""
  kwargs = vm_util.IncrementStackLevel(**kwargs)
  cmd = [
      flags.FLAGS.kubectl,
      '--kubeconfig',
      flags.FLAGS.kubeconfig,
  ] + command

  orig_suppress_failure = None
  if 'suppress_failure' in kwargs:
    orig_suppress_failure = kwargs['suppress_failure']

  def _DetectTimeoutViaSuppressFailure(stdout, stderr, retcode):
    # Check for kubectl timeout. If found, treat it the same as a regular
    # timeout.
    if retcode != 0:
      for error_substring in RETRYABLE_KUBECTL_ERRORS:
        if error_substring in stderr:
          # Raise timeout error regardless of raise_on_failure - as the intended
          # semantics is to ignore expected errors caused by invoking the
          # command not errors from PKB infrastructure.
          raise_on_timeout = (
              kwargs['raise_on_timeout']
              if 'raise_on_timeout' in kwargs
              else True
          )
          if raise_on_timeout:
            raise errors.VmUtil.IssueCommandTimeoutError(stderr)
    # Else, revert to user supplied kwargs values.
    if orig_suppress_failure is not None:
      return orig_suppress_failure(stdout, stderr, retcode)
    if 'raise_on_failure' in kwargs:
      return not kwargs['raise_on_failure']
    return False

  kwargs['suppress_failure'] = _DetectTimeoutViaSuppressFailure

  return vm_util.IssueCommand(cmd, **kwargs)


def RunRetryableKubectlCommand(
    run_cmd: list[str], timeout: int | None = None, **kwargs
) -> tuple[str, str, int]:
  """Runs a kubectl command, retrying somewhat expected errors."""
  if 'raise_on_timeout' in kwargs and kwargs['raise_on_timeout']:
    raise ValueError(
        'RunRetryableKubectlCommand does not allow `raise_on_timeout=True`'
        ' (since timeouts are retryable).'
    )

  kwargs = vm_util.IncrementStackLevel(**kwargs)

  @vm_util.Retry(
      timeout=timeout,
      retryable_exceptions=(errors.VmUtil.IssueCommandTimeoutError,),
  )
  def _RunRetryablePart(run_cmd: list[str], **kwargs) -> tuple[str, str, int]:
    """Inner function retries command so timeout can be passed to decorator."""
    kwargs['stack_level'] += 1
    return RunKubectlCommand(run_cmd, raise_on_timeout=True, **kwargs)

  return _RunRetryablePart(run_cmd, timeout=timeout, **kwargs)


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


class KubernetesClusterCommands:
  """Implementation of many Kubernetes commands.

  All methods just call generic kubectl commands without needing instance
  information.
  """

  @staticmethod
  def _DeleteAllFromDefaultNamespace():
    """Deletes all resources from a namespace.

    Since StatefulSets do not reclaim PVCs upon deletion, they are explicitly
    deleted here to prevent dynamically provisioned PDs from leaking once the
    cluster has been deleted.
    """
    try:
      # Delete deployments and jobs first as otherwise autorepair will redeploy
      # deleted pods.
      run_cmd = ['delete', 'deployment', '--all', '-n', 'default']
      RunRetryableKubectlCommand(run_cmd)

      run_cmd = ['delete', 'job', '--all', '-n', 'default']
      RunRetryableKubectlCommand(run_cmd)

      timeout = 60 * 20
      run_cmd = [
          'delete',
          'all',
          '--all',
          '-n',
          'default',
          f'--timeout={timeout}s',
      ]
      RunRetryableKubectlCommand(run_cmd, timeout=timeout)

      run_cmd = ['delete', 'pvc', '--all', '-n', 'default']
      RunKubectlCommand(run_cmd)
      # There maybe a slight race if resources are cleaned up in the background
      # where deleting the cluster immediately prevents the PVCs from being
      # deleted.
      logging.info(
          'Sleeping for %s seconds to give resources time to delete.',
          RESOURCE_DELETE_SLEEP_SECONDS,
      )
      time.sleep(RESOURCE_DELETE_SLEEP_SECONDS)
    except (
        errors.VmUtil.IssueCommandTimeoutError,
        vm_util.TimeoutExceededRetryError,
    ) as e:
      raise errors.Resource.RetryableDeletionError(
          'Timed out while deleting all resources from default namespace. We'
          ' should still continue trying to delete everything.'
      ) from e

  @staticmethod
  def ApplyManifest(
      manifest_file: str, should_log_file: bool = True, **kwargs
  ) -> Iterator[str]:
    """Applies a declarative Kubernetes manifest; possibly with jinja.

    Args:
      manifest_file: The name of the YAML file or YAML template.
      should_log_file: Whether to log the rendered manifest to stdout or not.
      **kwargs: Arguments to the jinja template.

    Returns:
      Names of the resources, e.g. [deployment.apps/mydeploy, pod/foo]
    """
    filename = data.ResourcePath(manifest_file)
    if not filename.endswith('.j2'):
      if kwargs:
        raise AssertionError(
            'kwargs should be empty for non-jinja templates. '
            f'Found when reading {manifest_file}'
        )
      return KubernetesClusterCommands._ApplyRenderedManifest(filename)

    manifest = vm_util.ReadAndRenderJinja2Template(
        manifest_file, trim_spaces=False, **kwargs
    )
    return KubernetesClusterCommands._WriteAndApplyManifest(
        manifest, should_log_file
    )

  @staticmethod
  def _ApplyRenderedManifest(manifest_file: str) -> Iterator[str]:
    """Applies a rendered Kubernetes manifest & returns resources created.

    Args:
      manifest_file: The full path of the YAML file.

    Returns:
      Names of the resources, e.g. [deployment.apps/mydeploy, pod/foo]
    """
    out, _, _ = RunKubectlCommand(['apply', '-f', manifest_file])

    def _ParseApplyOutput(stdout: str) -> Iterator[str]:
      """Parses the output of kubectl apply to get the name of the resource."""
      # Example input: deployment.apps/pkb123 created
      for line in stdout.splitlines():
        match = re.search(r'([^\s/]+/[^\s/]+) (created|configured)', line)
        if match:
          yield match.group(1)

    # Inner function needed to run kubectl command even if output is unused.
    return _ParseApplyOutput(out)

  @staticmethod
  def _WriteAndApplyManifest(
      manifest: str, should_log_file: bool = True
  ) -> Iterator[str]:
    """Writes the string to file and applies it."""
    with vm_util.NamedTemporaryFile(
        mode='w', suffix='.yaml'
    ) as rendered_template:
      rendered_template.write(manifest)
      rendered_template.close()
      if should_log_file:
        logging.info(
            'Rendered manifest file %s with contents:\n%s',
            rendered_template.name,
            manifest,
            stacklevel=2,
        )
      resource_names = KubernetesClusterCommands._ApplyRenderedManifest(
          rendered_template.name
      )
      return resource_names

  @staticmethod
  def ConvertManifestToYamlDicts(
      manifest_file: str, **kwargs
  ) -> list[dict[str, Any]]:
    """Reads a Kubernetes manifest from a file and converts it to YAML.

    Args:
      manifest_file: The name of the YAML file or YAML template.
      **kwargs: Arguments to the jinja template.

    Returns:
      The various YAML documents as a list of dictionaries.
    """
    manifest = vm_util.ReadAndRenderJinja2Template(
        manifest_file, trim_spaces=False, **kwargs
    )
    return vm_util.ReadYamlAsDicts(manifest)

  @staticmethod
  def ApplyYaml(
      yaml_dicts: list[dict[str, Any]], **logging_kwargs
  ) -> Iterator[str]:
    """Writes yaml to a file and applies it.

    Args:
      yaml_dicts: A list of YAML documents.
      **logging_kwargs: Keyword arguments passed to file writing.

    Returns:
      Names of the resources, e.g. [deployment.apps/mydeploy, pod/foo]
    """
    vm_util.IncrementStackLevel(**logging_kwargs)
    yaml_file = vm_util.WriteYaml(yaml_dicts, **logging_kwargs)
    return KubernetesClusterCommands._ApplyRenderedManifest(yaml_file)

  @staticmethod
  def WaitForResource(
      resource_name: str,
      condition_name: str,
      namespace: str | None = None,
      timeout: int = vm_util.DEFAULT_TIMEOUT,
      wait_for_all: bool = False,
      condition_type='condition=',
      extra_args: list[str] | None = None,
      **kwargs,
  ):
    """Waits for a condition on a Kubernetes resource (eg: deployment, pod)."""
    run_cmd = [
        'wait',
        f'--for={condition_type}{condition_name}',
        f'--timeout={timeout}s',
        resource_name,
    ]
    if namespace:
      run_cmd.append(f'--namespace={namespace}')
    if wait_for_all:
      run_cmd.append('--all')
    if extra_args:
      run_cmd.extend(extra_args)
    RunKubectlCommand(run_cmd, timeout=timeout + 10, **kwargs)

  @staticmethod
  def WaitForResourceForMultiConditions(
      resource_name: str,
      conditions: Iterable[str],
      namespace: str | None = None,
      timeout: int = vm_util.DEFAULT_TIMEOUT,
      wait_for_all: bool = False,
      extra_args: list[str] | None = None,
  ) -> str:
    """Waits for multiple conditions on a Kubernetes resource (eg: deployment, pod)."""

    def _WrappedWait(condition: str, queue: multiprocessing.Queue):
      try:
        KubernetesClusterCommands.WaitForResource(
            resource_name,
            condition,
            namespace,
            timeout,
            wait_for_all,
            '',
            extra_args,
            suppress_logging=True,
        )
        queue.put((condition, None))
      except Exception as e:  # pylint: disable=broad-except
        # Handle all exceptions and will raise it in main thread.
        queue.put((condition, e))

    queue = multiprocessing.Queue()
    processes = []

    for condition in conditions:
      proc = multiprocessing.Process(
          target=_WrappedWait, args=(condition, queue)
      )
      proc.daemon = True
      proc.start()
      processes.append(proc)

    try:
      # Wait for any one of the conditions to be met or timeout.
      condition, exc = queue.get(timeout=timeout + 1)
      logging.info('%s condition met: %s', resource_name, condition)
    except py_queue.Empty:
      condition = ''
      exc = TimeoutError(
          f'Timed out waiting for conditions on resource {resource_name}:'
          f' {conditions} '
      )

    # Terminate any remaining waiting processes.
    for proc in processes:
      if proc.is_alive():
        proc.terminate()
      proc.join()

    # TODO: cl/783410635 - Consider to raise the exception in child thread as
    # well for easier debugging.
    if exc is not None:
      raise exc
    return condition

  @staticmethod
  def WaitForSucceeded(
      resource_name: str,
      namespace: str | None = None,
      timeout: int = vm_util.DEFAULT_TIMEOUT,
      raise_on_failure: bool = True,
  ) -> tuple[str, str, int]:
    """Waits for a resource to complete (i.e. .status.phase=='Succeeded')."""
    run_cmd = [
        'wait',
        '--for=jsonpath={.status.phase}=Succeeded',
        f'--timeout={timeout}s',
        resource_name,
    ]
    if namespace:
      run_cmd.append(f'--namespace={namespace}')
    return RunKubectlCommand(
        run_cmd, timeout=timeout + 10, raise_on_failure=raise_on_failure
    )

  @staticmethod
  def WaitForRollout(
      resource_name: str, timeout: int = vm_util.DEFAULT_TIMEOUT
  ):
    """Blocks until a Kubernetes rollout is completed."""
    run_cmd = [
        'rollout',
        'status',
        '--timeout=%ds' % timeout,
        resource_name,
    ]

    RunRetryableKubectlCommand(
        run_cmd,
        timeout=timeout,
    )

  @staticmethod
  @vm_util.Retry(retryable_exceptions=(errors.Resource.RetryableCreationError,))
  def GetLoadBalancerIP(service_name: str):
    """Returns the IP address of a LoadBalancer service when ready."""
    get_cmd = [
        'get',
        'service',
        service_name,
        '-o',
        'jsonpath={.status.loadBalancer.ingress[0].ip}',
    ]

    stdout, _, _ = RunKubectlCommand(get_cmd)

    try:
      # Ensure the load balancer is ready by parsing the output IP
      ip_address = ipaddress.ip_address(stdout)
    except ValueError as e:
      raise errors.Resource.RetryableCreationError(
          "Load Balancer IP for service '%s' is not ready." % service_name
      ) from e

    return format(ip_address)

  @staticmethod
  @vm_util.Retry(retryable_exceptions=(errors.Resource.RetryableCreationError,))
  def GetClusterIP(service_name: str) -> str:
    """Returns the IP address of a ClusterIP service when ready."""
    get_cmd = [
        'get',
        'service',
        service_name,
        '-o',
        'jsonpath={.spec.clusterIP}',
    ]

    stdout, _, _ = RunKubectlCommand(get_cmd)

    if not stdout:
      raise errors.Resource.RetryableCreationError(
          "ClusterIP for service '%s' is not ready." % service_name
      )

    return stdout

  @staticmethod
  def GetNumReplicasSamples(
      resource_name: str, namespace: Optional[str] = None
  ) -> list[Sample]:
    """Returns a count of the replias (and state) for the specified resource.

    The number of ready and unready replicas should always sum to the total
    replicas.

    Args:
      resource_name: The deployment/statefulset/etc's name, e.g.
        'deployment/my_deployment'.
      namespace: The namespace of the resource. If omitted, the 'default'
        namespace will be used.

    Returns:
      A list of the (total replicas, ready replicas, unready replicas) for this
      resource (as `Sample`s), or an empty list if the resource cannot be found.
    """
    now = int(time.time())
    if namespace is None:
      namespace = 'default'
    stdout, stderr, retcode = RunKubectlCommand(
        [
            'get',
            resource_name,
            '-n',
            namespace,
            "-o=jsonpath='{.status.replicas}, {.status.readyReplicas}'",
        ],
        raise_on_failure=False,
    )
    if retcode != 0:
      if re.match('^Error from server \\(NotFound\\):.*', stderr) is not None:
        # The specified resource wasn't found
        return []
      else:
        # Some other error.
        raise errors.VmUtil.IssueCommandError(
            f'Unable to query list of replicas: {stderr}'
        )

    stdout = stdout.strip("' ")
    replicas = int(stdout.split(',')[0])
    ready_replicas = int(stdout.split(',')[1])
    unready_replicas = replicas - ready_replicas

    def _Sample(count: int, state: str) -> Sample:
      return Sample(
          metric='k8s/num_replicas_' + state,
          value=count,
          unit='',
          metadata={
              'namespace': namespace,
              'resource_name': resource_name,
          },
          timestamp=now,
      )

    return [
        _Sample(replicas, 'any'),
        _Sample(ready_replicas, 'ready'),
        _Sample(unready_replicas, 'unready'),
    ]

  @staticmethod
  def GetNumNodesSamples() -> list[Sample]:
    """Returns a count of nodes in each state for the cluster.

    The number of ready, unready, and unknown nodes should always sum to the
    total nodes.

    Returns:
      A List of the (total nodes, ready nodes, unready nodes, unknown nodes)
      for this cluster as `Sample`s.
    """
    now = int(time.time())

    jsonpath = (
        '{range .items[*]}'
        '{@.status.conditions[?(@.type=="Ready")].status}{"\\n"}'
        '{end}'
    )
    stdout, _, _ = RunKubectlCommand(
        ['get', 'nodes', f"-o=jsonpath='{jsonpath}'"]
    )

    total = ready = unready = unknown = 0
    for line in stdout.splitlines():
      status = line.strip("' ").lower()
      if not status:
        continue
      elif status == 'true':
        ready += 1
      elif status == 'false':
        unready += 1
      else:
        # status should be strictly 'unknown', but we'll also categorize any
        # other unexpected response as 'unknown'
        unknown += 1
      total += 1

    def _Sample(count: int, state: str) -> Sample:
      # TOCONSIDER: maybe include the nodepool name in the metadata?
      return Sample(
          metric='k8s/num_nodes_' + state,
          value=count,
          unit='',
          metadata={},
          timestamp=now,
      )

    return [
        _Sample(total, 'any'),
        _Sample(ready, 'ready'),
        _Sample(unready, 'unready'),
        _Sample(unknown, 'unknown'),
    ]

  @staticmethod
  def _GetPodNamesForResource(
      resource_name: str, namespace: str = ''
  ) -> list[str]:
    """Gets the names of pods managed by a resource (e.g., deployment).

    The resource must have a .spec.selector.matchLabels defined, and non-empty.

    Args:
      resource_name: the resource type and name, e.g. 'deployment/my_deploy'.
      namespace: The namespace of the resource.

    Raises:
      ValueError: if the resource does not have a (non-empty)
        `.spec.selector.matchLabels` field or if the field cannot be parsed.
    """
    # NB: If this approach is insufficient, then we could convert this to use
    # ownerReferences instead. This would mean going over several hops (i.e.
    # deployment->replicaset->pods) so probably is not worth it unless the
    # current approach prooves inadequate.

    # 1. Get the selector from the resource.
    get_selector_cmd = [
        'get',
        resource_name,
        '-n',
        namespace,
        "-o=jsonpath='{.spec.selector.matchLabels}'",
    ]
    selector_stdout, stderr, retcode = RunKubectlCommand(
        get_selector_cmd, raise_on_failure=False
    )
    if retcode != 0:
      if 'NotFound' in stderr:
        return []
      raise errors.VmUtil.IssueCommandError(
          f'Failed to get selector for resource {resource_name}: {stderr}'
      )

    try:
      # The output can be empty if there are no labels (which violates the
      # pre-condition).
      selector_str = selector_stdout.strip("'")
      if not selector_str:
        raise ValueError(
            'A resource without a .spec.selector.matchLabels was passed to'
            ' _GetPodNamesForResource.'
        )
      selector_dict = json.loads(selector_str)
    except json.JSONDecodeError as e:
      raise ValueError(
          f'Could not decode selector for resource {resource_name}:'
          f' {selector_stdout}'
      ) from e

    if not selector_dict:
      raise ValueError(
          'A resource without a (non-empty) .spec.selector.matchLabels was'
          ' passed to _GetPodNamesForResource.'
      )

    # 2. Construct the label selector string.
    selector_parts = [f'{key}={value}' for key, value in selector_dict.items()]
    label_selector = ','.join(selector_parts)

    # 3. Get pods using the selector.
    get_pods_cmd = [
        'get',
        'pods',
        '-l',
        label_selector,
        '-n',
        namespace,
        '-o=jsonpath={.items[*].metadata.name}',
    ]
    pods_stdout, _, _ = RunKubectlCommand(get_pods_cmd)

    return pods_stdout.strip().split()

  @staticmethod
  def GetCPURequestSamples(
      resource_name: str, namespace: str = ''
  ) -> list[Sample]:
    """Returns the CPU requests for all pods within the specified resource.

    Args:
      resource_name: The deployment/statefulset/etc's name, e.g.
        'deployment/my_deployment'.
      namespace: The namespace of the resource. If omitted, the 'default'
        namespace will be used.

    Returns:
      A list of Samples, each representing the CPU request of a pod.
    """
    now = int(time.time())

    pod_names = KubernetesClusterCommands._GetPodNamesForResource(
        resource_name, namespace
    )
    samples = []

    for pod_name in pod_names:
      # Get CPU requests for each pod
      cpu_request_stdout, _, _ = RunKubectlCommand(
          [
              'get',
              'pod',
              pod_name,
              '-n',
              namespace,
              '-o=jsonpath={.spec.containers[*].resources.requests.cpu}',
          ],
      )

      # Convert CPU string (e.g., "100m", "1") to float (cores)
      cpu_request_str = cpu_request_stdout.strip()
      if not cpu_request_str:
        continue
      if cpu_request_str.endswith('m'):
        cpu_request = float(cpu_request_str[:-1]) / 1000
      else:
        cpu_request = float(cpu_request_str)

      samples.append(
          Sample(
              metric='kubernetes_cpu_request',
              value=cpu_request,
              unit='cores',
              metadata={
                  'namespace': namespace,
                  'resource_name': resource_name,
                  'pod': pod_name,
              },
              timestamp=now,
          )
      )
    return samples

  @staticmethod
  def GetCPUUsageSamples(
      resource_name: str, namespace: str = ''
  ) -> list[Sample]:
    """Returns the CPU usage for all pods within the specified resource.

    Args:
      resource_name: The deployment/statefulset/etc's name, e.g.
        'deployment/my_deployment'.
      namespace: The namespace of the resource. If omitted, the 'default'
        namespace will be used.

    Returns:
      A list of Samples, each representing the CPU usage of a pod.
    """
    now = int(time.time())

    pod_names = KubernetesClusterCommands._GetPodNamesForResource(
        resource_name, namespace
    )
    samples = []

    for pod_name in pod_names:
      # Get CPU usage for each pod using kubectl top
      # kubectl top pod <pod-name> --namespace <namespace> --containers
      # This returns output like:
      # POD       NAME   CPU(cores)   MEMORY(bytes)
      # fib-xyz   fib    10m          20Mi
      top_output, stderr, retcode = RunKubectlCommand(
          ['top', 'pod', pod_name, '--namespace', namespace, '--containers'],
          raise_on_failure=False,
      )
      if retcode != 0:
        logging.warning(
            'Could not get CPU usage for pod %s: %s', pod_name, stderr
        )
        continue

      # Parse the output to get CPU usage
      # Skip header and split lines
      lines = top_output.strip().split('\n')[1:]
      for line in lines:
        parts = line.split()
        if len(parts) < 4:
          raise errors.VmUtil.IssueCommandError(
              f'Unexpected output line from kubectl top: {line}'
          )

        cpu_usage_str = parts[2]
        if cpu_usage_str.endswith('m'):
          cpu_usage = float(cpu_usage_str[:-1]) / 1000
        else:
          cpu_usage = float(cpu_usage_str)

        samples.append(
            Sample(
                metric='kubernetes_cpu_usage',
                value=cpu_usage,
                unit='cores',
                metadata={
                    'namespace': namespace,
                    'resource_name': resource_name,
                    'pod': pod_name,
                    'container': parts[1],  # Container name
                },
                timestamp=now,
            )
        )
    return samples

  @staticmethod
  def CreateConfigMap(name: str, from_file_dir: str):
    """Creates a Kubernetes ConfigMap.

    Args:
      name: The name of the ConfigMap to create
      from_file_dir: The directory name containing files that will be key/values
        in the ConfigMap
    """
    RunKubectlCommand(
        ['create', 'configmap', name, '--from-file', from_file_dir]
    )

  @staticmethod
  def CreateServiceAccount(
      name: str, clusterrole: str | None = None, namespace='default'
  ):
    """Create a k8s service account and cluster-role-binding."""
    RunKubectlCommand(
        ['create', 'serviceaccount', name, '--namespace', namespace]
    )
    if clusterrole:
      # TODO(pclay): Support customer cluster roles?
      RunKubectlCommand([
          'create',
          'clusterrolebinding',
          f'{name}-role',
          f'--clusterrole={clusterrole}',
          f'--serviceaccount={namespace}:{name}',
          '--namespace',
          namespace,
      ])

  @property
  @functools.lru_cache(maxsize=1)
  def k8s_version(self) -> str:
    """Actual Kubernetes version reported by server."""
    stdout, _, _ = RunKubectlCommand(['version', '-o', 'yaml'])
    return yaml.safe_load(stdout)['serverVersion']['gitVersion']

  @staticmethod
  def GetPodLabel(resource_name):
    run_cmd = [
        'get',
        resource_name,
        '-o',
        'jsonpath="{.spec.selector.matchLabels.app}"',
    ]

    stdout, _, _ = RunKubectlCommand(run_cmd)
    return yaml.safe_load(stdout)

  @staticmethod
  def GetPodIpsByLabel(pod_label_key, pod_label_value) -> list[str]:
    """Returns a list of internal IPs for pod label key-value.

    Args:
      pod_label_key: The pod label name
      pod_label_value: The pod label value
    """
    get_cmd = [
        'get',
        'pods',
        '-l',
        f'{pod_label_key}={pod_label_value}',
        '-o',
        'jsonpath="{.items[*].status.podIP}"',
    ]

    stdout, _, _ = RunKubectlCommand(get_cmd)
    return yaml.safe_load(stdout).split()

  @staticmethod
  def GetPodIps(resource_name) -> list[str]:
    """Returns a list of internal IPs for a pod name.

    Args:
      resource_name: The pod resource name
    """
    pod_label = KubernetesClusterCommands.GetPodLabel(resource_name)
    return KubernetesClusterCommands.GetPodIpsByLabel('app', pod_label)

  @staticmethod
  def GetPodNames() -> list[str]:
    """Returns all pod names in the cluster."""
    return KubernetesClusterCommands.GetAllNamesForResourceType('pods')

  @staticmethod
  def GetNodeNames() -> list[str]:
    """Get the node names for the cluster."""
    return KubernetesClusterCommands.GetAllNamesForResourceType('nodes')

  @staticmethod
  def GetAllNamesForResourceType(resource_type: str) -> list[str]:
    """Get all names for the specified resource. Type should be plural."""
    stdout, _, _ = RunKubectlCommand(
        ['get', resource_type, '-o', 'jsonpath={.items[*].metadata.name}']
    )
    return stdout.split()

  @staticmethod
  def RunKubectlExec(pod_name, cmd):
    run_cmd = ['exec', '-it', pod_name, '--'] + cmd
    RunKubectlCommand(run_cmd)

  @staticmethod
  def _GetPvcs() -> Sequence[Any]:
    stdout, _, _ = RunKubectlCommand(['get', 'pvc', '-o', 'yaml'])
    return yaml.safe_load(stdout)['items']

  @staticmethod
  def _GetEvents(**kwargs) -> set['KubernetesEvent']:
    """Get the events for the cluster."""
    stdout, _, _ = RunRetryableKubectlCommand(
        ['get', 'events', '-o', 'yaml'], **kwargs
    )
    k8s_events = set()
    for item in yaml.safe_load(stdout)['items']:
      k8s_event = KubernetesEvent.FromDict(item)
      if k8s_event:
        k8s_events.add(k8s_event)
    return k8s_events

  @staticmethod
  def GetResourceMetadataByName(
      resource_name: str,
      resource_label: Optional[str] = None,
      output_format: str = 'yaml',
      output_formatter=yaml.safe_load,
      **kwargs,
  ) -> Dict[str, Any]:
    """Gets the resource metadata from a Kubernetes resource."""
    get_cmd = [
        'get',
        resource_name,
    ]
    if resource_label:
      get_cmd.extend(['-l', resource_label])
    get_cmd.extend(['-o', output_format])

    stdout, _, _ = RunRetryableKubectlCommand(get_cmd, **kwargs)

    return output_formatter(stdout)

  @staticmethod
  def RetryableGetPodNameFromJob(
      job_name: str, timeout: int | None = None
  ) -> str:
    """Get the name of the pod from a job, retry until the pod is created.

    Args:
      job_name: The name of the job.
      timeout: The timeout in seconds.

    Returns:
      The name of the pod.
    """

    class EmptyPodNameError(Exception):
      pass

    @vm_util.Retry(timeout=timeout, retryable_exceptions=EmptyPodNameError)
    def _RetryFunction():
      pod_name = KubernetesClusterCommands.GetResourceMetadataByName(
          'pods', f'job-name={job_name}', 'jsonpath={.items[*].metadata.name}'
      )
      if not pod_name:
        raise EmptyPodNameError(
            f'No pod found for job {job_name}, the pod may not be created yet.'
        )
      return pod_name

    return _RetryFunction()

  @staticmethod
  def DeleteResource(
      resource_identifier: str, ignore_not_found: bool = True, **kwargs
  ) -> None:
    """Deletes a kubernetes resource."""
    delete_cmd = [
        'delete',
        resource_identifier,
    ]
    if ignore_not_found:
      delete_cmd.append('--ignore-not-found=true')
    RunKubectlCommand(delete_cmd, raise_on_failure=False, **kwargs)

  @staticmethod
  def GetFileContentFromPod(pod_name: str, file_path: str) -> str:
    """Get the content of a file from a pod.

    Args:
      pod_name: The name of the pod.
      file_path: The path of the file to get.

    Returns:
      The content of the file.
    """
    get_cmd = [
        'exec',
        pod_name,
        '--',
        'cat',
        file_path,
    ]

    stdout, _, _ = RunRetryableKubectlCommand(get_cmd)
    return stdout

  @staticmethod
  def CopyFilesFromPod(
      pod_name: str, src_path: str, target_path: str, **kwargs
  ) -> None:
    """Copy files from a pod source path to the target path on current VM."""
    get_cmd = [
        'cp',
        f'{pod_name}:{src_path}',
        target_path,
    ]

    RunRetryableKubectlCommand(get_cmd, **kwargs)


class KubernetesEventPoller:
  """Wrapper which polls for Kubernetes events."""

  def __init__(self, get_events_fn: Callable[[], set['KubernetesEvent']]):
    self.get_events_fn = get_events_fn
    self.polled_events: set['KubernetesEvent'] = set()
    self.stop_polling = multiprocessing.Event()
    self.event_queue: multiprocessing.Queue = multiprocessing.Queue()
    self.event_poller = multiprocessing.Process(
        target=self._PollForEvents,
        args=((
            self.event_queue,
            self.stop_polling,
        )),
    )
    self.event_poller.daemon = True

  def _PollForEvents(
      self,
      queue: multiprocessing.Queue,
      stop_polling: synchronize.Event,
  ):
    """Polls for events & (ideally asynchronously) waits to poll again.

    Results are appended to the queue. Timeouts are ignored.

    Args:
      queue: The queue to append events to.
      stop_polling: Stop polling when set.
    """
    while True:
      try:
        k8s_events = self.get_events_fn()
        logging.info(
            'From async get events process, got %s events', len(k8s_events)
        )
        for event in k8s_events:
          queue.put(event)
      except errors.VmUtil.IssueCommandTimeoutError:
        logging.info(
            'Async get events command timed out. This may result in missing'
            ' events, but is not a reason to fail the benchmark.'
        )
        pass
      start_sleep_time = time.time()
      while time.time() - start_sleep_time < 60 * 40:
        time.sleep(1)
        if stop_polling.is_set():
          return

  def StartPolling(self):
    """Starts polling for events."""
    self.event_poller.start()

    # Stop polling events even if the resource is not deleted.
    def _StopPollingConnected(unused1, **kwargs):
      del unused1, kwargs
      self.StopPolling()

    events.benchmark_end.connect(_StopPollingConnected, weak=False)

  def StopPolling(self):
    """Stops polling for events, joining the poller process."""
    logging.info('Stopping event poller')
    self.stop_polling.set()
    while not self.event_queue.empty():
      self.polled_events.add(self.event_queue.get())
    if self.event_poller.is_alive():
      self.event_poller.join(timeout=30)
    if self.event_poller.is_alive():
      logging.warning(
          'Event poller process did not join in 30 seconds; killing it.'
      )
      self.event_poller.kill()
      self.event_poller.join(timeout=30)

  def GetEvents(self) -> set['KubernetesEvent']:
    """Gets the events for the cluster, including previously polled events."""
    k8s_events = self.get_events_fn()
    self.polled_events.update(k8s_events)
    while not self.event_queue.empty():
      self.polled_events.add(self.event_queue.get())
    return self.polled_events


class KubernetesCluster(
    container_service.BaseContainerCluster, KubernetesClusterCommands
):
  """A Kubernetes flavor of Container Cluster."""

  CLUSTER_TYPE = container_service.KUBERNETES

  def __init__(self, cluster_spec: container_spec_lib.ContainerClusterSpec):
    super().__init__(cluster_spec)
    self.event_poller: KubernetesEventPoller | None = None
    if cluster_spec.poll_for_events:

      def _GetEventsNoLogging():
        return self._GetEvents(suppress_logging=True)

      self.event_poller = KubernetesEventPoller(_GetEventsNoLogging)

    self.inference_server = (
        kubernetes_inference_server.GetKubernetesInferenceServer(
            cluster_spec.inference_server, self
        )
    )

  def Create(self, restore: bool = False) -> None:
    super().Create(restore)
    if self.inference_server:
      self.inference_server.Create()

  def _PostCreate(self):
    super()._PostCreate()
    if self.event_poller:
      self.event_poller.StartPolling()

  def Delete(self, freeze: bool = False) -> None:
    if self.inference_server:
      self.inference_server.Delete()
    super().Delete(freeze)

  def _PreDelete(self):
    self._DeleteAllFromDefaultNamespace()

  def _Delete(self):
    if self.event_poller:
      self.event_poller.StopPolling()
    self._DeleteAllFromDefaultNamespace()

  def GetEvents(self) -> set['KubernetesEvent']:
    """Gets the events for the cluster, including previously polled events."""
    if self.event_poller:
      return self.event_poller.GetEvents()
    return self._GetEvents()

  def __getstate__(self):
    state = self.__dict__.copy()
    if 'event_poller' in state:
      del state['event_poller']
    return state

  def GetResourceMetadata(self):
    """Returns a dict containing metadata about the cluster."""
    result = super().GetResourceMetadata()
    if self.created:
      result['version'] = self.k8s_version
    return result

  def DeployContainer(
      self, name: str, container_spec: container_spec_lib.ContainerSpec
  ):
    """Deploys Containers according to the ContainerSpec."""
    base_name = name
    name = base_name + str(len(self.containers[base_name]))
    container = KubernetesContainer(container_spec=container_spec, name=name)
    self.containers[base_name].append(container)
    container.Create()

  def DeployContainerService(
      self, name: str, container_spec: container_spec_lib.ContainerSpec
  ):
    """Deploys a ContainerSerivice according to the ContainerSpec."""
    service = KubernetesContainerService(container_spec, name)
    self.services[name] = service
    service.Create()

  # TODO(pclay): Move to cached property in Python 3.9
  @property
  @functools.lru_cache(maxsize=1)
  def node_memory_allocatable(self) -> units.Quantity:
    """Usable memory of each node in cluster in KiB."""
    stdout, _, _ = RunKubectlCommand(
        # TODO(pclay): Take a minimum of all nodes?
        ['get', 'nodes', '-o', 'jsonpath={.items[0].status.allocatable.memory}']
    )
    return units.ParseExpression(stdout)

  @property
  @functools.lru_cache(maxsize=1)
  def node_num_cpu(self) -> int:
    """vCPU of each node in cluster."""
    stdout, _, _ = RunKubectlCommand(
        ['get', 'nodes', '-o', 'jsonpath={.items[0].status.capacity.cpu}']
    )
    return int(stdout)

  def LabelDisks(self):
    """Propagate cluster labels to disks if not done by cloud provider."""
    pass

  # TODO(pclay): integrate with kubernetes_disk.
  def GetDefaultStorageClass(self) -> str:
    """Get the default storage class for the provider."""
    raise NotImplementedError

  def GetNodeSelectors(self, machine_type: str | None = None) -> dict[str, str]:
    """Get the node selectors section of a yaml for the provider."""
    return {}

  def ModifyPodSpecPlacementYaml(
      self,
      yaml_dicts: list[dict[str, Any]],
      name: str,
      machine_type: str | None = None,
  ) -> None:
    """Modifies the pod spec yaml in-place with additional needed attributes.

    The placement of pods (with eg node selectors or topology constraints) is
    the most likely to change from cloud to cloud.

    Args:
      yaml_dicts: The list of yaml dicts to search through & modify. See
        https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#podspec-v1-core
          for documentation on the pod spec fields. This is modified in place.
      name: The name of the app.
      machine_type: A specified machine type to request.
    """
    modified = False
    for yaml_dict in yaml_dicts:
      if yaml_dict['spec']['template']['spec']:
        self._ModifyPodSpecPlacementYaml(
            yaml_dict['spec']['template']['spec'], name, machine_type
        )
        modified = True
    if not modified:
      raise ValueError(
          'No pod spec yaml found to modify. Was the wrong jinja passed in?'
      )

  def _ModifyPodSpecPlacementYaml(
      self,
      pod_spec_yaml: dict[str, Any],
      name: str,
      machine_type: str | None = None,
  ) -> None:
    """Modifies the pod spec yaml in-place with additional needed attributes.

    The placement of pods (with eg node selectors or topology constraints) is
    the most likely to change from cloud to cloud.

    Args:
      pod_spec_yaml: The pod spec yaml to modify. See
        https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#podspec-v1-core
          for documentation on the pod spec fields. This is modified in place.
      name: The name of the app.
      machine_type: A specified machine type to request.
    """
    del name
    node_selectors = self.GetNodeSelectors(machine_type)
    if node_selectors:
      pod_spec_yaml['nodeSelector'].update(node_selectors)

  def DeployIngress(
      self, name: str, namespace: str, port: int, health_path: str = ''
  ) -> str:
    """Deploys an Ingress/load balancer resource to the cluster.

    Args:
      name: The name of the Ingress resource.
      namespace: The namespace of the resource.
      port: The port to expose to the internet.
      health_path: The path to use for health checks.

    Returns:
      The address of the Ingress.
    """
    del health_path
    self.ApplyManifest(
        'container/loadbalancer.yaml.j2',
        name=name,
        namespace=namespace,
        port=port,
    )
    self.WaitForResource(
        f'service/{name}',
        INGRESS_JSONPATH,
        namespace=namespace,
        condition_type='jsonpath=',
    )
    stdout, _, _ = RunKubectlCommand([
        'get',
        '-n',
        name,
        f'svc/{name}',
        '-o',
        f'jsonpath={INGRESS_JSONPATH}',
    ])
    return f'{self._GetAddressFromIngress(stdout)}:{port}'

  def _GetAddressFromIngress(self, ingress_out: str):
    """Gets the endpoint address from the Ingress resource."""
    ingress = json.loads(ingress_out.strip("'"))
    if 'ip' in ingress:
      ip = ingress['ip']
    elif 'hostname' in ingress:
      ip = ingress['hostname']
    else:
      raise errors.Benchmarks.RunError(
          'No IP or hostname found in ingress from stdout ' + ingress_out
      )
    return 'http://' + ip.strip()

  def AddNodepool(self, batch_name: str, pool_id: str):
    """Adds an additional nodepool with the given name to the cluster."""
    pass


@dataclasses.dataclass(eq=True, frozen=True)
class KubernetesEventResource:
  """Holder for Kubernetes event involved objects."""

  kind: str
  name: str | None

  @classmethod
  def FromDict(cls, yaml_data: dict[str, Any]) -> 'KubernetesEventResource':
    """Parse Kubernetes Event YAML output."""
    return cls(kind=yaml_data['kind'], name=yaml_data.get('name'))


@dataclasses.dataclass(eq=True, frozen=True)
class KubernetesEvent:
  """Holder for Kubernetes event data."""

  resource: KubernetesEventResource
  message: str
  # Reason is actually more of a machine readable message.
  reason: str | None
  # Examples: Normal, Warning, Error
  type: str
  timestamp: float

  @classmethod
  def FromDict(cls, yaml_data: dict[str, Any]) -> 'KubernetesEvent | None':
    """Parse Kubernetes Event YAML output."""
    if 'message' not in yaml_data:
      return None
    try:
      # There are multiple timestamps. They should be equivalent.
      raw_timestamp = yaml_data.get('lastTimestamp') or yaml_data.get(
          'eventTime'
      )
      assert raw_timestamp
      # Python 3.10 cannot handle Z as utc in ISO 8601 timestamps
      python_3_10_compatible_timestamp = re.sub('Z$', '+00:00', raw_timestamp)
      timestamp = calendar.timegm(
          datetime.datetime.fromisoformat(
              python_3_10_compatible_timestamp
          ).timetuple()
      )
      return cls(
          message=yaml_data['message'],
          reason=yaml_data.get('reason'),
          resource=KubernetesEventResource.FromDict(
              yaml_data['involvedObject']
          ),
          type=yaml_data['type'],
          timestamp=timestamp,
      )
    except (AssertionError, KeyError) as e:
      logging.exception(
          'Tried parsing event: %s but ran into error: %s', yaml_data, e
      )
      return None


@events.benchmark_start.connect
def _SetKubeConfig(unused_sender, benchmark_spec: BenchmarkSpec):
  """Sets the value for the kubeconfig flag if it's unspecified."""
  if not flags.FLAGS.kubeconfig:
    flags.FLAGS.kubeconfig = vm_util.PrependTempDir(
        'kubeconfig' + str(benchmark_spec.sequence_number)
    )
    # Store the value for subsequent run stages.
    benchmark_spec.config.flags['kubeconfig'] = flags.FLAGS.kubeconfig

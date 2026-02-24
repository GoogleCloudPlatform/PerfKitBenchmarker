"""Implementation of many k8s commands.

All methods just call generic kubectl commands without needing instance
information.
"""

import ipaddress
import json
import logging
import multiprocessing
import queue as py_queue
import re
import time
from typing import Any, Dict, Iterable, Iterator, Optional, Sequence
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_events
from perfkitbenchmarker.sample import Sample
import yaml


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
    return _ApplyRenderedManifest(filename)

  manifest = vm_util.ReadAndRenderJinja2Template(
      manifest_file, trim_spaces=False, **kwargs
  )
  return _WriteAndApplyManifest(manifest, should_log_file)


def _ApplyRenderedManifest(manifest_file: str) -> Iterator[str]:
  """Applies a rendered Kubernetes manifest & returns resources created.

  Args:
    manifest_file: The full path of the YAML file.

  Returns:
    Names of the resources, e.g. [deployment.apps/mydeploy, pod/foo]
  """
  out, _, _ = kubectl.RunKubectlCommand(['apply', '-f', manifest_file])

  def _ParseApplyOutput(stdout: str) -> Iterator[str]:
    """Parses the output of kubectl apply to get the name of the resource."""
    # Example input: deployment.apps/pkb123 created
    for line in stdout.splitlines():
      match = re.search(r'([^\s/]+/[^\s/]+) (created|configured)', line)
      if match:
        yield match.group(1)

  # Inner function needed to run kubectl command even if output is unused.
  return _ParseApplyOutput(out)


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
    resource_names = _ApplyRenderedManifest(rendered_template.name)
    return resource_names


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
  return _ApplyRenderedManifest(yaml_file)


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
  kubectl.RunKubectlCommand(run_cmd, timeout=timeout + 10, **kwargs)


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
      WaitForResource(
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
    proc = multiprocessing.Process(target=_WrappedWait, args=(condition, queue))
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
  return kubectl.RunKubectlCommand(
      run_cmd, timeout=timeout + 10, raise_on_failure=raise_on_failure
  )


def WaitForRollout(resource_name: str, timeout: int = vm_util.DEFAULT_TIMEOUT):
  """Blocks until a Kubernetes rollout is completed."""
  run_cmd = [
      'rollout',
      'status',
      '--timeout=%ds' % timeout,
      resource_name,
  ]

  kubectl.RunRetryableKubectlCommand(
      run_cmd,
      timeout=timeout,
  )


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

  stdout, _, _ = kubectl.RunKubectlCommand(get_cmd)

  try:
    # Ensure the load balancer is ready by parsing the output IP
    ip_address = ipaddress.ip_address(stdout)
  except ValueError as e:
    raise errors.Resource.RetryableCreationError(
        "Load Balancer IP for service '%s' is not ready." % service_name
    ) from e

  return format(ip_address)


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

  stdout, _, _ = kubectl.RunKubectlCommand(get_cmd)

  if not stdout:
    raise errors.Resource.RetryableCreationError(
        "ClusterIP for service '%s' is not ready." % service_name
    )

  return stdout


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
  stdout, stderr, retcode = kubectl.RunKubectlCommand(
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
  stdout, _, _ = kubectl.RunKubectlCommand(
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


def _GetPodNamesForResource(
    resource_name: str, namespace: str = ''
) -> list[str]:
  """Gets the names of pods managed by a resource (e.g., deployment).

  The resource must have a .spec.selector.matchLabels defined, and non-empty.

  Args:
    resource_name: the resource type and name, e.g. 'deployment/my_deploy'.
    namespace: The namespace of the resource.

  Returns:
    The names of pods managed by a resource (e.g., deployment).

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
  selector_stdout, stderr, retcode = kubectl.RunKubectlCommand(
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
  pods_stdout, _, _ = kubectl.RunKubectlCommand(get_pods_cmd)

  return pods_stdout.strip().split()


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

  pod_names = _GetPodNamesForResource(resource_name, namespace)
  samples = []

  for pod_name in pod_names:
    # Get CPU requests for each pod
    cpu_request_stdout, _, _ = kubectl.RunKubectlCommand(
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


def GetCPUUsageSamples(resource_name: str, namespace: str = '') -> list[Sample]:
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

  pod_names = _GetPodNamesForResource(resource_name, namespace)
  samples = []

  for pod_name in pod_names:
    # Get CPU usage for each pod using kubectl top
    # kubectl top pod <pod-name> --namespace <namespace> --containers
    # This returns output like:
    # POD       NAME   CPU(cores)   MEMORY(bytes)
    # fib-xyz   fib    10m          20Mi
    top_output, stderr, retcode = kubectl.RunKubectlCommand(
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


def CreateConfigMap(name: str, from_file_dir: str):
  """Creates a Kubernetes ConfigMap.

  Args:
    name: The name of the ConfigMap to create
    from_file_dir: The directory name containing files that will be key/values
      in the ConfigMap
  """
  kubectl.RunKubectlCommand(
      ['create', 'configmap', name, '--from-file', from_file_dir]
  )


def CreateServiceAccount(
    name: str, clusterrole: str | None = None, namespace='default'
):
  """Create a k8s service account and cluster-role-binding."""
  kubectl.RunKubectlCommand(
      ['create', 'serviceaccount', name, '--namespace', namespace]
  )
  if clusterrole:
    # TODO(pclay): Support customer cluster roles?
    kubectl.RunKubectlCommand([
        'create',
        'clusterrolebinding',
        f'{name}-role',
        f'--clusterrole={clusterrole}',
        f'--serviceaccount={namespace}:{name}',
        '--namespace',
        namespace,
    ])


def GetK8sVersion() -> str:
  """Actual Kubernetes version reported by server."""
  stdout, _, _ = kubectl.RunKubectlCommand(['version', '-o', 'yaml'])
  return yaml.safe_load(stdout)['serverVersion']['gitVersion']


def GetPodLabel(resource_name):
  run_cmd = [
      'get',
      resource_name,
      '-o',
      'jsonpath="{.spec.selector.matchLabels.app}"',
  ]

  stdout, _, _ = kubectl.RunKubectlCommand(run_cmd)
  return yaml.safe_load(stdout)


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

  stdout, _, _ = kubectl.RunKubectlCommand(get_cmd)
  return yaml.safe_load(stdout).split()


def GetPodIps(resource_name) -> list[str]:
  """Returns a list of internal IPs for a pod name.

  Args:
    resource_name: The pod resource name
  """
  pod_label = GetPodLabel(resource_name)
  return GetPodIpsByLabel('app', pod_label)


def GetPodNames() -> list[str]:
  """Returns all pod names in the cluster."""
  return GetAllNamesForResourceType('pods')


def GetNodeNames(suppress_logging: bool = False) -> list[str]:
  """Get the node names for the cluster."""
  return GetAllNamesForResourceType('nodes', suppress_logging=suppress_logging)


def GetAllNamesForResourceType(
    resource_type: str,
    suppress_logging: bool = False,
) -> list[str]:
  """Get all names for the specified resource. Type should be plural."""
  stdout, _, _ = kubectl.RunKubectlCommand(
      ['get', resource_type, '-o', 'jsonpath={.items[*].metadata.name}'],
      suppress_logging=suppress_logging,
  )
  return stdout.split()


def RunKubectlExec(pod_name, cmd):
  run_cmd = ['exec', '-it', pod_name, '--'] + cmd
  kubectl.RunKubectlCommand(run_cmd)


def GetPvcs() -> Sequence[Any]:
  stdout, _, _ = kubectl.RunKubectlCommand(['get', 'pvc', '-o', 'yaml'])
  return yaml.safe_load(stdout)['items']


def GetEvents(**kwargs) -> set['kubernetes_events.KubernetesEvent']:
  """Get the events for the cluster."""
  stdout, _, _ = kubectl.RunRetryableKubectlCommand(
      ['get', 'events', '-o', 'yaml'], **kwargs
  )
  k8s_events = set()
  for item in yaml.safe_load(stdout)['items']:
    k8s_event = kubernetes_events.KubernetesEvent.FromDict(item)
    if k8s_event:
      k8s_events.add(k8s_event)
  return k8s_events


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

  stdout, _, _ = kubectl.RunRetryableKubectlCommand(get_cmd, **kwargs)

  return output_formatter(stdout)


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
    pod_name = GetResourceMetadataByName(
        'pods', f'job-name={job_name}', 'jsonpath={.items[*].metadata.name}'
    )
    if not pod_name:
      raise EmptyPodNameError(
          f'No pod found for job {job_name}, the pod may not be created yet.'
      )
    return pod_name

  return _RetryFunction()


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
  kubectl.RunKubectlCommand(delete_cmd, raise_on_failure=False, **kwargs)


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

  stdout, _, _ = kubectl.RunRetryableKubectlCommand(get_cmd)
  return stdout


def CopyFilesFromPod(
    pod_name: str, src_path: str, target_path: str, **kwargs
) -> None:
  """Copy files from a pod source path to the target path on current VM."""
  get_cmd = [
      'cp',
      f'{pod_name}:{src_path}',
      target_path,
  ]

  kubectl.RunRetryableKubectlCommand(get_cmd, **kwargs)

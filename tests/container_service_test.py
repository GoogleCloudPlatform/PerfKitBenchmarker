import os
import tempfile
import time
from typing import Callable, Iterable, Protocol, Tuple
import unittest
from unittest import mock
from absl.testing import parameterized
from perfkitbenchmarker import container_service
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import container_spec
from perfkitbenchmarker.sample import Sample
from tests import container_service_mock
from tests import pkb_common_test_case


kubectl_timeout_tuple = (
    '',
    (
        'Unable to connect to the server: dial tcp 10.42.42.42:443:'
        'connect: connection timed out'
    ),
    1,
)

_ELECTION_EVENT_NO_NAME = """
apiVersion: v1
items:
- apiVersion: v1
  count: 1
  eventTime: null
  firstTimestamp: "2025-02-10T17:42:18Z"
  involvedObject:
    apiVersion: v1
    kind: ConfigMap
  kind: Event
  lastTimestamp: "2025-02-10T17:42:18Z"
  message: gke-49fe-vm became leader
  metadata:
    creationTimestamp: "2025-02-10T17:42:18Z"
    name: .1822e9ada6eadb76
    namespace: default
    resourceVersion: "96"
    uid: 87ed48a4-109b-4c9f-8335-81b24e9d9bfa
  reason: LeaderElection
  reportingComponent: ""
  reportingInstance: ""
  source:
    component: kubestore
  type: Normal
kind: List
metadata:
  resourceVersion: ""
"""


class _IssueCommandCallable(Protocol):

  def __call__(
      self,
      cmd: Iterable[str],
      suppress_failure: Callable[[str, str, int], bool] | None = None,
      **kwargs,
  ) -> Tuple[str, str, int]:
    ...


def _MockedIssueCommandSuppressing(
    stderr: str,
) -> _IssueCommandCallable:
  def _MockedCommand(
      cmd: Iterable[str],
      suppress_failure: Callable[[str, str, int], bool] | None = None,
      **kwargs,
  ):
    _ = cmd
    _ = kwargs
    stdout = ''
    status = 1
    if suppress_failure and suppress_failure(stdout, stderr, status):
      return stdout, '', 0
    return stdout, stderr, status

  return _MockedCommand


def _MockedIssueCommandFailure(
    cmd: Iterable[str],
    suppress_failure: Callable[[str, str, int], bool] | None = None,
    **kwargs,
) -> Tuple[str, str, int]:
  return _MockedIssueCommandSuppressing(
      stderr='A failure occurred',
  )(
      cmd,
      suppress_failure=suppress_failure,
      **kwargs,
  )


class ContainerServiceTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    container_service_mock.MockContainerInit(self)
    self.kubernetes_cluster = (
        container_service_mock.CreateTestKubernetesCluster()
    )

  @parameterized.parameters('created', 'configured')
  def test_apply_manifest_gets_deployment_name(self, suffix):
    self.MockIssueCommand(
        {'apply -f': [(f'deployment.apps/test-deployment {suffix}', '', 0)]}
    )
    self.enter_context(
        mock.patch.object(
            data,
            'ResourcePath',
            return_value='path/to/test-deployment.yaml',
        )
    )
    deploy_ids = self.kubernetes_cluster.ApplyManifest(
        'test-deployment.yaml',
    )
    self.assertEqual(next(deploy_ids), 'deployment.apps/test-deployment')

  def test_apply_manifest_logs_jinja(self):
    self.MockIssueCommand(
        {'apply -f': [('deployment.apps/test-deployment hello', '', 0)]}
    )
    self.enter_context(
        mock.patch.object(
            data,
            'ResourcePath',
            return_value=os.path.join(
                os.path.dirname(__file__), 'data', 'kube_apply.yaml.j2'
            ),
        )
    )
    with self.assertLogs(level='INFO') as logs:
      self.kubernetes_cluster.ApplyManifest(
          'tests/data/kube_apply.yaml.j2',
          should_log_file=True,
          name='hello-world',
          command=['echo', 'hello', 'world'],
      )
    # Asserting on logging isn't very important, but is easier than reading the
    # written file.
    full_logs = ';'.join(logs.output)
    self.assertIn('name: hello-world', full_logs)
    self.assertIn('echo', full_logs)

  def test_apply_manifest_yaml_logs(self):
    self.MockIssueCommand(
        {'apply -f': [('deployment.apps/test-deployment hello', '', 0)]}
    )
    self.enter_context(
        mock.patch.object(
            data,
            'ResourcePath',
            return_value=os.path.join(
                os.path.dirname(__file__), 'data', 'kube_apply.yaml.j2'
            ),
        )
    )
    self.enter_context(
        mock.patch.object(
            vm_util,
            'GetTempDir',
            return_value=tempfile.gettempdir(),
        )
    )
    with self.assertLogs(level='INFO') as logs:
      yamls = self.kubernetes_cluster.ConvertManifestToYamlDicts(
          'tests/data/kube_apply.yaml.j2',
          name='hello-world',
          command=[],
      )
      self.assertEqual(yamls[0]['kind'], 'Namespace')
      yamls[1]['spec']['selector']['app'] = 'hi-world'
      yamls[1]['spec']['template']['spec']['containers'].append(
          {'name': 'second-container'}
      )
      self.kubernetes_cluster.ApplyYaml(
          yamls,
          should_log_file=True,
      )
    full_logs = ';'.join(logs.output)
    self.assertIn('app: hi-world', full_logs)
    self.assertIn('name: hello-world', full_logs)
    self.assertIn('name: second-container', full_logs)
    # Check for no python artifacts.
    self.assertNotIn('dict', full_logs)
    self.assertNotIn('null', full_logs)

  @mock.patch.object(
      vm_util,
      'IssueCommand',
      side_effect=[errors.VmUtil.IssueCommandError()],
      autospec=True,
  )
  def test_retriable_kubectl_command_fails_on_random_error(self, _):
    with self.assertRaises(errors.VmUtil.IssueCommandError):
      container_service.RunRetryableKubectlCommand(['get', 'podpatchwork'])

  @mock.patch.object(
      vm_util,
      'IssueCommand',
      side_effect=[
          errors.VmUtil.IssueCommandTimeoutError(),
          ('pod1, pod2', '', 0),
      ],
      autospec=True,
  )
  @mock.patch.object(time, 'sleep', autospec=True)
  def test_retriable_kubectl_command_retries_on_retriable_error(
      self, sleep_mock, issue_command_mock
  ):
    out, err, ret = container_service.RunRetryableKubectlCommand(
        ['get', 'pods']
    )
    self.assertEqual(out, 'pod1, pod2')
    self.assertEqual(err, '')
    self.assertEqual(ret, 0)

  def test_retriable_kubectl_command_passes_timeout_through(self):
    def _VerifyTimeout(
        cmd: Iterable[str],
        timeout: int | None = vm_util.DEFAULT_TIMEOUT,
        **kwargs,
    ) -> Tuple[str, str, int]:
      _ = cmd
      _ = kwargs
      self.assertEqual(
          timeout,
          1,
          'timeout not correctly passed to underlying vm_util.IssueCommand()',
      )
      return 'ok', '', 0

    with mock.patch.object(vm_util, 'IssueCommand', _VerifyTimeout):
      container_service.RunRetryableKubectlCommand(['get', 'pods'], timeout=1)

  def test_retriable_kubectl_command_fails_with_raise_on_timeout(self):
    with self.assertRaises(ValueError):
      container_service.RunRetryableKubectlCommand(
          ['get', 'pods'], raise_on_timeout=True
      )

  def test_GetNumReplicasSamples_found(self):
    resource_name = 'deployment/my_deployment'
    namespace = 'my_namespace'
    self.MockIssueCommand({
        f'get {resource_name} -n {namespace} -o=jsonpath=': [
            ('456, 123', '', 0)
        ]
    })

    def _Sample(count: int, state: str) -> Sample:
      return Sample(
          metric='k8s/num_replicas_' + state,
          value=count,
          unit='',
          metadata={
              'namespace': namespace,
              'resource_name': resource_name,
          },
          timestamp=0,
      )

    samples = _ClearTimestamps(
        container_service.KubernetesClusterCommands.GetNumReplicasSamples(
            resource_name, namespace
        )
    )
    self.assertCountEqual(
        samples,
        [
            _Sample(456, 'any'),
            _Sample(123, 'ready'),
            _Sample(456 - 123, 'unready'),
        ],
    )

  def test_GetNumReplicasSamples_not_found(self):
    resource_name = 'deployment/my_deployment'
    namespace = 'my_namespace'
    self.MockIssueCommand({
        f'get {resource_name} -n {namespace} -o=jsonpath=': [
            ('', 'Error from server (NotFound): <details>', 1)
        ]
    })

    samples = container_service.KubernetesClusterCommands.GetNumReplicasSamples(
        resource_name, namespace
    )
    self.assertEmpty(samples)

  def test_GetNumNodesSamples(self):
    self.MockIssueCommand({
        'get nodes -o=jsonpath=': [
            ('True\nFalse\nTrue\nSomethingUnexpected\n', '', 0)
        ]
    })

    def _Sample(count: int, state: str) -> Sample:
      return Sample(
          metric='k8s/num_nodes_' + state,
          value=count,
          unit='',
          metadata={},
          timestamp=0,
      )

    samples = _ClearTimestamps(
        container_service.KubernetesClusterCommands.GetNumNodesSamples()
    )
    self.assertCountEqual(
        samples,
        [
            _Sample(4, 'any'),
            _Sample(2, 'ready'),
            _Sample(1, 'unready'),
            _Sample(1, 'unknown'),
        ],
    )

  @parameterized.named_parameters(
      ('aks default', 'aks-default-30566860-vmss000000', 'default'),
      ('gke default', 'gke-pkb-8ee57c86-default-pool-232fa391-34qh', 'default'),
      ('gke servers', 'gke-pkb-8ee57c86-servers-2cd25dd3-1r9l', 'servers'),
      ('check none', 'gke-pkb-8ee57c86-someother-2cd25dd3-1r9l', None),
  )
  def testGetNodepoolFromNodeName(
      self, node_name: str, expected_nodepool_name: str | None
  ):
    vm_spec = {
        container_service_mock.TEST_CLOUD: {
            'machine_type': 'fake-machine-type',
            'zone': 'us-east2-a',
        },
    }
    nodepool_cluster = container_service_mock.TestKubernetesCluster(
        container_spec.ContainerClusterSpec(
            'test-cluster',
            **{
                'cloud': container_service_mock.TEST_CLOUD,
                'vm_spec': vm_spec,
                'nodepools': {
                    'servers': {
                        'vm_spec': vm_spec,
                    },
                    'clients': {
                        'vm_spec': vm_spec,
                    },
                },
            },
        )
    )
    nodepool = nodepool_cluster.GetNodePoolFromNodeName(node_name)
    if expected_nodepool_name is None:
      self.assertIsNone(nodepool)
    else:
      assert nodepool is not None
      self.assertEqual(nodepool.name, expected_nodepool_name)

  def testGetNodepoolFromNodeName_raisesIfMultipleNodepoolsFound(self):
    vm_spec = {
        container_service_mock.TEST_CLOUD: {
            'machine_type': 'fake-machine-type',
            'zone': 'us-east2-a',
        },
    }
    nodepool_cluster = container_service_mock.TestKubernetesCluster(
        container_spec.ContainerClusterSpec(
            'test-cluster',
            **{
                'cloud': container_service_mock.TEST_CLOUD,
                'vm_spec': vm_spec,
                'nodepools': {
                    'default-for-serving': {
                        'vm_spec': vm_spec,
                    },
                },
            },
        )
    )
    with self.assertRaises(ValueError):
      nodepool_cluster.GetNodePoolFromNodeName(
          'gke-pkb-8ee57c86-default-for-serving-232fa391-34qh'
      )

  @parameterized.named_parameters(
      ('eks_auto', 'hostname', 'k8s-fib-fib-123.elb.us-east-1.amazonaws.com'),
      ('gke', 'ip', '34.16.24.55'),
  )
  def testGetIpFromIngress(self, field_name, address):
    # ex after f-string resolution: {"ip":"34.16.24.55"}
    ingress_out = f'{{"{field_name}":"{address}"}}'
    self.assertEqual(
        self.kubernetes_cluster._GetAddressFromIngress(ingress_out),
        f'http://{address}',
    )

  @mock.patch.object(
      vm_util,
      'IssueCommand',
      return_value=['stdout', 'stderr', 0],
      autospec=True,
  )
  def test_RunKubectlCommand(self, issue_command_mock):
    stdout, stderr, status = container_service.RunKubectlCommand(
        ['get', 'pods']
    )
    self.assertEqual(stdout, 'stdout')
    self.assertEqual(stderr, 'stderr')
    self.assertEqual(status, 0)

  @mock.patch.object(
      vm_util,
      'IssueCommand',
      side_effect=errors.VmUtil.IssueCommandTimeoutError(),
      autospec=True,
  )
  def test_RunKubectlCommand_CommandTimeoutPropagated(self, issue_command_mock):
    with self.assertRaises(errors.VmUtil.IssueCommandTimeoutError):
      container_service.RunKubectlCommand(['get', 'pods'])

  def test_RunKubectlCommand_KubectlTimeoutRaisesCommandTimeout(self):
    for err in container_service.RETRYABLE_KUBECTL_ERRORS:
      with mock.patch.object(
          vm_util, 'IssueCommand', _MockedIssueCommandSuppressing(stderr=err)
      ):
        with self.assertRaises(
            errors.VmUtil.IssueCommandTimeoutError,
            msg=f'Failed to raise timeout for error: {err}',
        ):
          container_service.RunKubectlCommand(['get', 'pods'])

  def test_RunKubectlCommand_KubectlTimeoutWithSuppressFailureRaisesCommandTimeout(
      self,
  ):
    for err in container_service.RETRYABLE_KUBECTL_ERRORS:
      with mock.patch.object(
          vm_util, 'IssueCommand', _MockedIssueCommandSuppressing(stderr=err)
      ):
        with self.assertRaises(
            errors.VmUtil.IssueCommandTimeoutError,
            msg=f'Failed to raise timeout for error: {err}',
        ):
          container_service.RunKubectlCommand(
              ['get', 'pods'], suppress_failure=lambda x, y, z: True
          )

  @mock.patch.object(vm_util, 'IssueCommand', _MockedIssueCommandFailure)
  def test_RunKubectlCommand_KubectlFailWithSuppressFailure(self):
    _, _, status = container_service.RunKubectlCommand(
        ['get', 'pods'], suppress_failure=lambda x, y, z: True
    )
    self.assertEqual(status, 0)

  @mock.patch.object(
      vm_util, 'IssueCommand', return_value=(_ELECTION_EVENT_NO_NAME, '', 0)
  )
  def test_GetKubectlEvents_Success(self, unused_mock):
    events = container_service.KubernetesClusterCommands._GetEvents()
    self.assertLen(events, 1)
    self.assertEqual(
        events.pop(),
        container_service.KubernetesEvent(
            container_service.KubernetesEventResource(
                kind='ConfigMap', name=None
            ),
            message='gke-49fe-vm became leader',
            reason='LeaderElection',
            type='Normal',
            timestamp=1739209338,
        ),
    )

  def test_KubernetesEventParsing(self):
    event = container_service.KubernetesEvent.FromDict({
        'eventTime': '2025-10-03T18:05:56.272315Z',
        'involvedObject': {
            'apiVersion': 'v1',
            'kind': 'Pod',
            'name': 'kubernetes-scaleup-5d6c5f45cf-wtbmv',
            'namespace': 'default',
            'uid': '8c0f9844-cb1f-4563-a3bc-fc75e3a2fc3f',
        },
        'kind': 'Event',
        'lastTimestamp': None,
        'message': 'Successfully assigned default/deploy-pod to gke-node',
        'metadata': {
            'creationTimestamp': '2025-10-03T18:05:56Z',
        },
        'reason': 'Scheduled',
        'reportingComponent': 'default-scheduler',
        'type': 'Normal',
    })
    self.assertIsNotNone(event)
    self.assertEqual(
        event.message, 'Successfully assigned default/deploy-pod to gke-node'
    )
    self.assertEqual(event.reason, 'Scheduled')
    self.assertEqual(event.type, 'Normal')
    self.assertEqual(event.timestamp, 1759514756)

  def test_GetPodNamesForResource_success(self):
    resource_name = 'deployment/my-app'
    namespace = 'default'
    selector = '{"app":"my-app"}'
    pod_names = 'pod-1 pod-2'
    self.MockIssueCommand({
        f"get {resource_name} -n {namespace} -o=jsonpath='{{.spec.selector.matchLabels}}'": [
            (f"'{selector}'", '', 0)
        ],
        f'get pods -l app=my-app -n {namespace} -o=jsonpath={{.items[*].metadata.name}}': [
            (pod_names, '', 0)
        ],
    })
    names = container_service.KubernetesClusterCommands._GetPodNamesForResource(
        resource_name, namespace
    )
    self.assertEqual(names, ['pod-1', 'pod-2'])

  def test_GetPodNamesForResource_no_selector(self):
    resource_name = 'deployment/my-app'
    namespace = 'default'
    self.MockIssueCommand({
        f"get {resource_name} -n {namespace} -o=jsonpath='{{.spec.selector.matchLabels}}'": [
            ("''", '', 0)
        ],
    })
    with self.assertRaises(ValueError):
      container_service.KubernetesClusterCommands._GetPodNamesForResource(
          resource_name, namespace
      )

  def test_GetPodNamesForResource_resource_not_found(self):
    resource_name = 'deployment/my-app'
    namespace = 'default'
    self.MockIssueCommand({
        f"get {resource_name} -n {namespace} -o=jsonpath='{{.spec.selector.matchLabels}}'": [(
            '',
            'Error from server (NotFound): deployments.apps "my-app" not found',
            1,
        )],
    })
    names = container_service.KubernetesClusterCommands._GetPodNamesForResource(
        resource_name, namespace
    )
    self.assertEqual(names, [])

  def test_GetCPURequestSamples_success(self):
    resource_name = 'deployment/my-app'
    namespace = 'default'
    pod_names = ['pod-1', 'pod-2']
    with mock.patch.object(
        container_service.KubernetesClusterCommands,
        '_GetPodNamesForResource',
        return_value=pod_names,
    ):
      self.MockIssueCommand({
          f'get pod pod-1 -n {namespace} -o=jsonpath={{.spec.containers[*].resources.requests.cpu}}': [
              ('500m', '', 0)
          ],
          f'get pod pod-2 -n {namespace} -o=jsonpath={{.spec.containers[*].resources.requests.cpu}}': [
              ('1', '', 0)
          ],
      })
      samples = (
          container_service.KubernetesClusterCommands.GetCPURequestSamples(
              resource_name, namespace
          )
      )
      self.assertLen(samples, 2)
      self.assertEqual(samples[0].metric, 'kubernetes_cpu_request')
      self.assertEqual(samples[0].value, 0.5)
      self.assertEqual(samples[0].metadata['pod'], 'pod-1')
      self.assertEqual(samples[1].metric, 'kubernetes_cpu_request')
      self.assertEqual(samples[1].value, 1.0)
      self.assertEqual(samples[1].metadata['pod'], 'pod-2')

  def test_GetCPURequestSamples_no_pods(self):
    with mock.patch.object(
        container_service.KubernetesClusterCommands,
        '_GetPodNamesForResource',
        return_value=[],
    ):
      samples = (
          container_service.KubernetesClusterCommands.GetCPURequestSamples(
              'deployment/my-app', 'default'
          )
      )
      self.assertEmpty(samples)

  def test_GetCPUUsageSamples_success(self):
    resource_name = 'deployment/my-app'
    namespace = 'default'
    pod_names = ['pod-1']
    top_output = """
POD_NAME   NAME      CPU(cores)   MEMORY(bytes)
pod-1      my-app    123m         456Mi
"""
    with mock.patch.object(
        container_service.KubernetesClusterCommands,
        '_GetPodNamesForResource',
        return_value=pod_names,
    ):
      self.MockIssueCommand({
          f'top pod pod-1 --namespace {namespace} --containers': [
              (top_output, '', 0)
          ],
      })
      samples = container_service.KubernetesClusterCommands.GetCPUUsageSamples(
          resource_name, namespace
      )
      self.assertLen(samples, 1)
      self.assertEqual(samples[0].metric, 'kubernetes_cpu_usage')
      self.assertEqual(samples[0].value, 0.123)
      self.assertEqual(samples[0].metadata['pod'], 'pod-1')
      self.assertEqual(samples[0].metadata['container'], 'my-app')

  def test_GetCPUUsageSamples_top_fails(self):
    resource_name = 'deployment/my-app'
    namespace = 'default'
    pod_names = ['pod-1']
    with mock.patch.object(
        container_service.KubernetesClusterCommands,
        '_GetPodNamesForResource',
        return_value=pod_names,
    ):
      self.MockIssueCommand({
          f'top pod pod-1 --namespace {namespace} --containers': [
              ('', 'error: metrics not available yet', 1)
          ],
      })
      samples = container_service.KubernetesClusterCommands.GetCPUUsageSamples(
          resource_name, namespace
      )
      self.assertEmpty(samples)


def _ClearTimestamps(samples: Iterable[Sample]) -> Iterable[Sample]:
  for s in samples:
    yield Sample(s.metric, s.value, s.unit, s.metadata, timestamp=0)


if __name__ == '__main__':
  unittest.main()

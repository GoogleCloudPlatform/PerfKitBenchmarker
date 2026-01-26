import unittest

from absl.testing import parameterized
import mock
from perfkitbenchmarker import container_service
from perfkitbenchmarker.container_service import kubectl
from perfkitbenchmarker.container_service import kubernetes_commands
from perfkitbenchmarker.resources.kubernetes import wg_serving_inference_server
from tests import pkb_common_test_case

_BENCHMARK_SPEC_YAML = """
cluster_boot:
  container_cluster:
    cloud: GCP
    type: Autopilot
    vm_count: 1
    vm_spec: *default_dual_core
    inference_server:
      model_server: vllm
      hf_token: gs://bucket/path/to/token
      model_name: llama3-8b
      catalog_components: 1-L4
      hpa_max_replicas: 10
      extra_deployment_args:
        container-image: vllm/vllm-openai:v0.8.5
"""

_INFERENCE_SERVER_MANIFEST = """
kind: Service
metadata:
  name: test-service
spec:
  selector:
    app: test-app
  ports:
  - port: 80
---
kind: Deployment
metadata:
  name: test-deployment
spec:
  replicas: 1
  template:
    spec:
      containers:
      - name: inference-server
        env:
        - name: MODEL_ID
          value: test-model
"""


class WgServingInferenceServerTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.mock_cluster = self.enter_context(
        mock.patch.object(container_service, 'KubernetesCluster', autospec=True)
    )
    self.mock_run_kubectl = self.enter_context(
        mock.patch.object(kubectl, 'RunKubectlCommand', autospec=True)
    )
    self.config_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        _BENCHMARK_SPEC_YAML
    )
    self.server = wg_serving_inference_server.WGServingInferenceServer(
        spec=self.config_spec.config.container_cluster.inference_server,
        cluster=self.mock_cluster,
    )

  @parameterized.parameters(
      dict(
          catalog_components='v6e-2x2',
          expected_accelerator_type='v6e',
          expected_accelerator_count=4,
      ),
      dict(
          catalog_components='1-L4',
          expected_accelerator_type='L4',
          expected_accelerator_count=1,
      ),
      dict(
          catalog_components='gcsfuse,8-H100',
          expected_accelerator_type='H100',
          expected_accelerator_count=8,
      ),
      dict(
          catalog_components='gcsfuse',
          expected_accelerator_type='unknown',
          expected_accelerator_count=0,
      ),
  )
  def testMetadataAcceleratorType(
      self,
      catalog_components,
      expected_accelerator_type,
      expected_accelerator_count,
  ):
    modified_spec = _BENCHMARK_SPEC_YAML.replace(
        'catalog_components: 1-L4', f'catalog_components: {catalog_components}'
    )
    self.config_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        modified_spec
    )
    self.server = wg_serving_inference_server.WGServingInferenceServer(
        spec=self.config_spec.config.container_cluster.inference_server,
        cluster=self.mock_cluster,
    )
    metadata = self.server.GetResourceMetadata()
    self.assertEqual(metadata['accelerator_type'], expected_accelerator_type)
    self.assertEqual(metadata['accelerator_count'], expected_accelerator_count)

  def testDelete(self):
    self.mock_run_kubectl.return_value = ('', '', 0)
    # No assertions, but it runs without error.
    self.server.Delete()

  def testParseAndStoreInferenceServerDetails(self):
    self.server.deployment_metadata = None
    self.server._ParseAndStoreInferenceServerDetails(_INFERENCE_SERVER_MANIFEST)
    self.assertEqual(self.server.service_name, 'test-service')
    self.assertEqual(self.server.service_port, 80)
    self.assertEqual(self.server.app_selector, 'test-app')
    self.assertEqual(self.server.model_id, 'test-model')
    self.assertIsNotNone(self.server.deployment_metadata)

  @mock.patch.object(
      kubernetes_commands,
      'ApplyManifest',
      return_value=['job/test-job'],
  )
  @mock.patch.object(
      kubernetes_commands,
      'RetryableGetPodNameFromJob',
      return_value='test-pod',
  )
  @mock.patch.object(
      kubernetes_commands,
      'GetFileContentFromPod',
      return_value=(_INFERENCE_SERVER_MANIFEST),
  )
  @mock.patch.object(kubernetes_commands, 'DeleteResource')
  def testGetInferenceServerManifest(
      self,
      delete_resource_mock,
      get_file_content_from_pod_mock,
      retryable_get_pod_name_from_job_mock,
      apply_manifest_mock,
  ):
    self.server.spec.model_server = 'vllm'
    self.server.spec.model_name = 'model1'
    self.server.spec.cloud = 'gcp'
    self.server.spec.catalog_components = 'gcsfuse'
    self.server.spec.extra_deployment_args = {}
    self.server.spec.runtime_class_name = 'test-runtime'
    manifest = self.server._GetInferenceServerManifest()
    self.assertIn('runtimeClassName: test-runtime', manifest)
    self.assertIn('kind: Deployment', manifest)
    delete_resource_mock.assert_called_with(
        'job/test-job', ignore_not_found=True
    )

  @parameterized.parameters(
      dict(
          node_labels={
              'node.kubernetes.io/instance-type': 'g2-standard-8',
              'nvidia.com/gpu.product': 'L4',
          },
          expected_metadata={
              'node_name': 'test-node',
              'node_machine_type': 'g2-standard-8',
              'node_machine_family': 'g2',
              'gpu': 'L4',
          },
          description='GCP',
      ),
      dict(
          node_labels={
              'node.kubernetes.io/instance-type': 'g6.xlarge',
              'karpenter.k8s.aws/instance-family': 'g6',
              'nvidia.com/gpu.product': 'L4',
          },
          expected_metadata={
              'node_name': 'test-node',
              'node_machine_type': 'g6.xlarge',
              'node_machine_family': 'g6',
              'gpu': 'L4',
          },
          description='AWS',
      ),
      dict(
          node_labels={
              'beta.kubernetes.io/instance-type': 'Standard_NC6s_v3',
              'nvidia.com/gpu.product': 'T4',
          },
          expected_metadata={
              'node_name': 'test-node',
              'node_machine_type': 'Standard_NC6s_v3',
              'node_machine_family': 'Standard',
              'gpu': 'T4',
          },
          description='Azure',
      ),
  )
  @mock.patch.object(kubernetes_commands, 'GetResourceMetadataByName')
  def testMonitorPodStartupNodeMetadata(
      self, mock_grmbn, node_labels, expected_metadata, description
  ):
    """Tests node metadata collection in _MonitorPodStartup across clouds."""
    pod_name = 'test-pod'
    timestamp = '2024-01-01T00:00:00Z'

    pod_metadata = {
        'metadata': {'creationTimestamp': timestamp},
        'spec': {'nodeName': 'test-node'},
        'status': {
            'conditions': [
                {'type': 'PodScheduled', 'lastTransitionTime': timestamp},
                {'type': 'Ready', 'lastTransitionTime': timestamp},
            ],
            'containerStatuses': [{
                'name': 'inference-server',
                'state': {'running': {'startedAt': timestamp}},
            }],
        },
    }

    node_metadata = {'metadata': {'labels': node_labels}}

    mock_grmbn.return_value = node_metadata
    self.server.GetInferenceServerLogsFromPod = mock.Mock(return_value='logs')
    self.server.GetPodTimeZone = mock.Mock(return_value='UTC')
    self.server.timezone = None

    result = self.server._MonitorPodStartup(pod_name, pod_metadata)

    self.assertIsNotNone(result)
    for key, value in expected_metadata.items():
      self.assertEqual(
          result.metadata.get(key), value, f'{description}: {key} mismatch'
      )


if __name__ == '__main__':
  unittest.main()

"""Tests for google3.third_party.py.perfkitbenchmarker.providers.gcp.gcp_spanner."""

import unittest

from absl import flags
from absl.testing import flagsaver
from google.api_core import exceptions as google_exceptions
import mock
from perfkitbenchmarker import errors
from perfkitbenchmarker.providers.gcp import util
from perfkitbenchmarker.providers.gcp import vertex_ai
from perfkitbenchmarker.resources import managed_ai_model_spec
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

CHANGE_SERVICE_ACCOUNT_CMD = 'gsutil iam ch serviceAccount'
CREATE_SERVICE_ACCOUNT_CMD = 'gcloud iam service-accounts create'


class VertexAiTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.enter_context(flagsaver.flagsaver(run_uri='123'))
    self.enter_context(flagsaver.flagsaver(project='my-project'))
    self.enter_context(flagsaver.flagsaver(zone=['us-west-1a']))
    self.enter_context(
        mock.patch.object(
            util,
            'GetProjectNumber',
            return_value='123',
        )
    )
    self.platform_model = mock.create_autospec(vertex_ai.aiplatform.Model)
    self.enter_context(
        mock.patch.object(
            vertex_ai.aiplatform.Model,
            'upload',
            return_value=self.platform_model,
        )
    )
    self.platform_endpoint = mock.create_autospec(vertex_ai.aiplatform.Endpoint)
    self.enter_context(
        mock.patch.object(
            vertex_ai.aiplatform,
            'Endpoint',
            return_value=self.platform_endpoint,
        )
    )
    self.enter_context(
        mock.patch.object(
            vertex_ai.aiplatform.Endpoint,
            'create',
            return_value=self.platform_endpoint,
        )
    )
    self.enter_context(mock.patch.object(vertex_ai.aiplatform, 'init'))
    config = {'model_name': 'llama2', 'model_size': '7b'}
    self.ai_spec = vertex_ai.VertexAiLlama2Spec('full_name', None, **config)
    self.pkb_ai: vertex_ai.VertexAiModelInRegistry = (
        vertex_ai.VertexAiModelInRegistry(self.ai_spec)
    )

  def test_model_spec_found(self):
    ai_spec = managed_ai_model_spec.GetManagedAiModelSpecClass(
        'GCP', 'llama2', '7b'
    )
    self.assertIsNotNone(ai_spec)
    self.assertEqual(ai_spec.__name__, 'VertexAiLlama2Spec')

  def test_model_create(self):
    self.MockIssueCommand({
        'gcloud ai endpoints create': [(
            '',
            'Created Vertex AI endpoint: endpoint-name.',
            0,
        )],
    })
    self.pkb_ai.Create()
    samples = self.pkb_ai.GetSamples()
    sampled_metrics = [sample.metric for sample in samples]
    self.assertIn('Model Upload Time', sampled_metrics)
    self.assertIn('Model Deploy Time', sampled_metrics)

  def test_model_quota_error(self):
    self.MockIssueCommand({
        'gcloud ai endpoints create': [(
            '',
            'Created Vertex AI endpoint: endpoint-name.',
            0,
        )],
    })
    self.platform_model.deploy.side_effect = google_exceptions.ServiceUnavailable(
        '503 Machine type temporarily unavailable, please deploy with a'
        ' different machine type or retry. 14: Machine type temporarily'
        ' unavailable, please deploy with a different machine type or retry.'
    )
    with self.assertRaises(errors.Benchmarks.QuotaFailure):
      self.pkb_ai.Create()

  def test_model_inited(self):
    # Assert on values from setup
    self.assertEqual(self.pkb_ai.name, 'pkb123')
    self.assertEqual(
        self.pkb_ai.service_account, '123-compute@developer.gserviceaccount.com'
    )

  def test_existing_model_found(self):
    self.MockIssueCommand({
        '': [(
            'ENDPOINT_ID          DISPLAY_NAME\n'
            + '12345                some_endpoint_name',
            '',
            0,
        )]
    })
    self.assertEqual(self.pkb_ai.ListExistingEndpoints(), ['12345'])

  def test_existing_models_found(self):
    self.MockIssueCommand({
        '': [(
            'ENDPOINT_ID          DISPLAY_NAME\n'
            + '12345                some_endpoint_name\n'
            + '45678                another_endpoint_name\n',
            '',
            0,
        )]
    })
    self.assertEqual(self.pkb_ai.ListExistingEndpoints(), ['12345', '45678'])

  def test_no_models_found(self):
    self.MockIssueCommand(
        {
            '': [(
                'ENDPOINT_ID          DISPLAY_NAME\n',
                '',
                0,
            )]
        }
    )
    self.assertEqual(self.pkb_ai.ListExistingEndpoints(), [])


class VertexAiEndpointTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.endpoint = vertex_ai.VertexAiEndpoint(
        name='my-endpoint', project='my-project', region='us-east1'
    )
    self.enter_context(
        mock.patch.object(
            vertex_ai.aiplatform,
            'Endpoint',
            return_value=mock.create_autospec(vertex_ai.aiplatform.Endpoint),
        )
    )

  def test_endpoint_create(self):
    self.MockIssueCommand({
        'gcloud ai endpoints describe': [(
            ("""Using endpoint [https://us-east1-aiplatform.googleapis.com/]
createTime: '2024-09-26T21:51:53.955656Z'
deployedModels:
- createTime: '2024-09-26T21:59:03.850181Z'
  id: '12345'
"""),
            '',
            0,
        )],
        'gcloud ai endpoints undeploy-model': [('', '', 0)],
        'gcloud ai endpoints create': [(
            '',
            (
                'Using endpoint'
                ' [https://us-east1-aiplatform.googleapis.com/]\nWaiting for'
                ' operation [3827]...done.\nCreated Vertex AI endpoint:'
                ' projects/6789/locations/us-east1/endpoints/1234.'
            ),
            1,
        )],
    })
    self.endpoint._Create()
    self.assertEqual(
        'projects/6789/locations/us-east1/endpoints/1234',
        self.endpoint.endpoint_name,
    )
    self.assertIsNotNone(self.endpoint.ai_endpoint)

  def test_endpoint_delete(self):
    self.endpoint.endpoint_name = (
        'projects/6789/locations/us-east1/endpoints/1234'
    )
    self.MockIssueCommand({
        'gcloud ai endpoints describe': [(
            ("""Using endpoint [https://us-east1-aiplatform.googleapis.com/]
createTime: '2024-09-26T21:51:53.955656Z'
deployedModels:
- createTime: '2024-09-26T21:59:03.850181Z'
  id: '12345'
"""),
            '',
            0,
        )],
        'gcloud ai endpoints undeploy-model': [('', '', 0)],
        'gcloud ai endpoints delete': [('', '', 0)],
    })
    self.endpoint._Delete()


if __name__ == '__main__':
  unittest.main()

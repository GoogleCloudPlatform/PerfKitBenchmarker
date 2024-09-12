"""Tests for google3.third_party.py.perfkitbenchmarker.providers.gcp.gcp_spanner."""

import unittest

from absl import flags
from absl.testing import flagsaver
from google.api_core import exceptions as google_exceptions
import mock
from perfkitbenchmarker import errors
from perfkitbenchmarker.providers.gcp import util
from perfkitbenchmarker.providers.gcp import vertex_ai
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
            vertex_ai.aiplatform.Endpoint,
            'create',
            return_value=self.platform_endpoint,
        )
    )
    self.enter_context(mock.patch.object(vertex_ai.aiplatform, 'init'))
    self.ai_spec = vertex_ai.VertexAiLlama27bSpec('full_name')
    self.ai_spec.model_name = self.ai_spec.MODEL_NAME
    self.pkb_ai = vertex_ai.VertexAiModelInRegistry(self.ai_spec)

  def test_model_create(self):
    self.pkb_ai.Create()
    samples = self.pkb_ai.GetSamples()
    sampled_metrics = [sample.metric for sample in samples]
    self.assertIn('Model Upload Time', sampled_metrics)
    self.assertIn('Model Deploy Time', sampled_metrics)

  def test_model_quota_error(self):
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


if __name__ == '__main__':
  unittest.main()

"""Tests for google3.third_party.py.perfkitbenchmarker.providers.gcp.gcp_spanner."""

import unittest

from absl import flags
from absl.testing import flagsaver
import mock
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
    self.enter_context(flagsaver.flagsaver(zone='us-west-1a'))
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
    self.ai_spec = vertex_ai.VertexAiLlama27bSpec('full_name')
    self.ai_spec.model_name = self.ai_spec.MODEL_NAME
    self.pkb_ai = vertex_ai.VertexAiModelInRegistry(self.ai_spec)

  def test_model_inited(self):
    # Assert on values from setup
    self.assertEqual(self.pkb_ai.name, 'pkb123')
    self.assertEqual(
        self.pkb_ai.service_account, '123-compute@developer.gserviceaccount.com'
    )


if __name__ == '__main__':
  unittest.main()

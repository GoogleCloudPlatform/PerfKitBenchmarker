"""Tests for aws_jump_start."""

import unittest
from unittest import mock

from absl.testing import flagsaver
from perfkitbenchmarker.providers.aws import aws_jump_start
from perfkitbenchmarker.providers.aws import util
from tests import pkb_common_test_case

CRAWLER_ROLE = 'arn:aws:iam::123456789012:role/service-role/AWSGlueServiceRole-CrawlerTutorial'


class AwsJumpStartTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.enter_context(
        mock.patch.object(util, 'GetAccount', return_value='1234')
    )
    self.enter_context(flagsaver.flagsaver(zone=['us-west-1a']))
    self.ai_model_spec = aws_jump_start.JumpStartLlama27bSpec('f_name')
    self.ai_model = aws_jump_start.JumpStartModelInRegistry(self.ai_model_spec)

  def testEndpointNameParsedCreate(self):
    self.MockIssueCommand({
        'cat': [(
            (
                'from sagemaker.jumpstart.model import JumpStartModel\n'
                + '#more code..'
            ),
            '',
            0,
        )],
        'python3': [(
            (
                'sagemaker.config INFO - Not applying SDK defaults from'
                ' location: /etc/xdg/sagemaker/config.yaml\n'
                '--!Endpoint name:'
                ' <meta-textgeneration-llama-2-7b-f-2025-08-16>'
            ),
            (
                "For forward compatibility, pin to model_version='2.*' in"
                ' your JumpStartModel or JumpStartEstimator definitions. Note'
                ' that major version upgrades may have different EULA'
                ' acceptance terms and input/output signatures.\nUsing'
                ' vulnerable JumpStart model'
                " 'meta-textgeneration-llama-2-7b-f' and version '2.0.4'."
            ),
            0,
        )],
    })
    self.ai_model._Create()
    self.assertEqual(
        self.ai_model.endpoint_name,
        'meta-textgeneration-llama-2-7b-f-2025-08-16',
    )


if __name__ == '__main__':
  unittest.main()

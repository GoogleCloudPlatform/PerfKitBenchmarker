"""Tests for aws_jump_start."""

import unittest
from unittest import mock

from absl.testing import flagsaver
from perfkitbenchmarker.providers.aws import aws_jump_start
from perfkitbenchmarker.providers.aws import util
from tests import pkb_common_test_case

CRAWLER_ROLE = 'arn:aws:iam::123456789012:role/service-role/AWSGlueServiceRole-CrawlerTutorial'
_ZONE = 'us-west-1a'


class AwsJumpStartTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.enter_context(
        mock.patch.object(util, 'GetAccount', return_value='1234')
    )
    self.enter_context(flagsaver.flagsaver(zone=[_ZONE]))
    self.ai_model_spec = aws_jump_start.JumpStartLlama27bSpec('f_name')
    self.ai_model = aws_jump_start.JumpStartModelInRegistry(self.ai_model_spec)

  def testRegionSet(self):
    self.enter_context(flagsaver.flagsaver(zone=['us-east-1a']))
    ai_model_spec = aws_jump_start.JumpStartLlama27bSpec('f_name')
    ai_model = aws_jump_start.JumpStartModelInRegistry(ai_model_spec)
    self.assertEqual(ai_model.region, 'us-east-1')

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

  def testListEndpointsParsesOutNames(self):
    self.MockIssueCommand({
        'aws sagemaker list-endpoints': [(
            ("""{
    "Endpoints": [
        {
            "EndpointName": "woo-test",
            "EndpointArn": "arn:aws:sagemaker:us-west-2:1234:endpoint/woo-test",
            "CreationTime": "2024-08-19T18:33:34.178000+00:00",
            "LastModifiedTime": "2024-08-19T18:33:34.521000+00:00",
            "EndpointStatus": "Creating"
        },
        {
            "EndpointName": "meta-7b-f-2024-08",
            "EndpointArn": "arn:aws:sagemaker:us-west-2:1234:endpoint/meta-7b-f-2024-08",
            "CreationTime": "2024-08-16T17:59:22.752000+00:00",
            "LastModifiedTime": "2024-08-16T18:10:43.808000+00:00",
            "EndpointStatus": "InService"
        }
        ]}"""),
            '',
            0,
        )]
    })
    endpoints = self.ai_model.ListExistingEndpoints()
    self.assertEqual(endpoints, ['woo-test', 'meta-7b-f-2024-08'])

  def testListEndpointsParsesOutEmpty(self):
    self.MockIssueCommand(
        {
            'aws sagemaker list-endpoints': [(
                """{"Endpoints": []}""",
                '',
                0,
            )]
        }
    )
    endpoints = self.ai_model.ListExistingEndpoints()
    self.assertEmpty(endpoints)

  def testListEndpointsUsesAiModelRegion(self):
    mock_cmd = self.MockIssueCommand(
        {
            'aws sagemaker list-endpoints': [(
                """{"Endpoints": []}""",
                '',
                0,
            )]
        }
    )
    self.ai_model.ListExistingEndpoints()
    mock_cmd.func_to_mock.assert_called_once_with(
        ['aws', 'sagemaker', 'list-endpoints', '--region=us-west-1']
    )
    self.assertEqual(self.ai_model.region, 'us-west-1')

  def testListEndpointsUsesPassedInRegion(self):
    mock_cmd = self.MockIssueCommand(
        {
            'aws sagemaker list-endpoints': [(
                """{"Endpoints": []}""",
                '',
                0,
            )]
        }
    )
    self.ai_model.ListExistingEndpoints('us-east-1')
    mock_cmd.func_to_mock.assert_called_once_with(
        ['aws', 'sagemaker', 'list-endpoints', '--region=us-east-1']
    )

  def testPromptResponseParsed(self):
    expected_response = ''' Assistant: Here's how you can travel from Beijing to New York:

Fly from Beijing Capital International Airport to John F. Kennedy International Airport or Newark Liberty International Airport.

'''
    self.MockIssueCommand({
        'cat': [(
            '#some code',
            '',
            0,
        )],
        'python3': [(
            (f"""
Response>>>>{expected_response}====
"""),
            '',
            0,
        )],
    })
    responses = self.ai_model.SendPrompt(
        'How can you to get from Beijing to New York?', 512, 1.0
    )
    self.assertEqual(
        responses[0],
        expected_response,
    )


if __name__ == '__main__':
  unittest.main()

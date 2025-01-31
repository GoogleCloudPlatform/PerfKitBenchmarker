"""Tests for aws_jump_start."""

import unittest
from unittest import mock

from absl.testing import flagsaver
from perfkitbenchmarker import virtual_machine
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
    self.ai_model_spec = aws_jump_start.JumpStartLlama2Spec('f_name')
    self.mock_vm = mock.create_autospec(virtual_machine.BaseVirtualMachine)
    self.ai_model = aws_jump_start.JumpStartModelInRegistry(
        self.mock_vm,
        self.ai_model_spec,
    )

  def testRegionSet(self):
    self.enter_context(flagsaver.flagsaver(zone=['us-east-1a']))
    ai_model_spec = aws_jump_start.JumpStartLlama2Spec('f_name')
    ai_model = aws_jump_start.JumpStartModelInRegistry(
        self.mock_vm, ai_model_spec
    )
    self.assertEqual(ai_model.region, 'us-east-1')

  def testNamesParsedCreate(self):
    self.MockRunCommand(
        {
            'python3': [(
                (
                    'Model name:'
                    ' <meta-textgeneration-llama-2-7b-f-2025-08-16-05>sagemaker.config'
                    ' INFO - Not applying SDK defaults from location:'
                    ' /etc/xdg/sagemaker/config.yaml\n--!Endpoint name:'
                    ' <meta-textgeneration-llama-2-7b-f-2025-08-16-06>'
                ),
                (
                    "For forward compatibility, pin to model_version='2.*' in"
                    ' your JumpStartModel or JumpStartEstimator definitions.'
                    ' Note that major version upgrades may have different EULA'
                    ' acceptance terms and input/output signatures.\nUsing'
                    ' vulnerable JumpStart model'
                    " 'meta-textgeneration-llama-2-7b-f' and version '2.0.4'."
                ),
                0,
            )],
        },
        self.mock_vm,
    )
    self.ai_model._Create()
    self.assertEqual(
        self.ai_model.model_name,
        'meta-textgeneration-llama-2-7b-f-2025-08-16-05',
    )
    self.assertEqual(
        self.ai_model.endpoint_name,
        'meta-textgeneration-llama-2-7b-f-2025-08-16-06',
    )

  def testTagsAddedCreate(self):
    self.enter_context(
        mock.patch.object(
            util, 'MakeFormattedDefaultTags', return_value=['Key=K,Value=V']
        )
    )
    self.MockRunCommand(
        {
            'aws sagemaker add-tags': [(
                'Tags added',
                '',
                0,
            )],
        },
        self.mock_vm,
    )
    self.ai_model.endpoint_name = 'endpoint_name'
    self.ai_model.model_name = 'model_name'
    self.ai_model._PostCreate()
    self.mock_vm.RunCommand.assert_any_call(
        command=' '.join([
            'aws',
            'sagemaker',
            'add-tags',
            '--region=us-west-1',
            '--resource-arn=arn:aws:sagemaker:us-west-1:1234:model/model_name',
            '--tags',
            'Key=K,Value=V',
        ]),
    )
    self.mock_vm.RunCommand.assert_any_call(
        command=' '.join([
            'aws',
            'sagemaker',
            'add-tags',
            '--region=us-west-1',
            '--resource-arn=arn:aws:sagemaker:us-west-1:1234:endpoint/endpoint_name',
            '--tags',
            'Key=K,Value=V',
        ]),
    )

  def testListEndpointsParsesOutNames(self):
    self.MockRunCommand(
        {
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
        },
        self.mock_vm,
    )
    endpoints = self.ai_model.ListExistingEndpoints()
    self.assertEqual(endpoints, ['woo-test', 'meta-7b-f-2024-08'])

  def testListEndpointsParsesOutEmpty(self):
    self.MockRunCommand(
        {
            'aws sagemaker list-endpoints': [(
                """{"Endpoints": []}""",
                '',
                0,
            )]
        },
        self.mock_vm,
    )
    endpoints = self.ai_model.ListExistingEndpoints()
    self.assertEmpty(endpoints)

  def testListEndpointsUsesAiModelRegion(self):
    self.MockRunCommand(
        {
            'aws sagemaker list-endpoints': [(
                """{"Endpoints": []}""",
                '',
                0,
            )]
        },
        self.mock_vm,
    )
    self.ai_model.ListExistingEndpoints()
    self.mock_vm.RunCommand.assert_called_once_with(
        command=['aws', 'sagemaker', 'list-endpoints', '--region=us-west-1'],
    )
    self.assertEqual(self.ai_model.region, 'us-west-1')

  def testListEndpointsUsesPassedInRegion(self):
    self.MockRunCommand(
        {
            'aws sagemaker list-endpoints': [(
                """{"Endpoints": []}""",
                '',
                0,
            )]
        },
        self.mock_vm,
    )
    self.ai_model.ListExistingEndpoints('us-east-1')
    self.mock_vm.RunCommand.assert_called_once_with(
        command=['aws', 'sagemaker', 'list-endpoints', '--region=us-east-1'],
    )

  def testPromptResponseParsed(self):
    expected_response = """ Assistant: Here's how you can travel from Beijing to New York:

Fly from Beijing Capital International Airport to John F. Kennedy International Airport or Newark Liberty International Airport.

"""
    self.MockRunCommand(
        {
            'python3': [(
                f"""
Response>>>>{expected_response}====
""",
                '',
                0,
            )],
        },
        self.mock_vm,
    )
    responses = self.ai_model.SendPrompt(
        'How can you to get from Beijing to New York?', 512, 1.0
    )
    self.assertEqual(
        responses[0],
        expected_response,
    )


if __name__ == '__main__':
  unittest.main()

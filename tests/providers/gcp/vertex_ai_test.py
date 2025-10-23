"""Tests for gcp providers vertex_ai.py."""

import unittest

from absl import flags
from absl.testing import flagsaver
from absl.testing import parameterized
import mock
from perfkitbenchmarker import virtual_machine
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
    self.enter_context(flagsaver.flagsaver(ai_bucket_uri='my-bucket'))
    self.enter_context(
        mock.patch.object(
            util,
            'GetProjectNumber',
            return_value='123',
        )
    )
    config = {'model_name': 'llama2', 'model_size': '7b'}
    self.ai_spec = vertex_ai.VertexAiLlama2Spec('full_name', None, **config)
    self.vm = mock.create_autospec(virtual_machine.BaseVirtualMachine)


class VertexAiCliInterfaceTest(VertexAiTest):

  def setUp(self):
    super().setUp()
    self.pkb_ai: vertex_ai.CliVertexAiModel = vertex_ai.CliVertexAiModel(
        self.vm,
        self.ai_spec,
    )

  def test_model_spec_found(self):
    ai_spec = managed_ai_model_spec.GetManagedAiModelSpecClass(
        'GCP', 'llama2', '7b'
    )
    self.assertIsNotNone(ai_spec)
    self.assertEqual(ai_spec.__name__, 'VertexAiLlama2Spec')

  def test_model_spec_found_llama3(self):
    ai_spec = managed_ai_model_spec.GetManagedAiModelSpecClass(
        'GCP', 'llama3', '8b'
    )
    self.assertIsNotNone(ai_spec)
    self.assertEqual(ai_spec.__name__, 'VertexAiLlama3Spec')

  def test_model_create_via_gcloud(self):
    cli = self.MockRunCommand(
        {
            'gcloud ai models upload': [(
                'uploaded',
                '',
                0,
            )],
            'gcloud ai models list': [(
                'MODEL_ID             DISPLAY_NAME\n1234  pkb123',
                '',
                0,
            )],
            'gcloud ai endpoints deploy-model': [(
                'Model deployed',
                '',
                0,
            )],
        },
        self.pkb_ai.vm,
    )
    self.pkb_ai.endpoint.endpoint_name = (
        'projects/6789/locations/us-east1/endpoints/1234'
    )
    self.pkb_ai._Create()
    cli.RunCommand.assert_has_calls([
        mock.call(
            'gcloud ai models upload --display-name=pkb123 --project=my-project'
            ' --region=us-west'
            ' --artifact-uri=gs://my-bucket/llama2/llama2-7b-hf'
            ' --container-image-uri=us-docker.pkg.dev/vertex-ai/vertex-vision-model-garden-dockers/pytorch-vllm-serve:20240715_0916_RC00'
            ' --container-command=python,-m,vllm.entrypoints.api_server'
            ' --container-args=--host=0.0.0.0,--port=7080,--swap-space=16,--gpu-memory-utilization=0.9,--max-model-len=1024,--max-num-batched-tokens=4096,--tensor-parallel-size=1'
            ' --container-ports=7080 --container-predict-route=/generate'
            ' --container-health-route=/ping'
            ' --container-env-vars=MODEL_ID=gs://my-bucket/llama2/llama2-7b-hf,DEPLOY_SOURCE=pkb'
        ),
        mock.call(
            'gcloud ai models list --project=my-project --region=us-west'
        ),
        mock.call(
            'gcloud ai endpoints deploy-model'
            ' projects/6789/locations/us-east1/endpoints/1234 --model=1234'
            ' --region=us-west --project=my-project --display-name=pkb123'
            ' --machine-type=g2-standard-12'
            ' --accelerator=type=nvidia-l4,count=1'
            ' --service-account=123-compute@developer.gserviceaccount.com'
            ' --max-replica-count=1',
            ignore_failure=True,
            timeout=60 * 60,
        ),
    ])  # pytype: disable=attribute-error
    self.assertEqual(
        self.pkb_ai.model_resource_name,
        '1234',
    )

  def test_model_create_via_gcloud_waits_until_ready(self):
    self.pkb_ai.endpoint.endpoint_name = (
        'projects/6789/locations/us-east1/endpoints/1234'
    )
    cli = self.MockRunCommand(
        {
            'gcloud ai models upload': [(
                'uploaded',
                '',
                0,
            )],
            'gcloud ai models list': [(
                'MODEL_ID             DISPLAY_NAME\n1234  pkb123',
                '',
                0,
            )],
            'gcloud ai endpoints deploy-model': [(
                '',
                (
                    '(gcloud.ai.endpoints.deploy-model) Operation'
                    ' https://us-central1-aiplatform.googleapis.com/v1beta1/projects/123/locations/us-central1/endpoints/123/operations/123'
                    ' has not finished in 1800 seconds. The operations may'
                    ' still be underway remotely and may still succeed; use'
                    ' gcloud list and describe commands or'
                    ' https://console.developers.google.com/ to check resource'
                    ' state.'
                ),
                1,
            )],
            'curl': [
                ('', 'No endpoint', 1),
                (
                    '[Prompt:What is crab?\nOutput:Crabs are tasty.\n]',
                    '',
                    0,
                ),
            ],
        },
        self.pkb_ai.vm,
    )
    self.pkb_ai._Create()
    self.assertEqual(cli.RunCommand.mock_command.progress_through_calls['curl'], 2)  # pytype: disable=attribute-error

  def test_model_inited(self):
    # Assert on values from setup
    self.assertEqual(self.pkb_ai.name, 'pkb123')
    self.assertEqual(
        self.pkb_ai.service_account, '123-compute@developer.gserviceaccount.com'
    )

  def test_existing_model_found(self):
    self.MockRunCommand(
        {
            '': [(
                'ENDPOINT_ID          DISPLAY_NAME\n'
                + '12345                some_endpoint_name',
                '',
                0,
            )]
        },
        self.pkb_ai.vm,
    )
    self.assertEqual(self.pkb_ai.ListExistingEndpoints(), ['12345'])

  def test_existing_models_found(self):
    self.MockRunCommand(
        {
            '': [(
                'ENDPOINT_ID          DISPLAY_NAME\n'
                + '12345                some_endpoint_name\n'
                + '45678                another_endpoint_name\n',
                '',
                0,
            )]
        },
        self.pkb_ai.vm,
    )
    self.assertEqual(self.pkb_ai.ListExistingEndpoints(), ['12345', '45678'])

  def test_no_models_found(self):
    self.MockRunCommand(
        {
            '': [(
                '',
                'Listed 0 items',
                0,
            )]
        },
        self.pkb_ai.vm,
    )
    self.assertEqual(self.pkb_ai.ListExistingEndpoints(), [])

  def test_send_prompt(self):
    self.pkb_ai.endpoint.endpoint_name = (
        'projects/1234/locations/us-east1/endpoints/12345'
    )
    self.MockRunCommand(
        {
            'curl': [(
                '[Prompt:What is crab?\nOutput:Crabs are tasty.\n]',
                '',
                0,
            )],
        },
        self.pkb_ai.vm,
    )
    self.assertEqual(
        self.pkb_ai.SendPrompt('What is crab?', 512, 0.8),
        ['Prompt:What is crab?\nOutput:Crabs are tasty.\n'],
    )

  def test_prompt_gives_samples(self):
    self.pkb_ai.endpoint.endpoint_name = (
        'projects/1234/locations/us-east1/endpoints/12345'
    )
    self.MockRunCommand(
        {
            'curl': [(
                '[Prompt:What is crab?\nOutput:Crabs are tasty.\n]',
                '',
                0,
            )],
        },
        self.pkb_ai.vm,
    )
    self.pkb_ai.SendPrompt('What is crab?', 512, 0.8)
    samples = self.pkb_ai.GetSamples()
    metrics = [sample.metric for sample in samples]
    self.assertEqual(
        metrics,
        [
            'response_time_0',
            'Max JSON Write Time',
        ],
    )


class VertexAiModelGardenCliTest(VertexAiTest):

  def setUp(self):
    super().setUp()
    self.pkb_ai: vertex_ai.ModelGardenCliVertexAiModel = (
        vertex_ai.ModelGardenCliVertexAiModel(
            self.vm,
            self.ai_spec,
        )
    )

  @parameterized.parameters(
      '@1',
      '',
  )
  def test_model_create_via_model_garden_cli(self, ending):
    self.MockRunCommand(
        {
            'model-garden models deploy': [
                (
                    '',
                    """
                Using the selected deployment configuration:
                 Machine type: g2-standard-12
                Deploying the model to the endpoint. To check the deployment status, you can try one of the following methods:
1) Look for endpoint `meta-meta-llama-3-8b-mg-cli-deploy` at the [Vertex AI] -> [Online prediction] tab in Cloud Console
2) Use `gcloud ai operations describe 12345 --region=us-east1` to find the status of the deployment long-running operation
Waiting for operation [12345]...done.
""",
                    0,
                ),
            ],
            'ai operations describe': [(
                f"""Using endpoint [https://us-east1-aiplatform.googleapis.com/]
done: true
metadata:
  '@type': type.googleapis.com/google.cloud.aiplatform.v1beta1.DeployOperationMetadata
  projectNumber: '123'
  publisherModel: publishers/meta/models/llama3@meta-llama-3-8b
name: projects/123/locations/us-west/operations/12345
response:
  '@type': type.googleapis.com/google.cloud.aiplatform.v1beta1.DeployResponse
  endpoint: projects/123/locations/us-west/endpoints/fooendpoint
  model: projects/123/locations/us-west/models/foomodel{ending}
  publisherModel: publishers/meta/models/llama3@meta-llama-3-8b
""",
                '',
                0,
            )],
            'ai endpoints describe update': [('', '', 0)],
            'curl': [(
                '[Prompt:What is crab?\nOutput:Crabs are tasty.\n]',
                '',
                0,
            )],
        },
        self.pkb_ai.vm,
    )
    self.pkb_ai.Create()
    self.assertEqual(self.pkb_ai.model_resource_name, 'foomodel')
    self.assertEqual(
        self.pkb_ai.endpoint.endpoint_name,
        'projects/123/locations/us-west/endpoints/fooendpoint',
    )

  @flagsaver.flagsaver(ai_fast_tryout=True)
  def test_get_prompt_command_fast_tryout(self):
    self.pkb_ai.endpoint.endpoint_name = (
        'projects/pid1/locations/us-east1/endpoints/fooendpoint'
    )
    self.assertRegex(
        self.pkb_ai.GetPromptCommand('How are you?', 512, 1.0),
        r'curl -X .*'
        r' https://fooendpoint.us-west-fasttryout.prediction.vertexai.goog/v1/projects/123/locations/us-west/endpoints/fooendpoint:predict .*',
    )
    self.assertEqual(
        self.pkb_ai.endpoint.short_endpoint_name,
        'fooendpoint',
    )

  def test_model_garden_llama4_init(self):
    ai_spec = vertex_ai.VertexAiLlama4Spec('spec_name')
    self.pkb_ai: vertex_ai.ModelGardenCliVertexAiModel = (
        vertex_ai.ModelGardenCliVertexAiModel(
            self.vm,
            ai_spec,
        )
    )


class VertexAiCliEndpointTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.vm = mock.create_autospec(virtual_machine.BaseVirtualMachine)
    self.endpoint = vertex_ai.VertexAiCliEndpoint(
        name='my-endpoint',
        project='my-project',
        region='us-east1',
        vm=self.vm,
    )

  def test_endpoint_create(self):
    self.MockRunCommand(
        {
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
                    ' [https://us-east1-aiplatform.googleapis.com/]\nWaiting'
                    ' for operation [3827]...done.\nCreated Vertex AI endpoint:'
                    ' projects/6789/locations/us-east1/endpoints/1234.'
                ),
                1,
            )],
        },
        self.endpoint.vm,
    )
    self.endpoint._Create()
    self.assertEqual(
        'projects/6789/locations/us-east1/endpoints/1234',
        self.endpoint.endpoint_name,
    )

  def test_endpoint_delete(self):
    self.endpoint.endpoint_name = (
        'projects/6789/locations/us-east1/endpoints/1234'
    )
    cli = self.MockRunCommand(
        {
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
        },
        self.endpoint.vm,
    )
    self.endpoint._Delete()
    self.assertLen(cli.RunCommand.call_args_list, 3)  # pytype: disable=attribute-error
    mock_cmd = cli.RunCommand.mock_command  # pytype: disable=attribute-error
    self.assertEqual(
        mock_cmd.progress_through_calls['gcloud ai endpoints delete'], 1
    )
    self.assertEqual(
        mock_cmd.progress_through_calls['gcloud ai endpoints undeploy-model'], 1
    )

  def test_endpoint_delete_no_deployed_models(self):
    self.endpoint.endpoint_name = (
        'projects/6789/locations/us-east1/endpoints/1234'
    )
    cli = self.MockRunCommand(
        {
            'gcloud ai endpoints describe': [(
                ("""Using endpoint [https://us-east1-aiplatform.googleapis.com/]
createTime: '2024-09-26T21:51:53.955656Z'
"""),
                '',
                0,
            )],
            'gcloud ai endpoints undeploy-model': [('', '', 0)],
            'gcloud ai endpoints delete': [('', '', 0)],
        },
        self.endpoint.vm,
    )
    self.endpoint._Delete()
    self.assertLen(cli.RunCommand.call_args_list, 2)  # pytype: disable=attribute-error
    mock_cmd = cli.RunCommand.mock_command  # pytype: disable=attribute-error
    self.assertEqual(
        mock_cmd.progress_through_calls['gcloud ai endpoints delete'], 1
    )
    self.assertEqual(
        mock_cmd.progress_through_calls['gcloud ai endpoints undeploy-model'], 0
    )


if __name__ == '__main__':
  unittest.main()

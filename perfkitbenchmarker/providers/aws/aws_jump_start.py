"""Implementation of a model using Jumpstart on AWS Sagemaker.

Uses amazon python library to deploy & manage that model.

One time step:
- Create an Sagemaker execution ARN for your account. See here for details:
https://docs.aws.amazon.com/sagemaker/latest/dg/sagemaker-geospatial-roles-create-execution-role.html
"""

import json
import logging
import re
from typing import Any
from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.providers.aws import util
from perfkitbenchmarker.resources import managed_ai_model
from perfkitbenchmarker.resources import managed_ai_model_spec

FLAGS = flags.FLAGS


EXECUTION_ARN_BASE = 'arn:aws:iam::{account_number}:role/sagemaker-full-access'


# File located at google3/third_party/py/perfkitbenchmarker/scripts/
AWS_RUNNER_SCRIPT = 'aws_jump_start_runner.py'


class JumpStartModelInRegistry(managed_ai_model.BaseManagedAiModel):
  """Represents a Vertex AI model in the model registry.

  Attributes:
    model_name: The official name of the model in Model Garden, e.g. Llama2.
    name: The name of the created model in private model registry.
    account_id: The AWS account id.
    endpoint_name: The name of the deployed endpoint, if initialized.
    execution_arn: The role the model uses to run.
    vm: A vm to run commands on.
    python_script: The path to the helper python script.
  """

  CLOUD = 'AWS'
  INTERFACE = ['SDK']

  model_spec: 'JumpStartModelSpec'
  model_name: str
  model_id: str
  model_version: str | None
  name: str
  account_id: str
  endpoint_name: str | None
  execution_arn: str
  python_script: str

  def __init__(
      self,
      vm: virtual_machine.BaseVirtualMachine,
      model_spec: managed_ai_model_spec.BaseManagedAiModelSpec,
      **kwargs,
  ):
    super().__init__(model_spec, vm, **kwargs)
    if not isinstance(model_spec, JumpStartModelSpec):
      raise errors.Config.InvalidValue(
          f'Invalid model spec class: "{model_spec.__class__.__name__}". '
          'Must be a JumpStartModelSpec. It had config values of '
          f'{model_spec.model_name} & {model_spec.cloud}'
      )
    self.model_spec = model_spec
    self.model_name = model_spec.model_name
    self.model_id = model_spec.model_id
    self.model_version = model_spec.model_version
    self.account_id = util.GetAccount()
    self.execution_arn = EXECUTION_ARN_BASE.format(
        account_number=self.account_id
    )
    self.endpoint_name = None
    self.metadata.update({
        'model_name': self.model_name,
        'model_size': self.model_spec.model_size,
    })
    self.python_script = ''

  def _InitializeNewModel(self) -> 'JumpStartModelInRegistry':
    """Returns a new instance of the same class."""
    return self.__class__(vm=self.vm, model_spec=self.model_spec)

  def GetRegionFromZone(self, zone: str) -> str:
    return util.GetRegionFromZone(zone)

  def ListExistingEndpoints(self, region: str | None = None) -> list[str]:
    """Returns list of endpoint names."""
    if region is None:
      region = self.region
    out, _, _ = self.vm.RunCommand(
        ['aws', 'sagemaker', 'list-endpoints', f'--region={region}']
    )
    out_json = json.loads(out)
    json_endpoints = out_json['Endpoints']
    endpoints = []
    for json_endpoint in json_endpoints:
      endpoints.append(json_endpoint['EndpointName'])
    return endpoints

  def _RunPythonScript(self, args: list[str]) -> tuple[str, str]:
    """Calls the on-client-vm python script with appropriate arguments.

    We do this rather than just run the python code in this file to avoid
    importing the AWS libraries.
    Args:
      args: Additional arguments for the python script.

    Returns:
      Tuple of [stdout, stderr].
    """
    out, err, _ = self.vm.RunCommand(
        self._GetPythonScriptCommand(args),
        raise_on_failure=False,
        timeout=60 * 30,
        stack_level=2,
    )
    if 'Traceback' in out:
      raise errors.VirtualMachine.RemoteCommandError(
          'Ran into stack trace exception while running'
          f' {self.python_script}.\nFull output:\n{out}\nErr:\n{err}'
      )
    return out, err

  def _GetPythonScriptCommand(self, args: list[str]) -> str:
    """Returns the command to run the python script with the given args."""
    # When run without the region variable, get the error:
    # "ARN should be scoped to correct region: us-west-2"
    cmd = (
        f'export AWS_DEFAULT_REGION={self.region} && '
        # These arguments are needed for all operations.
        'python3'
        f' {self.python_script} --region={self.region} --model_id={self.model_id} '
        + ' '.join(args)
    )
    if self.model_version:
      cmd += f' --model_version={self.model_version}'
    return cmd

  def _SendPrompt(
      self, prompt: str, max_tokens: int, temperature: float, **kwargs: Any
  ) -> list[str]:
    """Sends a prompt to the model and returns the response."""
    payload = self.model_spec.GetPayload(prompt, max_tokens, temperature)
    payload_json = json.dumps(payload)
    out, err = self._RunPythonScript([
        '--operation=prompt',
        f'--endpoint_name={self.endpoint_name}',
        f"--payload='{payload_json}'",
    ])
    matches = re.search('Response>>>>(.*)====', out, flags=re.DOTALL)
    if not matches:
      raise errors.Resource.GetError(
          'Could not find response in endpoint call stdout.\nStdout:'
          f' {out}\nStderr:{err}',
      )
    return [matches.group(1)]

  def GetPromptCommand(
      self, prompt: str, max_tokens: int, temperature: float, **kwargs: Any
  ) -> str:
    """Doesn't need to be implemented as _SendPrompt is overridden."""
    raise NotImplementedError('Not implemented')

  def _Create(self) -> None:
    """Creates the underlying resource."""
    logging.info('Creating Jump Start Model: %s', self.model_id)
    out, err = self._RunPythonScript(
        ['--operation=create', f'--role={self.execution_arn}']
    )

    # TODO(user): Handle errors rather than swallowing them.
    # Unfortunately even a correct run gives some errors.
    def _FindNameMatch(out: str, resource_type: str) -> str:
      """Finds the name of the resource in the output of the python script."""
      matches = re.search(f'{resource_type}: <(.+?)>', out)
      if not matches:
        raise errors.Resource.CreationError(
            f'Could not find {resource_type} in python create output.\nStdout:'
            f' {out}\nStderr:{err}',
        )
      return matches.group(1)

    self.endpoint_name = _FindNameMatch(out, 'Endpoint name')
    self.model_name = _FindNameMatch(out, 'Model name')

  def _PostCreate(self) -> None:
    """Adds tags & metadata after creation timing."""
    self._AddTags('endpoint', self.endpoint_name)
    self._AddTags('model', self.model_name)
    describe_cmd = (
        f'aws sagemaker describe-endpoint --region={self.region} '
        f'--endpoint-name={self.endpoint_name}'
    )
    describe_out, _, _ = self.vm.RunCommand(describe_cmd)
    describe_json = json.loads(describe_out)
    endpoint_config_name = describe_json['EndpointConfigName']
    describe_endpoint_config_cmd = (
        f'aws sagemaker describe-endpoint-config --region={self.region} '
        f'--endpoint-config-name={endpoint_config_name}'
    )
    describe_endpoint_config_out, _, _ = self.vm.RunCommand(
        describe_endpoint_config_cmd
    )
    describe_endpoint_config_json = json.loads(describe_endpoint_config_out)
    self.metadata.update({
        'machine_type': describe_endpoint_config_json['ProductionVariants'][0][
            'InstanceType'
        ]
    })

  def _AddTags(self, resource_type: str, resource_name: str) -> None:
    """Adds tags to the resource with the given type & name."""
    arn = f'arn:aws:sagemaker:{self.region}:{self.account_id}:{resource_type}/{resource_name}'
    cmd = (
        f'aws sagemaker add-tags --region={self.region} --resource-arn={arn} '
        + '--tags '
        + ' '.join(util.MakeFormattedDefaultTags())
    )
    self.vm.RunCommand(cmd)

  def _CreateDependencies(self) -> None:
    self.vm.Install('pip')
    self.vm.Install('awscli')
    self.vm.RunCommand('pip install sagemaker')
    self.vm.RunCommand('pip install absl-py')
    self.python_script = self.vm.PrepareResourcePath(AWS_RUNNER_SCRIPT)
    super()._CreateDependencies()

  def _Delete(self) -> None:
    """Deletes the underlying resource."""
    assert self.endpoint_name
    self._RunPythonScript(
        ['--operation=delete', f'--endpoint_name={self.endpoint_name}']
    )


class JumpStartModelSpec(managed_ai_model_spec.BaseManagedAiModelSpec):
  """Spec for a Sagemaker JumpStart model.

  Attributes:
    model_id: Id of the model .
    model_version: Version of the model.
  """

  CLOUD = 'AWS'

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)
    self.model_id: str
    self.model_version: str | None

  def GetPayload(
      self, prompt: str, max_tokens: int, temperature: float
  ) -> dict[str, Any]:
    """Returns the payload to send to the model."""
    return {
        'inputs': [[
            {'role': 'user', 'content': prompt},
        ]],
        'parameters': {
            'max_new_tokens': max_tokens,
            'temperature': temperature,
        },
    }


class JumpStartLlama2Spec(JumpStartModelSpec):
  """Spec for running the Llama2 model.

  Source is this python notebook:
  https://github.com/aws/amazon-sagemaker-examples/blob/main/introduction_to_amazon_algorithms/jumpstart-foundation-models/llama-2-text-completion.ipynb
  """

  MODEL_NAME = 'llama2'
  MODEL_SIZE = ['7b', '70b']

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)
    self.model_id = f'meta-textgeneration-llama-2-{self.model_size}-f'
    self.model_version = '2.*'


class JumpStartLlama3Spec(JumpStartModelSpec):
  """Spec for running the Llama3 model.

  Source is this python notebook:
  https://github.com/aws/amazon-sagemaker-examples/blob/main/introduction_to_amazon_algorithms/jumpstart-foundation-models/llama-3-text-completion.ipynb
  """

  MODEL_NAME = 'llama3'
  MODEL_SIZE = ['8b', '70b']

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)
    self.model_id = f'meta-textgeneration-llama-3-{self.model_size}'
    self.model_version = '2.*'

  def GetPayload(
      self, prompt: str, max_tokens: int, temperature: float
  ) -> dict[str, Any]:
    """Returns the payload to send to the model."""
    return {
        'inputs': prompt,
        'parameters': {
            'max_new_tokens': max_tokens,
            'temperature': temperature,
        },
    }


class JumpStartLlama4Spec(JumpStartModelSpec):
  """Spec for running the Llama4 model."""

  MODEL_NAME = 'llama4'
  MODEL_SIZE = ['scout-17b', 'maverick-17b']

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)
    self.model_id = f'meta-vlm-llama-4-{self.model_size}'
    if self.model_size == 'scout-17b':
      self.model_id += '-16e-instruct'
    else:
      self.model_id += '-128e-instruct'
    self.model_version = None

  def GetPayload(
      self, prompt: str, max_tokens: int, temperature: float
  ) -> dict[str, Any]:
    """Returns the payload to send to the model."""
    return {
        'messages': [
            {'role': 'user', 'content': prompt},
        ],
        'parameters': {
            'max_new_tokens': max_tokens,
            'temperature': temperature,
        },
    }

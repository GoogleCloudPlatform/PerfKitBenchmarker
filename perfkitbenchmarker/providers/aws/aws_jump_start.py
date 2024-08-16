"""Implementation of a model using Jumpstart on AWS Sagemaker.

Uses amazon python library to deploy & manage that model.

One time step:
- Create an Sagemaker execution ARN for your account. See here for details:
https://docs.aws.amazon.com/sagemaker/latest/dg/sagemaker-geospatial-roles-create-execution-role.html
"""

import logging
from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker.providers.aws import util
from perfkitbenchmarker.resources import managed_ai_model
from perfkitbenchmarker.resources import managed_ai_model_spec

FLAGS = flags.FLAGS


EXECUTION_ARN_BASE = 'arn:aws:iam::{account_number}:role/sagemaker-full-access'


class JumpStartModelInRegistry(managed_ai_model.BaseManagedAiModel):
  """Represents a Vertex AI model in the model registry.

  Attributes:
    model_name: The official name of the model in Model Garden, e.g. Llama2.
    model_bucket_path: Where the model bucket is located.
    name: The name of the created model in private model registry.
    region: The region.
    project: The project.
    endpoint: The PKB resource endpoint the model is deployed to.
  """

  CLOUD = 'AWS'

  model_spec: 'JumpStartModelSpec'
  model_name: str
  model_id: str
  model_version: str
  model_bucket_path: str
  execution_arn: str
  name: str
  region: str
  account_id: str

  def __init__(
      self, model_spec: managed_ai_model_spec.BaseManagedAiModelSpec, **kwargs
  ):
    super().__init__(**kwargs)
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
    self.metadata.update({
        'model_name': self.model_name,
    })

  def ListExistingModels(self, zone: str | None = None) -> list[str]:
    # TODO(user): Add real code here.
    return [self.model_name]

  def _Create(self) -> None:
    """Creates the underlying resource."""
    logging.info('Creating the resource: %s for ai model.', self.model_name)

  def _Delete(self) -> None:
    """Deletes the underlying resource."""
    logging.info('Deleting the resource: %s.', self.model_name)


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
    self.model_version: str


class JumpStartLlama27bSpec(JumpStartModelSpec):
  """Spec for running the Llama2 7b model.

  Source is this python notebook:
  https://github.com/aws/amazon-sagemaker-examples/blob/main/introduction_to_amazon_algorithms/jumpstart-foundation-models/llama-2-text-completion.ipynb
  """

  MODEL_NAME = 'llama2_7b'

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)
    self.model_id = 'meta-textgeneration-llama-2-7b-f'
    self.model_version = '2.*'

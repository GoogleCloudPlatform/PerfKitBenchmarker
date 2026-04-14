"""Classes relating to decoding a Vertex Vector Search resource."""

from perfkitbenchmarker import errors
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import providers
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec


class BaseAiAgentServiceSpec(spec.BaseSpec):
  """Spec for an agentic deployment resource.

  Attributes:
    cloud: The cloud provider.
    deployment_type: The type of deployment (e.g. client_vm, vertex_ai).
  """

  SPEC_TYPE = 'BaseAiAgentServiceSpec'
  SPEC_ATTRS = ['CLOUD', 'DEPLOYMENT_TYPE']
  CLOUD = None
  DEPLOYMENT_TYPE = None

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    self.cloud: str = None
    self.deployment_type: str = None
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Modifies config options based on runtime flag values."""
    super()._ApplyFlags(config_values, flag_values)
    if flag_values['cloud'].present or 'cloud' not in config_values:
      config_values['cloud'] = flag_values.cloud

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option."""
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'cloud': (
            option_decoders.EnumDecoder,
            {
                'valid_values': provider_info.VALID_CLOUDS + (None,),
                'default': provider_info.GCP,
            },
        ),
        'deployment_type': (
            option_decoders.StringDecoder,
            {
                'none_ok': False,
                'default': 'client_vm',
            },
        ),
    })
    return result


class VertexAiCustomJobAiAgentServiceSpec(BaseAiAgentServiceSpec):
  """Spec for VertexAiCustomJobAiAgentService.

  Attributes:
    replica_count: The number of replicas.
    machine_type: The machine type.
    executor_image_uri: The executor image URI.
  """

  CLOUD = 'GCP'
  DEPLOYMENT_TYPE = 'custom_job'

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option."""
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'replica_count': (
            option_decoders.IntDecoder,
            {'default': None, 'none_ok': True},
        ),
        'machine_type': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'executor_image_uri': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
    })
    return result


class AiAgentServiceDecoder(option_decoders.TypeVerifier):
  """Validate the ai_agent_service dictionary of a benchmark config object."""

  def Decode(self, value, component_full_name, flag_values):
    """Verify ai_agent_service dict of a benchmark config object.

    Args:
      value: dict. Config dictionary
      component_full_name: string. Fully qualified name of the configurable
        component containing the config option.
      flag_values: flags.FlagValues. Runtime flag values to be propagated to
        BaseSpec constructors.

    Returns:
      _AiAgentServiceSpec built from the config passed in value.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    config = super().Decode(value, component_full_name, flag_values)
    deployment_type = config.get('deployment_type')
    if not deployment_type:
      raise errors.Config.InvalidValue(
          'Required attribute `deployment_type` missing from '
          f'ai_agent_service spec config {config}.'
      )
    if deployment_type == 'custom_job':
      for required_config in (
          'replica_count',
          'machine_type',
      ):
        if config.get(required_config) is None:
          raise errors.Config.InvalidValue(
              f'ai_agent_service.{required_config} config must be present'
          )
    cloud = config.get('cloud') or (
        flag_values.cloud if 'cloud' in flag_values else None
    )
    if cloud:
      providers.LoadProvider(cloud)
    else:
      raise errors.Config.InvalidValue(
          'ai_agent_service requires a cloud to be set.'
      )
    spec_class = GetAiAgentServiceSpecClass(cloud, deployment_type)
    return spec_class(
        self._GetOptionFullName(component_full_name),
        flag_values,
        **config,
    )


def GetAiAgentServiceSpecClass(
    cloud: str, deployment_type: str
) -> spec.BaseSpecMetaClass | None:
  """Gets spec class for the given attributes or defaults to base spec."""
  try:
    spec_class = spec.GetSpecClass(
        BaseAiAgentServiceSpec,
        CLOUD=cloud,
        DEPLOYMENT_TYPE=deployment_type,
    )
  except errors.Resource.SubclassNotFoundError:
    # fall back to basic spec (without custom config values)
    spec_class = BaseAiAgentServiceSpec
  return spec_class

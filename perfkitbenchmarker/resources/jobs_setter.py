"""Module containing class for BaseJob and BaseJobSpec."""

from typing import Any, Dict, List, Optional
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec


class BaseJobSpec(spec.BaseSpec):
  """Spec storing various data about cloud run jobs.

  That data is parsed from command lines or yaml benchmark_spec configs, using
  bunchmark_config_spec._JobDecoder.

  Attributes:
    SPEC_TYPE: The class / spec name.
    SPEC_ATTRS: Required field(s) for subclasses.
    job_region: The region to run in.
    job_backend: Amount of memory to use.
  """

  SPEC_TYPE: str = 'BaseJobSpec'
  SPEC_ATTRS: List[str] = ['SERVICE', 'CLOUD']
  job_region: str
  job_backend: str
  CLOUD: str = 'GCP'

  @classmethod
  def _GetOptionDecoderConstructions(cls) -> Dict[str, Any]:
    result = super(BaseJobSpec, cls)._GetOptionDecoderConstructions()
    result.update({
        'job_type': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'job_region': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'job_backend': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'job_count': (
            option_decoders.IntDecoder,
            {'default': 1, 'none_ok': True},
        ),
    })

    return result


def GetJobSpecClass(service: str) -> Optional[spec.BaseSpecMetaClass]:
  """Returns the job spec class corresponding to the given service.

  Args:
    service: String which matches an inherited BaseJobSpec's required SERVICE
      value.
  """
  return spec.GetSpecClass(BaseJobSpec, SERVICE=service)

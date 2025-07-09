"""Base class for Kubernetes Inference Server resource."""

from __future__ import annotations
from typing import Optional
from perfkitbenchmarker import errors
from perfkitbenchmarker import resource as pkb_resource


class BaseKubernetesInferenceServer(pkb_resource.BaseResource):
  """Base class for Inference Server resource."""

  RESOURCE_TYPE = 'BaseKubernetesInferenceServer'
  REQUIRED_ATTRS = ['INFERENCE_SERVER_TYPE']

  # The name of the container for the inference server
  WORKLOAD_NAME = ''

  def __init__(
      self,
      spec,
      cluster,
  ):
    super().__init__()
    self.spec = spec
    self.cluster = cluster
    if self.cluster is None:
      raise errors.Resource.CreationError(
          'kubernetes cluster is required for inference server resource.'
      )


def GetKubernetesInferenceServer(
    spec,
    cluster,
) -> Optional[BaseKubernetesInferenceServer]:
  """Gets the KubernetesInferenceServer class."""
  if not spec:
    return None
  inference_server_class: type[BaseKubernetesInferenceServer] = (
      pkb_resource.GetResourceClass(
          BaseKubernetesInferenceServer, INFERENCE_SERVER_TYPE=spec.type
      )
  )
  if isinstance(inference_server_class, BaseKubernetesInferenceServer):
    raise errors.Resource.CreationError(
        'Failed to find inference server resource for type: %s' % spec.type
    )
  return inference_server_class(spec, cluster)

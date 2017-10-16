# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import abc

from perfkitbenchmarker import flags
from perfkitbenchmarker import resource


flags.DEFINE_string('tpu_cidr_range', None, """CIDR Range for the TPU. The IP
                    range that the TPU will select an IP address from. Must be
                    in CIDR notation and a /29 range, for example
                    192.168.0.0/29. Errors will occur if the CIDR range has
                    already been used for a currently existing TPU, the CIDR
                    range conflicts with any networks in the user's provided
                    network, or the provided network is peered with another
                    network that is using that CIDR range.""")
flags.DEFINE_string('tpu_accelerator_type', 'tpu-v2',
                    'TPU accelerator type for the TPU.')
flags.DEFINE_string('tpu_description', None,
                    'Specifies a text description of the TPU.')
flags.DEFINE_string('tpu_network', None,
                    'Specifies the network that this TPU will be a part of.')
flags.DEFINE_string('tpu_tf_version', None,
                    'TensorFlow version for the TPU.')
flags.DEFINE_string('tpu_zone', None,
                    'The zone of the tpu to create. Zone in which TPU lives.')
flags.DEFINE_string('tpu_name', None,
                    'The name of the cloud TPU to create.')

_CLOUD_TPU_REGISTRY = {}
FLAGS = flags.FLAGS


def GetCloudTpuClass(cloud):
  """Gets the cloud TPU class corresponding to 'cloud'.

  Args:
    cloud: String. name of cloud to get the class for.

  Returns:
    Implementation class corresponding to the argument cloud

  Raises:
    Exception: An invalid cloud TPU was provided
  """
  if cloud in _CLOUD_TPU_REGISTRY:
    return _CLOUD_TPU_REGISTRY.get(cloud)
  else:
    raise Exception('No cloud TPU found for {0}'.format(cloud))


class AutoRegisterCloudTpuMeta(abc.ABCMeta):
  """Metaclass which allows Cloud TPU to register."""

  def __init__(cls, name, bases, dct):
    if hasattr(cls, 'CLOUD'):
      if cls.CLOUD is None:
        raise Exception('cloud TPU concrete subclasses must have a cloud '
                        'attribute.')
      else:
        _CLOUD_TPU_REGISTRY[cls.CLOUD] = cls
    super(AutoRegisterCloudTpuMeta, cls).__init__(name, bases, dct)


class BaseCloudTpu(resource.BaseResource):
  """Object representing a cloud TPU."""

  __metaclass__ = AutoRegisterCloudTpuMeta

  def __init__(self, cloud_tpu_spec):
    """Initialize the cloud TPU object.

    Args:
      cloud_tpu_spec: spec of the cloud TPU.
    """
    super(BaseCloudTpu, self).__init__()
    self.spec = cloud_tpu_spec

  def _Create(self):
    """Creates the cloud TPU."""
    raise NotImplementedError()

  def _Delete(self):
    """Deletes the cloud TPU.
    """
    raise NotImplementedError()

  @abc.abstractmethod
  def GetCloudTpuIp(self):
    raise NotImplementedError()

  def GetResourceMetadata(self):
    """Returns a dictionary of cluster metadata."""
    metadata = {
        'cidr_range': self.spec.tpu_cidr_range,
        'accelerator_type': self.spec.tpu_accelerator_type,
        'description': self.spec.tpu_description,
        'network': self.spec.tpu_network,
        'tf_version': self.spec.tpu_tf_version,
        'zone': self.spec.tpu_zone,
        'name': self.spec.tpu_name
    }
    return metadata

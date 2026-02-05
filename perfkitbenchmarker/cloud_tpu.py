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
"""Module containing class for TPU."""

from absl import flags
from perfkitbenchmarker import resource


flags.DEFINE_string('tpu_name', None, 'The name of the TPU to create.')
flags.DEFINE_string('tpu_type', None, 'TPU type for the TPU.')
flags.DEFINE_string('tpu_topology', None, 'TPU topology for the TPU.')
flags.DEFINE_string('tpu_tf_version', None, 'TensorFlow version for the TPU.')
flags.DEFINE_string(
    'tpu_zone', None, 'The zone of the tpu to create. Zone in which TPU lives.'
)

FLAGS = flags.FLAGS


def GetTpuClass(cloud):
  """Gets the TPU class corresponding to 'cloud'.

  Args:
    cloud: String. name of cloud to get the class for.

  Returns:
    Implementation class corresponding to the argument cloud

  Raises:
    Exception: An invalid TPU was provided
  """
  return resource.GetResourceClass(BaseTpu, CLOUD=cloud)


class BaseTpu(resource.BaseResource):
  """Object representing a TPU."""

  RESOURCE_TYPE = 'BaseTpu'

  def __init__(self, tpu_spec):
    """Initialize the TPU object.

    Args:
      tpu_spec: spec of the TPU.
    """
    super().__init__()
    self.spec = tpu_spec
    self.create_start_time = -1
    self.create_time = -1

  def GetResourceMetadata(self):
    """Returns a dictionary of cluster metadata."""
    metadata = {
        'name': self.spec.tpu_name,
        'tpu_type': self.spec.tpu_type,
        'tpu_topology': self.spec.tpu_topology,
        'tf_version': self.spec.tpu_tf_version,
        'zone': self.spec.tpu_zone,
    }
    return metadata

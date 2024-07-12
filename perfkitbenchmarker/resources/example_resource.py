# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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

"""A simple example resource.

A simple example resource that just logs. To automatically create/delete a new
resource, also add references in benchmark_spec.py. To instantiate this resource
from a BENCHMARK_CONFIG, see example_benchmark.py.
"""

import logging
from typing import Optional
from absl import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker.resources import example_resource_spec

FLAGS = flags.FLAGS


class BaseExampleResource(resource.BaseResource):
  """A simple example resource."""

  RESOURCE_TYPE = 'BaseExampleResource'
  REQUIRED_ATTRS = ['EXAMPLE_TYPE']


class ImplementedExampleResource(BaseExampleResource):
  """An implementation of a simple example resource."""

  EXAMPLE_TYPE = 'ImplementedExampleResource'

  def __init__(
      self, ex_spec: example_resource_spec.BaseExampleResourceSpec, **kwargs
  ):
    super().__init__(**kwargs)
    self.log_text = ex_spec.log_text
    self.name = FLAGS.run_uri
    self.metadata.update({
        'name': self.name,
        'log_text': self.log_text,
    })

  def _ExampleLogString(self) -> str:
    """Returns a string summary of the resource."""
    return f'ExampleResource: {self.name} logging {self.log_text}'

  def _Create(self) -> None:
    """Creates the underlying resource."""
    logging.info('Creating the resource: %s.', self._ExampleLogString())

  def _Delete(self) -> None:
    """Deletes the underlying resource."""
    logging.info('Deleting the resource: %s.', self._ExampleLogString())


def GetExampleResourceClass(
    example_type: str,
) -> Optional[resource.AutoRegisterResourceMeta]:
  """Gets the example resource class corresponding to 'example_type'."""
  return resource.GetResourceClass(
      BaseExampleResource, EXAMPLE_TYPE=example_type
  )

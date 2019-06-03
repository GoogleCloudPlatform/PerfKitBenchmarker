# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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

"""Contains code related to Docker resource specs"""

from perfkitbenchmarker import providers
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import custom_virtual_machine_spec
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec

class DockerSpec(virtual_machine.BaseVmSpec):
  """Object containing the information needed to create a MesosDockerInstance.

  Attributes:
    docker_cpus: None or float. Number of CPUs for Docker instances.
    docker_memory_mb: None or int. Memory limit (in MB) for Docker instances.
    mesos_privileged_docker: None of boolean. Indicates if Docker container
        should be run in privileged mode.
  """

  CLOUD = providers.DOCKER

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    result = super(DockerSpec, cls)._GetOptionDecoderConstructions()
    result.update({
        'docker_provider_cpus': (option_decoders.FloatDecoder, {'default': 0}),
        'docker_provider_memory_mb': (option_decoders.IntDecoder, {'default': 0}),
        'privileged_docker': (option_decoders.BooleanDecoder,
                                    {'default': False})})
    return result

  def _ApplyFlags(self, config_values, flag_values):
    super(DockerSpec, self)._ApplyFlags(config_values, flag_values)
    if flag_values['docker_provider_cpus'].present:
      config_values['docker_provider_cpus'] = flag_values.docker_provider_cpus
    if flag_values['docker_provider_memory_mb'].present:
      config_values['docker_provider_memory_mb'] = flag_values.docker_provider_memory_mb
    if flag_values['privileged_docker'].present:
      config_values['privileged_docker'] =\
          flag_values.privileged_docker

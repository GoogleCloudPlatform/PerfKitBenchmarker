# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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

from perfkitbenchmarker import flags

flags.DEFINE_string('remote_image_repository', None,
                    'Remote Repository to use')

flags.DEFINE_boolean('use_google_container_builder', False,
                     'Chooses whether to build locally or use Google container builder')

flags.DEFINE_string('custom_docker_image', None,
                    'Custom docker image location')

# _DOCKER_VOLUME_TYPES = ['something', 'something_else']

# flags.DEFINE_enum('docker_volume_type', None, _DOCKER_VOLUME_TYPES,
#                   'The name of the types of Docker volumes available')

flags.DEFINE_boolean('privileged_docker', False,
                     'If set to True, will attempt to create Docker containers '
                     'in a privileged mode. Note that some benchmarks execute '
                     'commands which are only allowed in privileged mode.')


flags.DEFINE_string('docker_cli', 'docker',
                    'Path to docker cli. You can set it here if it is'
                    'not in your system PATH or not at a default location')


# Defined in providers/mesos/flags.py
# flags.DEFINE_integer('docker_memory_mb', 2048,
#                      'Memory limit for docker containers.')

# flags.DEFINE_float('docker_cpus', 1,
#                    'CPU limit for docker containers.')

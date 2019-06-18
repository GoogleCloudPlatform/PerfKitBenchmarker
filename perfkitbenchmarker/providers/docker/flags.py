# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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

flags.DEFINE_string('docker_custom_image', None,
                    'Custom docker image location')

flags.DEFINE_boolean('privileged_docker', False,
                     'If set to True, will attempt to create Docker containers '
                     'in a privileged mode. Note that some benchmarks execute '
                     'commands which are only allowed in privileged mode.')

flags.DEFINE_string('docker_cli', 'docker',
                    'Path to docker cli. You can set it here if it is'
                    'not in your system PATH or not at a default location')

flags.DEFINE_integer('docker_provider_memory_mb', 0,
                     'Memory limit for docker containers.')

flags.DEFINE_float('docker_provider_cpus', 0,
                   'CPU limit for docker containers.')

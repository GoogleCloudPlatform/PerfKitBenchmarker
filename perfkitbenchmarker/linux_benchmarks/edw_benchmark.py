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

"""Runs Enterprise Data Warehouse (edw) performance benchmarks.

This benchmark adds the ability to run arbitrary sql workloads on hosted fully
managed data warehouse solutions such as Redshift and BigQuery.
"""


import copy
import json
import os

from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample

BENCHMARK_NAME = 'edw_benchmark'

BENCHMARK_CONFIG = """
edw_benchmark:
  description: Sample edw benchmark
  edw_service:
    type: redshift
    cluster_identifier: _cluster_id_
    endpoint: cluster.endpoint
    db: _database_name_
    user: _username_
    password: _password_
    node_type: dc1.large
    node_count: 2
    snapshot:
  vm_groups:
    client:
      vm_spec: *default_single_core
"""

SERVICE_SPECIFIC_SCRIPT_LIST = ['script_runner.sh',
                                'provider_specific_script_driver.py']

flags.DEFINE_string('edw_benchmark_script', None, 'Path to the sql script.')

FLAGS = flags.FLAGS


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def _PushScripts(benchmark_spec, vm):
  """Method to push the sql script and driver scripts on the client vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
    required to run the benchmark.
    vm: Client vm on which the script will be run.
  """
  service_specific_dir = os.path.join('edw',
                                      benchmark_spec.edw_service.SERVICE_TYPE)

  def _PushServiceSpecificScripts():
    """Push service type specific scripts."""
    for script in SERVICE_SPECIFIC_SCRIPT_LIST:
      vm.PushFile(data.ResourcePath(os.path.join(service_specific_dir, script)))
    runner_perms_update_cmd = 'chmod 755 {}'.format('script_runner.sh')
    vm.RemoteCommand(runner_perms_update_cmd)

  vm.PushFile(data.ResourcePath(os.path.join('edw', 'script_driver.py')))
  _PushServiceSpecificScripts()
  vm.PushFile(FLAGS.edw_benchmark_script)


def RunScriptsAndParseOutput(launch_command, vm):
  """A function to launch the command and return the performance.

  Args:
    launch_command: Arguments to the script driver.
    vm: Client vm on which the script will be run.

  Returns:
    json representation of the performance results.
  """
  stdout, _ = vm.RemoteCommand(launch_command)
  performance = json.loads(stdout)
  return performance


def Prepare(benchmark_spec):
  """Install script execution environment on the client vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
    required to run the benchmark.
  """
  vm = benchmark_spec.vms[0]
  vm.Install('pip')
  vm.RemoteCommand('sudo pip install absl-py')
  edw_service_instance = benchmark_spec.edw_service
  edw_service_instance.InstallAndAuthenticateRunner(vm)
  _PushScripts(benchmark_spec, vm)


def Run(benchmark_spec):
  """Run phase executes the sql scripts on edw cluster and collects duration."""
  vm = benchmark_spec.vms[0]
  edw_service_instance = benchmark_spec.edw_service
  instance_specific_launch_command = edw_service_instance.RunCommandHelper()
  script_name = os.path.basename(os.path.normpath(FLAGS.edw_benchmark_script))
  launch_command = ' '.join(['python', 'script_driver.py',
                             '--script={}'.format(script_name),
                             instance_specific_launch_command])
  script_performance = RunScriptsAndParseOutput(launch_command, vm)[script_name]
  results = []
  script_metadata = copy.copy(edw_service_instance.GetMetadata())
  script_metadata['script'] = script_name
  script_performance_sample = sample.Sample('script_runtime', script_performance,
                                            'seconds', script_metadata)
  results.append(script_performance_sample)
  return results


def Cleanup(benchmark_spec):
  del benchmark_spec  # Unused by Cleanup.

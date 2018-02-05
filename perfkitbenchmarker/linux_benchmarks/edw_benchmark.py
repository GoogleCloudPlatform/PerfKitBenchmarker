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
    cluster_identifier: cluster123
    endpoint: cluster123.c85qhtyvrisa.us-west-2.redshift.amazonaws.com
    db: dev
    user: masteruser
    password: Password123
    node_type: dc1.large
    node_count: 2
    snapshot:
  vm_groups:
    client:
      vm_spec: *default_single_core
"""
flags.DEFINE_list('edw_benchmark_scripts', 'sample.sql', 'Comma separated '
                                                         'list of scripts.')

FLAGS = flags.FLAGS


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  vm = benchmark_spec.vms[0]
  vm.Install('pgbench')


def Run(benchmark_spec):
  """Run phase executes the sql scripts on edw cluster and collects duration."""
  vm = benchmark_spec.vms[0]
  driver_name = '{}_driver.sh'.format(benchmark_spec.edw_service.SERVICE_TYPE)
  driver_path = data.ResourcePath(os.path.join('edw', driver_name))
  vm.PushFile(driver_path)

  scripts_dir = '{}_sql'.format(benchmark_spec.edw_service.SERVICE_TYPE)
  scripts_list = FLAGS.edw_benchmark_scripts
  for script in scripts_list:
    script_path = data.ResourcePath(os.path.join('edw', scripts_dir, script))
    vm.PushFile(script_path)

  driver_perms_update_cmd = 'chmod 755 {}'.format(driver_name)
  vm.RemoteCommand(driver_perms_update_cmd)

  endpoint = benchmark_spec.edw_service.endpoint
  db = benchmark_spec.edw_service.db
  user = benchmark_spec.edw_service.user
  password = benchmark_spec.edw_service.password

  launch_command_generic = './{} {} {} {} {} '.format(driver_name, endpoint, db,
                                                      user, password)
  scripts_list = FLAGS.edw_benchmark_scripts

  results = []
  edw_service_instance = benchmark_spec.edw_service
  edw_metadata = copy.copy(edw_service_instance.GetMetadata())

  if FLAGS.edw_query_execution_mode == 'sequential':
    total_time = 0.0
    for script in scripts_list:
      launch_command = '{}{}'.format(launch_command_generic, script)
      stdout, _ = vm.RemoteCommand(launch_command)
      sql_script_metadata = copy.copy(edw_metadata)
      sql_script_metadata['edw_benchmark_script'] = script
      results.append(sample.Sample('sql_script_run_time', float(stdout),
                                   'seconds', sql_script_metadata))
      total_time += float(stdout)
    edw_metadata['edw_query_execution_mode'] = 'sequential'
    edw_metadata['edw_benchmark_scripts'] = FLAGS.edw_benchmark_scripts
    results.append(sample.Sample('all_sql_run_time', total_time, 'seconds',
                                 edw_metadata))
  else:
    scripts_list_serialized = ' '.join(scripts_list)
    launch_command = '{}{}'.format(launch_command_generic,
                                   scripts_list_serialized)
    stdout, _ = vm.RemoteCommand(launch_command)
    edw_metadata['edw_query_execution_mode'] = 'concurrent'
    edw_metadata['edw_benchmark_scripts'] = FLAGS.edw_benchmark_scripts
    results.append(sample.Sample('all_sql_run_time', float(stdout), 'seconds',
                                 edw_metadata))
  return results


def Cleanup(benchmark_spec):
  del benchmark_spec  # Unused by Cleanup.

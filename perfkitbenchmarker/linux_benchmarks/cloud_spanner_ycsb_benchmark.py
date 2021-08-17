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

"""Run YCSB benchmark against Google Cloud Spanner.

By default, this benchmark provision 1 single-CPU VM and spawn 1 thread
to test Spanner. Configure the number of VMs via --ycsb_client_vms.
"""

import logging
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import ycsb
from perfkitbenchmarker.providers.gcp import gcp_spanner
from perfkitbenchmarker.providers.gcp import util

BENCHMARK_NAME = 'cloud_spanner_ycsb'

BENCHMARK_DESCRIPTION = 'YCSB'
BENCHMARK_TABLE = 'usertable'
BENCHMARK_ZERO_PADDING = 12

REQUIRED_SCOPES = (
    'https://www.googleapis.com/auth/spanner.admin',
    'https://www.googleapis.com/auth/spanner.data')

BENCHMARK_CONFIG = f"""
cloud_spanner_ycsb:
  description: >
      Run YCSB against Google Cloud Spanner.
      Configure the number of VMs via --ycsb_client_vms.
  vm_groups:
    default:
      vm_spec: *default_single_core
      vm_count: 1
  spanner:
    service_type: {gcp_spanner.DEFAULT_SPANNER_TYPE}
    nodes: 1
    description: {BENCHMARK_DESCRIPTION}
    enable_freeze_restore: True
  flags:
    gcloud_scopes: >
      {' '.join(REQUIRED_SCOPES)}"""

CLIENT_TAR_URL = {
    'go': 'https://storage.googleapis.com/cloud-spanner-client-packages/'
          'ycsb-go-20180531_f0afaf5fad3c46ae392ebab6b7553d37d65d07ac.tar.gz',
}

FLAGS = flags.FLAGS
flags.DEFINE_enum('cloud_spanner_ycsb_client_type', 'java', ['java', 'go'],
                  'The type of the client.')
flags.DEFINE_integer('cloud_spanner_ycsb_batchinserts',
                     1,
                     'The Cloud Spanner batch inserts used in the YCSB '
                     'benchmark.')
flags.DEFINE_integer('cloud_spanner_ycsb_boundedstaleness',
                     0,
                     'The Cloud Spanner bounded staleness used in the YCSB '
                     'benchmark.')
flags.DEFINE_enum('cloud_spanner_ycsb_readmode',
                  'query', ['query', 'read'],
                  'The Cloud Spanner read mode used in the YCSB benchmark.')
flags.DEFINE_list('cloud_spanner_ycsb_custom_vm_install_commands', [],
                  'A list of strings. If specified, execute them on every '
                  'VM during the installation phase.')


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS['ycsb_client_vms'].present:
    config['vm_groups']['default']['vm_count'] = FLAGS.ycsb_client_vms
  config['spanner']['ddl'] = _BuildSchema()
  return config


def CheckPrerequisites(benchmark_config):
  for scope in REQUIRED_SCOPES:
    if scope not in FLAGS.gcloud_scopes:
      raise ValueError('Scope {0} required.'.format(scope))


def Prepare(benchmark_spec):
  """Prepare the virtual machines to run cloud spanner benchmarks.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  benchmark_spec.always_call_cleanup = True

  if FLAGS.cloud_spanner_ycsb_client_type != 'java':
    ycsb.SetYcsbTarUrl(CLIENT_TAR_URL[FLAGS.cloud_spanner_ycsb_client_type])

  vms = benchmark_spec.vms

  # Install required packages and copy credential files
  vm_util.RunThreaded(_Install, vms)

  benchmark_spec.executor = ycsb.YCSBExecutor('cloudspanner')


def Run(benchmark_spec):
  """Spawn YCSB and gather the results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample instances.
  """
  vms = benchmark_spec.vms
  run_kwargs = {
      'table': BENCHMARK_TABLE,
      'zeropadding': BENCHMARK_ZERO_PADDING,
      'cloudspanner.instance': benchmark_spec.spanner.name,
      'cloudspanner.database': benchmark_spec.spanner.database,
      'cloudspanner.readmode': FLAGS.cloud_spanner_ycsb_readmode,
      'cloudspanner.boundedstaleness':
          FLAGS.cloud_spanner_ycsb_boundedstaleness,
      'cloudspanner.batchinserts': FLAGS.cloud_spanner_ycsb_batchinserts,
  }
  # Uses overridden cloud spanner endpoint in gcloud configuration
  end_point = benchmark_spec.spanner.GetEndPoint()
  if end_point:
    run_kwargs['cloudspanner.host'] = end_point

  if FLAGS.cloud_spanner_ycsb_client_type == 'go':
    run_kwargs['cloudspanner.project'] = util.GetDefaultProject()

  load_kwargs = run_kwargs.copy()
  samples = list(benchmark_spec.executor.LoadAndRun(
      vms, load_kwargs=load_kwargs, run_kwargs=run_kwargs))

  metadata = {'ycsb_client_type': FLAGS.cloud_spanner_ycsb_client_type}
  for sample in samples:
    sample.metadata.update(metadata)

  return samples


def Cleanup(benchmark_spec):
  """Cleanup.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  del benchmark_spec


def _BuildSchema():
  """BuildSchema.

  Returns:
    A string of DDL for creating a Spanner table.
  """
  fields = ',\n'.join(
      [f'field{i} STRING(MAX)' for i in range(FLAGS.ycsb_field_count)]
  )
  return f"""
  CREATE TABLE {BENCHMARK_TABLE} (
    id     STRING(MAX),
    {fields}
  ) PRIMARY KEY(id)
  """


def _Install(vm):
  if FLAGS.cloud_spanner_ycsb_client_type == 'go':
    logging.info('Installing go packages.')
    vm.Install('go_lang')
    vm.Install('google_cloud_go')
  vm.Install('ycsb')

  # Run custom VM installation commands.
  for command in FLAGS.cloud_spanner_ycsb_custom_vm_install_commands:
    _, _ = vm.RemoteCommand(command)

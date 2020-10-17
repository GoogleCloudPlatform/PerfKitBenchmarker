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

"""Run YCSB benchmark against Google Cloud Spanner

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
BENCHMARK_CONFIG = """
cloud_spanner_ycsb:
  description: >
      Run YCSB against Google Cloud Spanner.
      Configure the number of VMs via --ycsb_client_vms.
  vm_groups:
    default:
      vm_spec: *default_single_core
      vm_count: 1
  flags:
    gcloud_scopes: >
      https://www.googleapis.com/auth/spanner.admin
      https://www.googleapis.com/auth/spanner.data"""
BENCHMARK_INSTANCE_PREFIX = 'ycsb-'
BENCHMARK_DESCRIPTION = 'YCSB'
BENCHMARK_DATABASE = 'ycsb'
BENCHMARK_TABLE = 'usertable'
BENCHMARK_ZERO_PADDING = 12
BENCHMARK_SCHEMA = """
CREATE TABLE %s (
  id     STRING(MAX),
  field0 STRING(MAX),
  field1 STRING(MAX),
  field2 STRING(MAX),
  field3 STRING(MAX),
  field4 STRING(MAX),
  field5 STRING(MAX),
  field6 STRING(MAX),
  field7 STRING(MAX),
  field8 STRING(MAX),
  field9 STRING(MAX),
) PRIMARY KEY(id)
""" % BENCHMARK_TABLE

REQUIRED_SCOPES = (
    'https://www.googleapis.com/auth/spanner.admin',
    'https://www.googleapis.com/auth/spanner.data')

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

  benchmark_spec.spanner_instance = gcp_spanner.GcpSpannerInstance(
      name=BENCHMARK_INSTANCE_PREFIX + FLAGS.run_uri,
      description=BENCHMARK_DESCRIPTION,
      database=BENCHMARK_DATABASE,
      ddl=BENCHMARK_SCHEMA)
  if benchmark_spec.spanner_instance._Exists(instance_only=True):
    logging.warning('Cloud Spanner instance %s exists, delete it first.' %
                    FLAGS.cloud_spanner_ycsb_instance)
    benchmark_spec.spanner_instance.Delete()
  benchmark_spec.spanner_instance.Create()
  if not benchmark_spec.spanner_instance._Exists():
    logging.warning('Failed to create Cloud Spanner instance and database.')
    benchmark_spec.spanner_instance.Delete()

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
      'cloudspanner.instance': BENCHMARK_INSTANCE_PREFIX + FLAGS.run_uri,
      'cloudspanner.database': BENCHMARK_DATABASE,
      'cloudspanner.readmode': FLAGS.cloud_spanner_ycsb_readmode,
      'cloudspanner.boundedstaleness':
          FLAGS.cloud_spanner_ycsb_boundedstaleness,
      'cloudspanner.batchinserts': FLAGS.cloud_spanner_ycsb_batchinserts,
  }

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
  benchmark_spec.spanner_instance.Delete()


def _Install(vm):
  if FLAGS.cloud_spanner_ycsb_client_type == 'go':
    logging.info('Installing go packages.')
    vm.Install('go_lang')
    vm.Install('google_cloud_go')
  vm.Install('ycsb')

  # Run custom VM installation commands.
  for command in FLAGS.cloud_spanner_ycsb_custom_vm_install_commands:
    _, _ = vm.RemoteCommand(command)

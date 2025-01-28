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

"""Run YCSB benchmark against Google Cloud Datastore.

Before running this benchmark, you have to download your P12
service account private key file to local machine, and pass the path
via 'google_datastore_keyfile' parameters to PKB.

Service Account email associated with the key file is also needed to
pass to PKB.

By default, this benchmark provision 1 single-CPU VM and spawn 1 thread
to test Datastore.
"""

import logging

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.linux_packages import ycsb
from perfkitbenchmarker.providers.gcp import util


BENCHMARK_NAME = 'cloud_datastore_ycsb'
BENCHMARK_CONFIG = """
cloud_datastore_ycsb:
  description: >
      Run YCSB agains Google Cloud Datastore.
      Configure the number of VMs via --num-vms.
  vm_groups:
    default:
      os_type: ubuntu2204  # Python 2
      vm_spec: *default_single_core
      vm_count: 1
  flags:
    openjdk_version: 11
    gcloud_scopes: >
      trace
      datastore
      cloud-platform"""

# the name of the database entity created when running datastore YCSB
# https://github.com/brianfrankcooper/YCSB/tree/master/googledatastore
_YCSB_COLLECTIONS = ['usertable']

FLAGS = flags.FLAGS
_DATASET_ID = flags.DEFINE_string(
    'google_datastore_datasetId',
    None,
    'The ID of the database to use for benchmarking',
)
_DEBUG = flags.DEFINE_string(
    'google_datastore_debug', 'false', 'The logging level when running YCSB'
)
_TARGET_LOAD_QPS = flags.DEFINE_integer(
    'google_datastore_target_load_qps',
    500,
    'The target QPS to load the database at. See'
    ' https://cloud.google.com/datastore/docs/best-practices#ramping_up_traffic'
    ' for more info.',
)

_INSERTION_RETRY_LIMIT = 100


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS['ycsb_client_vms'].present:
    config['vm_groups']['default']['vm_count'] = FLAGS.ycsb_client_vms
  return config


def CheckPrerequisites(_):
  if not ycsb.SKIP_LOAD_STAGE.value and not _TARGET_LOAD_QPS.value:
    raise errors.Setup.InvalidFlagConfigurationError(
        '--google_datastore_target_load_qps must be set when loading the'
        ' database.'
    )


def _Install(vm):
  """Installs YCSB benchmark & copies datastore keyfile to client vm."""
  vm.Install('ycsb')


def _GetCommonYcsbArgs():
  """Returns common YCSB args."""
  project = FLAGS.project or util.GetDefaultProject()
  args = {
      'googledatastore.projectId': project,
      'googledatastore.debug': _DEBUG.value,
  }
  # if not provided, use the (default) database.
  if _DATASET_ID.value:
    args['googledatastore.datasetId'] = _DATASET_ID.value
  return args


def _GetYcsbExecutor():
  return ycsb.YCSBExecutor('googledatastore')


def RampUpLoad(
    ycsb_executor: ycsb.YCSBExecutor,
    vms: list[virtual_machine.VirtualMachine],
    load_kwargs: dict[str, str] = None,
) -> None:
  """Loads YCSB by gradually incrementing target QPS.

  Note that this requires clients to be overprovisioned, as the target QPS
  for YCSB is generally a "throttling" mechanism where the threads try to send
  as much QPS as possible and then get throttled. If clients are
  underprovisioned then it's possible for the run to not hit the desired
  target, which may be undesired behavior.

  See
  https://cloud.google.com/datastore/docs/best-practices#ramping_up_traffic
  for an example of why this is needed.

  Args:
    ycsb_executor: The YCSB executor to use.
    vms: The client VMs to generate the load.
    load_kwargs: Extra run arguments.
  """
  target_load_qps = _TARGET_LOAD_QPS.value
  incremental_targets = ycsb_executor.GetIncrementalQpsTargets(target_load_qps)
  logging.info('Incremental load stage target QPS: %s', incremental_targets)

  ramp_up_args = load_kwargs.copy()
  for target in incremental_targets:
    target /= len(vms)
    ramp_up_args['target'] = int(target)
    ramp_up_args['threads'] = min(FLAGS.ycsb_preload_threads, int(target))
    ramp_up_args['maxexecutiontime'] = ycsb.INCREMENTAL_TIMELIMIT_SEC
    ycsb_executor.Load(vms, load_kwargs=ramp_up_args)

  target_load_qps /= len(vms)
  load_kwargs['target'] = int(target_load_qps)
  ycsb_executor.Load(vms, load_kwargs=load_kwargs)


def Prepare(benchmark_spec):
  """Prepare the virtual machines to run cloud datastore.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  vms = benchmark_spec.vms

  # Install required packages and copy credential files
  background_tasks.RunThreaded(_Install, vms)

  if ycsb.SKIP_LOAD_STAGE.value:
    return

  load_kwargs = _GetCommonYcsbArgs()
  load_kwargs['core_workload_insertion_retry_limit'] = _INSERTION_RETRY_LIMIT
  executor = _GetYcsbExecutor()
  RampUpLoad(executor, vms, load_kwargs)


def Run(benchmark_spec):
  """Spawn YCSB and gather the results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample instances.
  """
  executor = _GetYcsbExecutor()
  vms = benchmark_spec.vms
  run_kwargs = _GetCommonYcsbArgs()
  run_kwargs.update({
      'googledatastore.tracingenabled': True,
      'readallfields': True,
      'writeallfields': True,
  })
  samples = list(executor.Run(vms, run_kwargs=run_kwargs))
  return samples


def Cleanup(_):
  pass

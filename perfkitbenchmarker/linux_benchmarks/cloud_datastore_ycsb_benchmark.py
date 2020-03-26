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

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import ycsb
from perfkitbenchmarker.providers.gcp import gcp_datastore

PRIVATE_KEYFILE_DIR = '/tmp/key.p12'

BENCHMARK_NAME = 'cloud_datastore_ycsb'
BENCHMARK_CONFIG = """
cloud_datastore_ycsb:
  description: >
      Run YCSB agains Google Cloud Datastore.
      Configure the number of VMs via --num-vms.
  vm_groups:
    default:
      vm_spec: *default_single_core
      vm_count: 1"""

FLAGS = flags.FLAGS

flags.DEFINE_string('google_datastore_debug',
                    'false',
                    'The logging level when running YCSB')


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS['ycsb_client_vms'].present:
    config['vm_groups']['default']['vm_count'] = FLAGS.ycsb_client_vms
  return config


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present.

  Args:
    benchmark_config: Unused.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  benchmark_config.datastore_instance = gcp_datastore.GcpDatastoreInstance()


def Prepare(benchmark_spec):
  """Prepare the virtual machines to run cloud datastore.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  benchmark_spec.always_call_cleanup = True
  vms = benchmark_spec.vms

  datastore_keyfile = benchmark_spec.datastore_instance.keyfile

  # Install required packages and copy credential files
  vm_util.RunThreaded(_Install, vms, datastore_keyfile)

  benchmark_spec.executor = ycsb.YCSBExecutor('googledatastore')


def Run(benchmark_spec):
  """Spawn YCSB and gather the results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample instances.
  """
  vms = benchmark_spec.vms
  run_kwargs = benchmark_spec.datastore_instance.run_kwargs
  run_kwargs['googledatastore.privateKeyFile'] = PRIVATE_KEYFILE_DIR
  run_kwargs['googledatastore.debug'] = FLAGS.google_datastore_debug
  load_kwargs = run_kwargs.copy()
  if FLAGS['ycsb_preload_threads'].present:
    load_kwargs['threads'] = FLAGS['ycsb_preload_threads']
  samples = list(benchmark_spec.executor.LoadAndRun(
      vms, load_kwargs=load_kwargs, run_kwargs=run_kwargs))
  return samples


def Cleanup(benchmark_spec):
  """Cleanup.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  # TODO(buggay): change when gcloud datastore delete is supported.
  benchmark_spec.datastore_instance.DeleteDatabase()


def _Install(vm, datastore_keyfile):
  vm.Install('ycsb')

  # Copy private key file to VM
  vm.RemoteCopy(datastore_keyfile, PRIVATE_KEYFILE_DIR)

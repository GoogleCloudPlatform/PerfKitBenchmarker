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

"""Run YCSB benchmark against Google Cloud Datastore

By default, this benchmark provision 1 single-CPU VM and spawn 1 thread
to test Datastore.
"""

import logging

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
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
      vm_spec: *default_single_core
      vm_count: 1
  flags:
    gcloud_scopes: >
      https://www.googleapis.com/auth/datastore"""
YCSB_BINDING_TAR_URL = ('https://storage.googleapis.com/datastore-ycsb/'
                        'ycsb-googledatastore-binding-0.13.0-SNAPSHOT.tar.gz')
REQUIRED_SCOPES = ('https://www.googleapis.com/auth/datastore')

FLAGS = flags.FLAGS
flags.DEFINE_string('google_datastore_datasetId',
                    None,
                    'The project ID that has Cloud Datastore service.')
flags.DEFINE_string('google_datastore_debug',
                    'false',
                    'The logging level when running YCSB.')


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
  benchmark_spec.always_call_cleanup = True
  default_ycsb_tar_url = ycsb.YCSB_TAR_URL
  vms = benchmark_spec.vms

  # TODO: figure out a less hacky way to override.
  # Override so that we only need to download the required binding.
  ycsb.YCSB_TAR_URL = YCSB_BINDING_TAR_URL

  # Install required packages and copy credential files
  vm_util.RunThreaded(_Install, vms)

  # Restore YCSB_TAR_URL
  ycsb.YCSB_TAR_URL = default_ycsb_tar_url

  benchmark_spec.executor = ycsb.YCSBExecutor('googledatastore')


def Run(benchmark_spec):
  vms = benchmark_spec.vms
  run_kwargs = {
      'googledatastore.datasetId': (FLAGS.google_datastore_datasetId
                                    if FLAGS.google_datastore_datasetId
                                    else util.GetDefaultProject()),
      'googledatastore.debug': FLAGS.google_datastore_debug,
  }
  load_kwargs = run_kwargs.copy()
  samples = list(benchmark_spec.executor.LoadAndRun(
      vms, load_kwargs=load_kwargs, run_kwargs=run_kwargs))
  return samples


def Cleanup(benchmark_spec):
  # TODO: support automatic cleanup.
  logging.warning(
      "For now, we can only manually delete all the entries via GCP portal.")


def _Install(vm):
  vm.Install('ycsb')

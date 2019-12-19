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
"""Run YCSB benchmark against Google Cloud Firestore.

Before running this benchmark, you have to download your P12
service account private key file to local machine, and pass the path
via 'google_firestore_keyfile' parameters to PKB.

Service Account email associated with the key file is also needed to
pass to PKB.

By default, this benchmark provision 1 single-CPU VM and spawn 1 thread
to test Firestore.
"""

import posixpath
import logging

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import ycsb

BENCHMARK_NAME = 'cloud_firestore_ycsb'
BENCHMARK_CONFIG = """
cloud_firestore_ycsb:
  description: >
      Run YCSB agains Google Cloud Firestore.
      Configure the number of VMs via --num-vms.
  vm_groups:
    default:
      vm_spec: *default_single_core
      vm_count: 1"""

YCSB_BINDING_TAR_URL = ('https://github.com/charlie-lobo/YCSB/releases'
                        '/download/0.17.0fs/'
                        'ycsb-googlefirestore-binding-0.17.0.tar.gz')
YCSB_BINDING_LIB_DIR = posixpath.join(ycsb.YCSB_DIR, 'lib')
PRIVATE_KEYFILE_DIR = '/tmp/key.p12'

FLAGS = flags.FLAGS
flags.DEFINE_string('google_firestore_keyfile', None,
                    'The path to Google API P12 private key file')
flags.DEFINE_string('google_firestore_project_id', None,
                    'The project ID that has Cloud Firestore service')
flags.DEFINE_string('google_firestore_debug', 'false',
                    'The logging level when running YCSB')


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS['ycsb_client_vms'].present:
    config['vm_groups']['default']['vm_count'] = FLAGS.ycsb_client_vms
  return config


def CheckPrerequisites(benchmark_config):
  # Before YCSB Cloud Firestore supports Application Default Credential,
  # we should always make sure valid credential flags are set.
  if not FLAGS.google_firestore_keyfile:
    raise ValueError('"google_firestore_keyfile" must be set')
  if not FLAGS.google_firestore_project_id:
    raise ValueError('"google_firestore_project_id" must be set ')


def Prepare(benchmark_spec):
  benchmark_spec.always_call_cleanup = True
  ycsb.SetYcsbTarUrl(YCSB_BINDING_TAR_URL)
  vms = benchmark_spec.vms

  # Install required packages and copy credential files
  vm_util.RunThreaded(_Install, vms)

  # Restore YCSB_TAR_URL
  ycsb.SetYcsbTarUrl(None)
  benchmark_spec.executor = ycsb.YCSBExecutor('googlefirestore')


def Run(benchmark_spec):
  vms = benchmark_spec.vms
  run_kwargs = {
      'googlefirestore.projectId': FLAGS.google_firestore_project_id,
      'googlefirestore.serviceAccountKey': PRIVATE_KEYFILE_DIR,
      'log4j.rootLogger': 'DEBUG' if FLAGS.google_firestore_debug else 'INFO',
  }
  load_kwargs = run_kwargs.copy()
  if FLAGS['ycsb_preload_threads'].present:
    load_kwargs['threads'] = FLAGS['ycsb_preload_threads']
  samples = list(
      benchmark_spec.executor.LoadAndRun(
          vms, load_kwargs=load_kwargs, run_kwargs=run_kwargs))
  return samples


def Cleanup(benchmark_spec):
  # TODO: support automatic cleanup.
  logging.warning(
      'For now, we can only manually delete all the entries via GCP portal.')


def _Install(vm):
  vm.Install('ycsb')

  # Copy private key file to VM
  vm.RemoteCopy(FLAGS.google_firestore_keyfile, PRIVATE_KEYFILE_DIR)

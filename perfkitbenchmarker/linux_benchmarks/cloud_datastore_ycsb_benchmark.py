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
import concurrent.futures

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import ycsb

from google.cloud import datastore
from google.oauth2 import service_account


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

_THREAD_POOL_WORKERS = 100
_BATCH_SIZE = 500
# the name of the database entity created when running datastore YCSB
# https://github.com/brianfrankcooper/YCSB/tree/master/googledatastore
_YCSB_COLLECTIONS = ['usertable']

FLAGS = flags.FLAGS
flags.DEFINE_string('google_datastore_keyfile', None,
                    'The path to Google API P12 private key file')
flags.DEFINE_string(
    'private_keyfile_dir', '/tmp/key.p12',
    'The directory path where the private key file is copied to on a VM.')
flags.DEFINE_string(
    'google_datastore_serviceAccount', None,
    'The service account email associated with'
    'datastore private key file')
flags.DEFINE_string('google_datastore_datasetId', None,
                    'The project ID that has Cloud Datastore service')
flags.DEFINE_string('google_datastore_debug', 'false',
                    'The logging level when running YCSB')
# the JSON keyfile is needed to validate credentials in the Cleanup phase
flags.DEFINE_string('google_datastore_deletion_keyfile', None,
                    'The path to Google API JSON private key file')


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS['ycsb_client_vms'].present:
    config['vm_groups']['default']['vm_count'] = FLAGS.ycsb_client_vms
  return config


def CheckPrerequisites(_):
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  # Before YCSB Cloud Datastore supports Application Default Credential,
  # we should always make sure valid credential flags are set.
  if not FLAGS.google_datastore_keyfile:
    raise ValueError('"google_datastore_keyfile" must be set')
  if not FLAGS.google_datastore_serviceAccount:
    raise ValueError('"google_datastore_serviceAccount" must be set')
  if not FLAGS.google_datastore_datasetId:
    raise ValueError('"google_datastore_datasetId" must be set ')


def Prepare(benchmark_spec):
  """Prepare the virtual machines to run cloud datastore.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  benchmark_spec.always_call_cleanup = True
  vms = benchmark_spec.vms

  # Install required packages and copy credential files
  vm_util.RunThreaded(_Install, vms)

  # Restore YCSB_TAR_URL
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
  run_kwargs = {
      'googledatastore.datasetId': FLAGS.google_datastore_datasetId,
      'googledatastore.privateKeyFile': FLAGS.private_keyfile_dir,
      'googledatastore.serviceAccountEmail':
          FLAGS.google_datastore_serviceAccount,
      'googledatastore.debug': FLAGS.google_datastore_debug,
  }
  load_kwargs = run_kwargs.copy()
  if FLAGS['ycsb_preload_threads'].present:
    load_kwargs['threads'] = FLAGS['ycsb_preload_threads']
  samples = list(benchmark_spec.executor.LoadAndRun(
      vms, load_kwargs=load_kwargs, run_kwargs=run_kwargs))
  return samples


def Cleanup(_):
  """Deletes all entries in a datastore database."""
  if FLAGS.google_datastore_deletion_keyfile:
    dataset_id = FLAGS.google_datastore_datasetId
    executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=_THREAD_POOL_WORKERS)
    credentials_path = FLAGS.google_datastore_deletion_keyfile

    logging.info('Attempting to delete all data in %s', dataset_id)

    credentials = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=datastore.client.Client.SCOPE,
    )

    db = datastore.Client(project=dataset_id, credentials=credentials)

    for kind in _YCSB_COLLECTIONS:
      while True:
        futures = []
        for entity in db.query(kind=kind).fetch(limit=_BATCH_SIZE):
          futures.append(executor.submit(db.delete(entity.key)))
        if not futures:
          logging.info('Deleted all data in %s', dataset_id)
          break
        logging.info('Enqueued %d deletes', len(futures))
        concurrent.futures.wait(
            futures, timeout=None, return_when=concurrent.futures.ALL_COMPLETED)
        logging.info('Finished %d deletes', len(futures))
  else:
    logging.warning('Manually delete all the entries via GCP portal.')


def _Install(vm):
  vm.Install('ycsb')

  # Copy private key file to VM
  vm.RemoteCopy(FLAGS.google_datastore_keyfile, FLAGS.private_keyfile_dir)

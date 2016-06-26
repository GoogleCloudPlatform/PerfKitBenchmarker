import json
import logging
import os
import pipes
import posixpath
import re
import subprocess

from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import ycsb


BENCHMARK_NAME = 'cloud_datastore_ycsb'
BENCHMARK_CONFIG = '''
cloud_datastore_ycsb:
  description: >
      Run YCSB agains Google Cloud Datastore.
      Configure the number of VMs via --num-vms.
  vm_groups:
    default:
      vm_spec: n1-standard-4
      vm_count: null'''

YCSB_BINDING_TAR_URL = 'https://github.com/brianfrankcooper/YCSB/releases/download/0.9.0/ycsb-googledatastore-binding-0.9.0.tar.gz'
YCSB_BINDING_LIB_DIR = posixpath.join(ycsb.YCSB_DIR, 'lib')
PRIVATE_KEYFILE_DIR = '/tmp/key.p12'

FLAGS = flags.FLAGS
flags.DEFINE_string('google_datastore_keyfile',
                    None,
                    'The path to Google API P12 private key file')
flags.DEFINE_string('google_datastore_serviceAccount',
                    None,
                    'The service account email associated with datastore private key file')
flags.DEFINE_string('google_datastore_datasetId',
                    None,
                    'The project ID that has Cloud Datastore service')


def GetConfig(user_config):
    return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites():
    # Before YCSB Cloud Datastore supports accessing Application Default Credential,
    # we should always make sure valid credential flags are set.
    if not (FLAGS.google_datastore_keyfile and FLAGS.google_datastore_serviceAccount):
        raise ValueError("'google_datastore_keyfile' and 'google_datastore_serviceAccount' must be set")


def Prepare(benchmark_spec):
    benchmark_spec.always_call_cleanup = True
    vms = benchmark_spec.vms
    # Install required packages and copy credential files
    vm_util.RunThreaded(_Install, vms)


def Run(benchmark_spec):
    executor = ycsb.YCSBExecutor('googledatastore')
    # TODO: support more flags, like thread, operationcount, recordcount, etc.
    if not FLAGS['ycsb_preload_threads'].present:
      load_kwargs['threads'] = 32
    samples = list(executor.LoadAndRun(vms,
                                       load_kwargs=load_kwargs,
                                       run_kwargs=run_kwargs))
    return samples


def Cleanup(benchmark_spec):
    # For now, we can only manually delete all the entries via GCP portal.
    return


def _Install(vm):
    # Override YCSB_TAR_URL so that we only need to download the binding we need.
    ycsb.YCSB_TAR_URL = YCSB_BINDING_TAR_URL
    vm.Install('ycsb')
    vm.Install('curl')
    
    # Copy private key file to VM
    vm.RemoteCopy(FLAGS.google_datastore_keyfile, PRIVATE_KEYFILE_DIR)

"""Runs a python script on gcsfuse data."""

import logging
import os
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

flags.DEFINE_string(
    'gcsfuse_data_dir', '',
    'The GCS directory storing the files for the benchmark, such as '
    'gs://bucket/dir/')

BENCHMARK_NAME = 'gcsfuse'
BENCHMARK_CONFIG = """
gcsfuse:
  description: >
    Read GCS data via gcsfuse. Specify the number of VMs with --num_vms.
  vm_groups:
    default:
      disk_spec:
        GCP:
          disk_type: object_storage
          mount_point: /gcs
      vm_spec:
        GCP:
          machine_type: n1-standard-96
          zone: us-central1-c
          image_family: tf-latest-gpu
          image_project: deeplearning-platform-release
"""

_DLVM_PYTHON = '/opt/conda/bin/python'
_REMOTE_SCRIPTS_DIR = 'gcsfuse_scripts'
_REMOTE_SCRIPT = 'read.py'
_UNIT = 'MB/s'

FLAGS = flags.FLAGS


def GetConfig(user_config):
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Install and set up gcsfuse on all the VMs.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vm_util.RunThreaded(_Prepare, vms)


def _Prepare(vm):
  """Mount gcsfuse and set up the test script."""
  # Set up the test script
  path = data.ResourcePath(os.path.join(_REMOTE_SCRIPTS_DIR, _REMOTE_SCRIPT))
  logging.info('Uploading %s to %s', path, vm)
  vm.PushFile(path, _REMOTE_SCRIPT)
  vm.RemoteCommand(f'sudo chmod 777 {_REMOTE_SCRIPT}')


def Run(benchmark_spec):
  """Read the files concurrently and measure the throughput.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample instances.
  """
  vms = benchmark_spec.vms
  gfile_sample_lists = vm_util.RunThreaded(_ReadThroughputTestViaGfile, vms)
  gcsfuse_sample_lists = vm_util.RunThreaded(_ReadThroughputTestViaGcsfuse, vms)
  samples = []
  for sample_list in gfile_sample_lists + gcsfuse_sample_lists:
    samples.extend(sample_list)
  return samples


def _ReadThroughputTestViaGcsfuse(vm):
  metrics = _ReadThroughputTest(vm, vm.GetScratchDir() + '/')
  metadata = {
      'gcsfuse_version': FLAGS.gcsfuse_version,
      'gcsfuse_options': FLAGS.gcsfuse_options,
  }
  return [
      sample.Sample('gcsfuse read throughput', x, _UNIT, metadata)
      for x in metrics
  ]


def _ReadThroughputTestViaGfile(vm):
  metrics = _ReadThroughputTest(vm, '')
  return [sample.Sample('gfile read throughput', x, _UNIT) for x in metrics]


def _ReadThroughputTest(vm, mountpoint):
  """Read the files in the directory via tf.io.gfile or gcsfuse."""
  data_dir = FLAGS.gcsfuse_data_dir
  options = f'--mountpoint="{mountpoint}"'
  cmd = f'gsutil ls "{data_dir}" | {_DLVM_PYTHON} {_REMOTE_SCRIPT} {options}'
  logging.info(cmd)
  stdout, stderr = vm.RemoteCommand(cmd)
  logging.info(stdout)
  logging.info(stderr)
  return [float(line) for line in stdout.split('\n') if line]


def Cleanup(_):
  pass

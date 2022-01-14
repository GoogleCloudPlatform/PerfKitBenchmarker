# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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
# limitations under the License


r"""Runs a TensorFlow BigQuery Connector benchmark.

Benchmark measures the numbers of tensors/rows we can read per second from a
specific Dataset. It is a relative measure, but we can compare it with GCS
performance, and also observe how it changes over the time to catch
regressions and track improvements progress.

Sample command:
blaze run third_party/py/perfkitbenchmarker:pkb -- \
  --benchmarks=bigquery_tf_connector \
  --gce_network_name=default \
  --gce_subnet_name=default \
  --gcloud_scopes=https://www.googleapis.com/auth/bigquery
"""
import logging
import os
import re
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import sample
from perfkitbenchmarker.providers.gcp import util as gcp_util

flags.DEFINE_string('dataset_project_id', 'bigquery-public-data',
                    'GCP project where dataset is located.')
flags.DEFINE_string('dataset_id', 'baseball', 'Dataset id.')
flags.DEFINE_string('table_id', 'games_wide', 'Table id.')
flags.DEFINE_integer('requested_streams', 1, 'Number of streams.')
flags.DEFINE_integer('batch_size', 2048, 'Batch size.')
flags.DEFINE_enum('format', 'AVRO', ['AVRO', 'ARROW'],
                  'Serialization format - AVRO or ARROW')

BENCHMARK_NAME = 'bigquery_tf_connector'
BENCHMARK_CONFIG = """
bigquery_tf_connector:
  description: Runs a python script to benchmark TensorFlow BigQuery connector.
  flags:
    cloud: GCP
    gcloud_scopes: >
      https://www.googleapis.com/auth/bigquery
  vm_groups:
    default:
      vm_spec:
        GCP:
          machine_type: n1-standard-16
          zone: us-central1-f
          image_family: tf2-ent-2-3-cpu
          image_project: deeplearning-platform-release
"""

DLVM_PYTHON = '/opt/conda/bin/python'
REMOTE_SCRIPTS_DIR = 'bigquery_tensorflow_connector_test_scripts'
REMOTE_SCRIPT = 'test_runner.py'
UNIT = 'Rows/s'
FLAGS = flags.FLAGS
RESULT_MATCH_REGEX = r'^Benchmark result: \[(\d+\.?\d+)\] rows/s$'


def GetConfig(user_config):
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)
  Returns:
    loaded benchmark configuration
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Install and set up tensorflow io on the target VM."""
  vm = benchmark_spec.vms[0]
  path = data.ResourcePath(os.path.join(REMOTE_SCRIPTS_DIR, REMOTE_SCRIPT))
  logging.info('Uploading %s to %s', path, vm)
  vm.PushFile(path, REMOTE_SCRIPT)
  vm.RemoteCommand(f'sudo chmod 777 {REMOTE_SCRIPT}')


def _RunBenchmark(vm, streams=1, batch_size=2048, data_format='AVRO'):
  """Run a benchmark python script on a VM for given arguments.

  Args:
    vm: GCE VM instance where to run the benchmark.
    streams: Number of BigQuery streams to use.
    batch_size: batch size to use.
    data_format: serialization data format to use (AVRO or ARROW).
  Returns:
    Benchmark result.
  """
  project_id = FLAGS.project or gcp_util.GetDefaultProject()
  options = (f' --project_id="{project_id}"'
             f' --dataset_project_id="{FLAGS.dataset_project_id}"'
             f' --dataset_id="{FLAGS.dataset_id}"'
             f' --table_id="{FLAGS.table_id}"'
             f' --requested_streams="{streams}"'
             f' --batch_size="{batch_size}"'
             f' --format="{data_format}"')
  cmd = f'{DLVM_PYTHON} {REMOTE_SCRIPT} {options}'
  logging.info(cmd)
  stdout, stderr = vm.RemoteCommand(cmd)
  logging.info(stdout)
  logging.info(stderr)

  result = ''
  for line in stdout.split('\n'):
    m = re.match(RESULT_MATCH_REGEX, line)
    if m:
      result = m[1]
      break

  if not result:
    return []

  metadata = {
      'bq_requested_streams': streams,
      'bq_batch_size': batch_size,
      'bq_data_format': data_format,
      'dataset_project_id': FLAGS.dataset_project_id,
      'dataset_id': FLAGS.dataset_id,
      'table_id': FLAGS.table_id,
  }
  return [
      sample.Sample(
          'BigQuery TensorFlow connector read throughput',
          result,
          UNIT,
          metadata)
  ]


def Run(benchmark_spec):
  """Run a benchmark python script on a VM and returns results."""
  vm = benchmark_spec.vms[0]
  results = _RunBenchmark(vm,
                          streams=FLAGS.requested_streams,
                          batch_size=FLAGS.batch_size,
                          data_format=FLAGS.format)
  return results


def Cleanup(_):
  """Cleanup tensorflow_io on the VM."""
  pass

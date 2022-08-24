# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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

"""Runs an Apache Beam template on Dataflow processing data from Pub/Sub

For dataflow jobs, please build the dpb_job_jarfile based on
https://cloud.google.com/dataflow/docs/quickstarts/quickstart-java-maven
"""

import copy
import time
import datetime
from operator import add
import tempfile
import random
import string
import json
import logging

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.providers.gcp import gcp_dpb_dataflow
from perfkitbenchmarker.providers.gcp import util

BENCHMARK_NAME = 'dpb_df_template_benchmark'

BENCHMARK_CONFIG = """
dpb_df_template_benchmark:
  description: Run an Apache Beam template on Dataflow service
  dpb_service:
    service_type: dataflow_template
    worker_group:
      vm_spec:
        GCP:
          machine_type: n1-standard-4
      disk_spec:
        GCP:
          disk_size: 300
    worker_count: 1
"""

# Refer to flags with prefix 'dpb_df_template' defined in gcp provider flags.py
FLAGS = flags.FLAGS

# PubSub seek operation can take up to 1 minute to take full effect
# See https://cloud.google.com/pubsub/docs/replay-overview#seek_eventual_consistency
PUBSUB_SEEK_DELAY_MINUTES = 3
PUBSUB_SEEK_DELAY_SECONDS = PUBSUB_SEEK_DELAY_MINUTES * 60

def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  if FLAGS.dpb_df_template_gcs_location is None:
    raise errors.Config.InvalidValue(
        'Unspecified Dataflow template GCS location.')
  if FLAGS.dpb_df_template_input_subscription is None:
    raise errors.Config.InvalidValue(
        'Unspecified Pub/Sub subscription as input for Dataflow job.')

  # Get handle to the dpb service
  dpb_service_class = dpb_service.GetDpbServiceClass(
      benchmark_config.dpb_service.worker_group.cloud,
      benchmark_config.dpb_service.service_type)
  dpb_service_class.CheckPrerequisites(benchmark_config)


def Prepare(benchmark_spec):
  # Snapshot Pub/Sub input subscription to later restore at end of test
  suffix = ''.join(
    random.choice(string.ascii_lowercase + string.digits) for i in range(6))
  benchmark_spec.input_subscription_name = \
    FLAGS.dpb_df_template_input_subscription.split('/')[-1]
  benchmark_spec.input_subscription_snapshot_name = \
    f'{benchmark_spec.input_subscription_name}-{suffix}'

  cmd = util.GcloudCommand(None, 'pubsub', 'snapshots', 'create',
                          benchmark_spec.input_subscription_snapshot_name)
  cmd.flags = {
      'project':  util.GetDefaultProject(),
      'subscription': benchmark_spec.input_subscription_name,
      'format': 'json',
  }
  stdout, _, _ = cmd.Issue()
  snapshot_id = json.loads(stdout)[0]['snapshotId']
  logging.info('Prepare: Created snapshot %s for input subscription data',
      snapshot_id)
  pass

def Run(benchmark_spec):
  template_gcs_location = FLAGS.dpb_df_template_gcs_location
  output_ptransform = FLAGS.dpb_df_template_output_ptransform
  input_pubsub_id = FLAGS.dpb_df_template_input_subscription
  input_pubsub_name = benchmark_spec.input_subscription_name
  additional_args = FLAGS.dpb_df_template_additional_args

  # Get handle to the dpb service
  dpb_service_instance = benchmark_spec.dpb_service
  # TODO: set input pubsub name and id as job attributes

  # Create a file handle to contain the response from running the job on
  # the dpb service
  stdout_file = tempfile.NamedTemporaryFile(suffix='.stdout',
                                            prefix='dpb_df_template_benchmark',
                                            delete=False)
  stdout_file.close()

  # Pass template parameters
  template_params = []
  template_params.append(f'inputSubscription={input_pubsub_id}')
  for arg in additional_args:
    template_params.append(arg)

  results = []

  start_time = datetime.datetime.now()
  dpb_service_instance.SubmitJob(
      template_gcs_location=template_gcs_location,
      job_poll_interval=30,
      job_arguments=template_params,
      job_input_sub = input_pubsub_name)
  end_time = datetime.datetime.now()

  # Update metadata after job run to get job id
  metadata = copy.copy(dpb_service_instance.GetMetadata())
  metadata.update({'input_sub': input_pubsub_name})

  run_time = (end_time - start_time).total_seconds()
  results.append(sample.Sample('run_time', run_time, 'seconds', metadata))

  avg_cpu_util = dpb_service_instance.GetAvgCpuUtilization(
      start_time, end_time)
  results.append(sample.Sample('avg_cpu_util', avg_cpu_util, '%', metadata))

  max_throughput = dpb_service_instance.GetMaxOutputThroughput(
      output_ptransform, start_time, end_time)
  results.append(sample.Sample('max_throughput', max_throughput, '1/s',
      metadata))

  stats = dpb_service_instance.job_stats
  for name, value in stats.items():
    results.append(sample.Sample(name, value, 'number', metadata))

  total_cost = dpb_service_instance.CalculateCost(
      gcp_dpb_dataflow.DATAFLOW_TYPE_STREAMING)
  results.append(sample.Sample('total_cost', total_cost, '$', metadata))

  return results


def Cleanup(benchmark_spec):
  # Restore Pub/Sub to its original state before test started
  cmd = util.GcloudCommand(None, 'pubsub', 'subscriptions',
                          'seek',  benchmark_spec.input_subscription_name)
  cmd.flags = {
      'project':  util.GetDefaultProject(),
      'snapshot': benchmark_spec.input_subscription_snapshot_name,
      'format': 'json',
  }
  stdout, _, _ = cmd.Issue()
  snapshot_id = json.loads(stdout)['snapshotId']
  logging.info('Cleanup: Restore snapshot %s for input subscription data',
      snapshot_id)

  # Wait for seek operation to take full effect
  logging.info(
      'Cleanup: Waiting for Pub/Sub seek operation to take full effect (up to 1'
      ' minute)')
  time.sleep(PUBSUB_SEEK_DELAY_SECONDS)

  # Delete snapshot itself
  cmd = util.GcloudCommand(None, 'pubsub', 'snapshots', 'delete',
                          benchmark_spec.input_subscription_snapshot_name)
  cmd.flags = {
      'project':  util.GetDefaultProject(),
      'format': 'json',
  }
  stdout, _, _ = cmd.Issue()
  snapshot_id = json.loads(stdout)[0]['snapshotId']
  logging.info('Cleanup: Deleted snapshot %s', snapshot_id)
  pass


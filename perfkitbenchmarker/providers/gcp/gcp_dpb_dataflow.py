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
"""Module containing class for GCP's Dataflow service.
Use this module for running Dataflow jobs from compiled jar files.

No Clusters can be created or destroyed, since it is a managed solution
See details at: https://cloud.google.com/dataflow/
"""

from functools import cached_property
import os
import re
import time
import json
import logging
import datetime

from absl import flags
from google.cloud import monitoring_v3
from google.cloud.monitoring_v3.types import TimeInterval
from google.cloud.monitoring_v3.types import Aggregation
from google.cloud.monitoring_v3.types import ListTimeSeriesResponse
from perfkitbenchmarker import beam_benchmark_helper
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import util

flags.DEFINE_string(
    'dpb_dataflow_temp_location', None,
    'Cloud Storage path for Dataflow to stage most temporary files.')
flags.DEFINE_string(
    'dpb_dataflow_staging_location', None,
    'Google Cloud Storage bucket for Dataflow to stage the binary files. '
    'You must create this bucket ahead of time, before running your pipeline.')
flags.DEFINE_string('dpb_dataflow_runner', 'DataflowRunner',
                    'Flag to specify the pipeline runner at runtime.')
flags.DEFINE_string('dpb_dataflow_sdk', None,
                    'SDK used to build the Dataflow executable. The latest sdk '
                    'will be used by default.')


FLAGS = flags.FLAGS

DATAFLOW_WC_INPUT = 'gs://dataflow-samples/shakespeare/kinglear.txt'

# Compute Engine CPU Monitoring API has up to 4 minute delay.
# See https://cloud.google.com/monitoring/api/metrics_gcp#gcp-compute
CPU_API_DELAY_MINUTES = 4
CPU_API_DELAY_SECONDS = CPU_API_DELAY_MINUTES * 60
# Dataflow Monitoring API has up to 3 minute delay.
# See https://cloud.google.com/monitoring/api/metrics_gcp#gcp-dataflow
DATAFLOW_METRICS_DELAY_MINUTES = 3
DATAFLOW_METRICS_DELAY_SECONDS = DATAFLOW_METRICS_DELAY_MINUTES * 60

DATAFLOW_TYPE_BATCH = 'batch'
DATAFLOW_TYPE_STREAMING = 'streaming'

METRIC_TYPE_COUNTER = 'counter'
METRIC_TYPE_DISTRIBUTION = 'distribution'

# Dataflow resources cost factors (showing us-central-1 pricing).
# See https://cloud.google.com/dataflow/pricing#pricing-details
VCPU_PER_HR_BATCH = 0.056
VCPU_PER_HR_STREAMING = 0.069
MEM_PER_GB_HR_BATCH = 0.003557
MEM_PER_GB_HR_STREAMING = 0.0035557
PD_PER_GB_HR = 0.000054
PD_SSD_PER_GB_HR = 0.000298

class GcpDpbDataflow(dpb_service.BaseDpbService):
  """Object representing GCP Dataflow Service."""

  CLOUD = providers.GCP
  SERVICE_TYPE = 'dataflow'

  def __init__(self, dpb_service_spec):
    super(GcpDpbDataflow, self).__init__(dpb_service_spec)
    self.dpb_service_type = self.SERVICE_TYPE
    self.project = FLAGS.project
    self.job_id = None
    self.job_metrics = None

  @staticmethod
  def CheckPrerequisites(benchmark_config):
    del benchmark_config  # Unused
    if not FLAGS.dpb_job_jarfile or not os.path.exists(FLAGS.dpb_job_jarfile):
      raise errors.Config.InvalidValue('Job jar missing.')

  def Create(self):
    """See base class."""
    pass

  def Delete(self):
    """See base class."""
    pass

  # TODO(saksena): Make this actually follow the contract or better yet delete
  # this class.
  def SubmitJob(
      self,
      jarfile='',
      classname=None,
      job_poll_interval=None,
      job_arguments=None,
      job_stdout_file=None,
      job_type=None):
    """See base class."""

    if job_type == self.BEAM_JOB_TYPE:
      full_cmd, base_dir = beam_benchmark_helper.BuildBeamCommand(
          self.spec, classname, job_arguments)
      _, _, retcode = vm_util.IssueCommand(
          full_cmd,
          cwd=base_dir,
          timeout=FLAGS.beam_it_timeout,
          raise_on_failure=False)
      assert retcode == 0, 'Integration Test Failed.'
      return

    worker_machine_type = self.spec.worker_group.vm_spec.machine_type
    num_workers = self.spec.worker_count
    max_num_workers = self.spec.worker_count
    if (self.spec.worker_group.disk_spec and
        self.spec.worker_group.disk_spec.disk_size):
      disk_size_gb = self.spec.worker_group.disk_spec.disk_size
    elif self.spec.worker_group.vm_spec.boot_disk_size:
      disk_size_gb = self.spec.worker_group.vm_spec.boot_disk_size
    else:
      disk_size_gb = None

    cmd = []
    # Needed to verify java executable is on the path
    dataflow_executable = 'java'
    if not vm_util.ExecutableOnPath(dataflow_executable):
      raise errors.Setup.MissingExecutableError(
          'Could not find required executable "%s"' % dataflow_executable)
    cmd.append(dataflow_executable)

    cmd.append('-cp')
    cmd.append(jarfile)

    cmd.append(classname)
    cmd += job_arguments

    if FLAGS.dpb_dataflow_temp_location:
      cmd.append('--gcpTempLocation={}'.format(
          FLAGS.dpb_dataflow_temp_location))
    region = util.GetRegionFromZone(FLAGS.dpb_service_zone)
    cmd.append('--region={}'.format(region))
    cmd.append('--workerMachineType={}'.format(worker_machine_type))
    cmd.append('--numWorkers={}'.format(num_workers))
    cmd.append('--maxNumWorkers={}'.format(max_num_workers))

    if disk_size_gb:
      cmd.append('--diskSizeGb={}'.format(disk_size_gb))
    cmd.append('--defaultWorkerLogLevel={}'.format(FLAGS.dpb_log_level))
    cmd.append('--project={}'.format(self.project))
    _, stderr, _ = vm_util.IssueCommand(cmd)

    # Parse output to retrieve submitted job ID
    match = re.search('Submitted job: (.\S*)', stderr)
    if not match:
      logging.warn('Dataflow output in unexpected format. Failed to parse Dataflow job ID.')
      return
    
    self.job_id = match.group(1)
    logging.info('Dataflow job ID: %s', self.job_id)

  def SetClusterProperty(self):
    pass

  def GetMetadata(self):
    """Return a dictionary of the metadata for this cluster."""
    basic_data = super(GcpDpbDataflow, self).GetMetadata()
    basic_data['dpb_dataflow_runner'] = FLAGS.dpb_dataflow_runner
    basic_data['dpb_dataflow_sdk'] = FLAGS.dpb_dataflow_sdk
    basic_data['dpb_job_id'] = self.job_id
    return basic_data

  @cached_property
  def job_stats(self):
    """Collect series of relevant performance and cost stats."""
    stats = {}
    stats['total_vcpu_time'] = self.GetMetricValue('TotalVcpuTime')/3600         # vCPU-hr
    stats['total_mem_usage'] = self.GetMetricValue('TotalMemoryUsage')/1024/3600 # GB-hr
    stats['total_pd_usage'] = self.GetMetricValue('TotalPdUsage')/3600           # GB-hr
    # TODO(rarsan): retrieve BillableShuffleDataProcessed
    # and/or BillableStreamingDataProcessed when applicable
    return stats

  def CalculateCost(self, type=DATAFLOW_TYPE_BATCH):
    if type not in (DATAFLOW_TYPE_BATCH, DATAFLOW_TYPE_STREAMING):
      raise ValueError(f'Invalid type provided to CalculateCost(): {type}')

    total_vcpu_time = self.job_stats['total_vcpu_time']
    total_mem_usage = self.job_stats['total_mem_usage']
    total_pd_usage = self.job_stats['total_pd_usage']

    cost = 0
    if type == DATAFLOW_TYPE_BATCH:
      cost +=  total_vcpu_time * VCPU_PER_HR_BATCH
      cost +=  total_mem_usage * MEM_PER_GB_HR_BATCH
    else:
      cost += total_vcpu_time * VCPU_PER_HR_STREAMING
      cost += total_mem_usage * MEM_PER_GB_HR_STREAMING
    
    cost += total_pd_usage * PD_PER_GB_HR
    # TODO(rarsan): Add cost related to per-GB data processed by Dataflow Shuffle
    # (for batch) or Streaming Engine (for streaming) when applicable
    return cost

  def _PullJobMetrics(self, force_refresh=False):
    """Retrieve and cache all job metrics from Dataflow API"""
    # Skip if job metrics is already populated unless force_refresh is True
    if self.job_metrics is not None and not force_refresh:
      return
    # Skip if job id not available
    if self.job_id is None:
      logging.warn('Unable to pull job metrics. Job ID not available')
      return
    
    cmd = util.GcloudCommand(self, 'dataflow', 'metrics',
                            'list', self.job_id)
    cmd.use_alpha_gcloud = True
    cmd.flags = {
        'project': self.project,
        'region': util.GetRegionFromZone(FLAGS.dpb_service_zone),
        'format': 'json',
    }
    stdout, _, _ = cmd.Issue()
    results = json.loads(stdout)

    counters = {}
    distributions = {}
    for metric in results:
      if 'scalar' in metric:
        counters[metric['name']['name']] = int(metric['scalar'])
      elif 'distribution' in metric:
        distributions[metric['name']['name']] = metric['distribution']
      else:
        logging.warn(f'Unfamiliar metric type found: {metric}')

    self.job_metrics = {
      METRIC_TYPE_COUNTER: counters,
      METRIC_TYPE_DISTRIBUTION: distributions
    }

  def GetMetricValue(self, name, type=METRIC_TYPE_COUNTER):
    """Get value of a job's metric.
    
    Returns:
      Integer if metric is of type counter
      Dictionary if metric is of type distribution. Dictionary 
      contains keys such as count/max/mean/min/sum
    """
    if type not in (METRIC_TYPE_COUNTER, METRIC_TYPE_DISTRIBUTION):
      raise ValueError(f'Invalid type provided to GetMetricValue(): {type}')
    
    if self.job_metrics is None:
      self._PullJobMetrics()
    
    return self.job_metrics[type][name]

  def GetAvgCpuUtilization(self, start_time: datetime, end_time: datetime):
    """Get average cpu utilization across all pipeline workers.

    Args:
      start_time: datetime specifying the beginning of the time interval.
      end_time: datetime specifying the end of the time interval.

    Returns:
      Average value across time interval
    """
    client = monitoring_v3.MetricServiceClient()
    project_name = f'projects/{self.project}'
    
    now_seconds = int(time.time())
    start_time_seconds = int(start_time.timestamp())
    end_time_seconds = int(end_time.timestamp())
    # Cpu metrics data can take up to 240 seconds to appear
    if (now_seconds - end_time_seconds) < CPU_API_DELAY_SECONDS:
      logging.info('Waiting for CPU metrics to be available (up to 4 minutes)...')
      time.sleep(CPU_API_DELAY_SECONDS - (now_seconds - end_time_seconds))

    interval = TimeInterval(
        {
            'start_time': {'seconds': start_time_seconds},
            'end_time': {'seconds': end_time_seconds},
        }
    )

    api_filter = (
      'metric.type = "compute.googleapis.com/instance/cpu/utilization" '
      f'AND resource.labels.project_id = "{self.project}" '
      f'AND metadata.user_labels.dataflow_job_id = "{self.job_id}" '
    )

    aggregation = Aggregation(
        {
            'alignment_period': {'seconds': 60},  # 1 minute
            'per_series_aligner': Aggregation.Aligner.ALIGN_MEAN,
            'cross_series_reducer': Aggregation.Reducer.REDUCE_MEAN,
            'group_by_fields': ['resource.instance_id'],
        }
    )

    results = client.list_time_series(
      request={
        'name': project_name,
        'filter': api_filter,
        'interval': interval,
        'view': monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        'aggregation': aggregation,
      }
    )

    if not results:
      logging.warn('No monitoring data found. Unable to calculate avg CPU utilization.')
      return None

    return self._GetAvgValueFromTimeSeries(results)

  def _GetAvgValueFromTimeSeries(self, time_series: ListTimeSeriesResponse):
    """Parses time series data and returns average across intervals.

    Args:
      time_series: time series of cpu fractional utilization returned by monitoring.

    Returns:
      Average value across intervals
    """
    points = []
    for time_interval in time_series:
      for snapshot in time_interval.points:
        points.append(snapshot.value.double_value)

    if points:
      # Average over all minute intervals captured
      averaged = sum(points) / len(points)
      # If metric unit is a fractional number between 0 and 1 (e.g. CPU utilization metric)
      # multiply by 100 to display a percentage usage.
      if time_series.unit == "10^2.%":
        averaged = round(averaged * 100, 2)
      return averaged
    
    return None

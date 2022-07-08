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
"""Module containing class for GCP's dataflow service.

No Clusters can be created or destroyed, since it is a managed solution
See details at: https://cloud.google.com/dataflow/
"""

import os
import re
import time

from absl import flags
from google.cloud import monitoring_v3
from google.cloud.monitoring_v3.types import TimeInterval
from google.cloud.monitoring_v3.types import Aggregation
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

GCP_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

DATAFLOW_WC_INPUT = 'gs://dataflow-samples/shakespeare/kinglear.txt'

# Compute Engine CPU Monitoring API has up to 4 minute delay. See
# https://cloud.google.com/monitoring/api/metrics_gcp#gcp-compute
CPU_API_DELAY_MINUTES = 4
CPU_API_DELAY_SECONDS = CPU_API_DELAY_MINUTES * 60

class GcpDpbDataflow(dpb_service.BaseDpbService):
  """Object representing GCP Dataflow Service."""

  CLOUD = providers.GCP
  SERVICE_TYPE = 'dataflow'

  def __init__(self, dpb_service_spec):
    super(GcpDpbDataflow, self).__init__(dpb_service_spec)
    self.project = util.GetDefaultProject()
    self.job_id = None

  @staticmethod
  def _GetStats(stdout):
    """Get Stats.

    TODO(saksena): Hook up the metrics API of dataflow to retrieve performance
    metrics when available
    """
    pass

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
    stdout, stderr, retcode = vm_util.IssueCommand(cmd)

    # Parse output to retrieve submitted job ID
    match = re.search('Submitted job: (.\S*)', stderr)
    if not match:
      raise Exception('Dataflow output in unexpected format.')
    self.job_id = match.group(1)
    # print('job_id: ', self.job_id)

  def SetClusterProperty(self):
    pass

  def GetMetadata(self):
    """Return a dictionary of the metadata for this cluster."""
    basic_data = super(GcpDpbDataflow, self).GetMetadata()
    basic_data['dpb_dataflow_runner'] = FLAGS.dpb_dataflow_runner
    basic_data['dpb_dataflow_sdk'] = FLAGS.dpb_dataflow_sdk
    basic_data['dpb_job_id'] = self.job_id
    return basic_data

  def GetAvgCpuUtilization(self, start_time, end_time):
    """Get average cpu utilization across all pipeline workers.

    Args:
      start_time: datetime specifying the beginning of the time interval.
      end_time: datetime specifying the end of the time interval.

    Returns:
      Average value across time interval
    """
    client = monitoring_v3.MetricServiceClient()
    project_name = f"projects/{self.project}"
    
    now_seconds = int(time.time())
    start_time_seconds = int(start_time.timestamp())
    end_time_seconds = int(end_time.timestamp())
    # Cpu metrics data can take up to 240 seconds to appear
    if (now_seconds - end_time_seconds) < CPU_API_DELAY_SECONDS:
      print('Waiting for CPU metrics to be available (up to 4 minutes)...')
      time.sleep(CPU_API_DELAY_SECONDS - (now_seconds - end_time_seconds))

    interval = TimeInterval(
        {
            "start_time": {"seconds": start_time_seconds},
            "end_time": {"seconds": end_time_seconds},
        }
    )
    # print("interval:\n", interval)

    api_filter = (
      'metric.type = "compute.googleapis.com/instance/cpu/utilization" '
      'AND resource.labels.project_id = "' + self.project + '" '
      'AND metadata.user_labels.dataflow_job_id = "' + self.job_id + '" '
    )
    # print('api_filter: ', api_filter)

    aggregation = Aggregation(
        {
            "alignment_period": {"seconds": 60},  # 1 minute
            "per_series_aligner": Aggregation.Aligner.ALIGN_MEAN,
            "cross_series_reducer": Aggregation.Reducer.REDUCE_MEAN,
            "group_by_fields": ["resource.instance_id"],
        }
    )

    results = client.list_time_series(
      request={
        "name": project_name,
        "filter": api_filter,
        "interval": interval,
        "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        "aggregation": aggregation,
      }
    )

    # print(results)
    # if not results:
    #   raise Exception('No monitoring data found. Unable to calculate avg CPU utilization')
    return self._ParseTimeSeriesData(results)

  def _ParseTimeSeriesData(self, time_series):
    """Parses time series data and returns average across intervals.

    Args:
      time_series: time series of cpu fractional utilization returned by monitoring.

    Returns:
      Average value across intervals
    """
    points = []
    for i, time_interval in enumerate(time_series):
      for j, snapshot in enumerate(time_interval.points):
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
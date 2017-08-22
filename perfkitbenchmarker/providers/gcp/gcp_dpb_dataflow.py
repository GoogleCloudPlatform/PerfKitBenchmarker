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

from perfkitbenchmarker import beam_benchmark_helper
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util

flags.DEFINE_string('dpb_dataflow_staging_location', None,
                    'Google Cloud Storage bucket for Dataflow to stage the '
                    'binary and any temporary files. You must create this '
                    'bucket ahead of time, before running your pipeline.')
flags.DEFINE_string('dpb_dataflow_runner', 'DataflowRunner',
                    'Flag to specify the pipeline runner at runtime.')
flags.DEFINE_string('dpb_dataflow_sdk', None,
                    'SDK used to build the Dataflow executable.')


FLAGS = flags.FLAGS

GCP_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

DATAFLOW_WC_INPUT = 'gs://dataflow-samples/shakespeare/kinglear.txt'


class GcpDpbDataflow(dpb_service.BaseDpbService):
  """Object representing GCP Dataflow Service."""

  CLOUD = providers.GCP
  SERVICE_TYPE = 'dataflow'

  def __init__(self, dpb_service_spec):
    super(GcpDpbDataflow, self).__init__(dpb_service_spec)
    self.project = None

  @staticmethod
  def _GetStats(stdout):
    """
    TODO(saksena): Hook up the metrics API of dataflow to retrieve performance
    metrics when available
    """
    pass

  @staticmethod
  def CheckPrerequisites(benchmark_config):
    del benchmark_config  # Unused
    if not FLAGS.dpb_job_jarfile or not os.path.exists(FLAGS.dpb_job_jarfile):
      raise errors.Config.InvalidValue('Job jar missing.')
    if not FLAGS.dpb_dataflow_sdk:
      raise errors.Config.InvalidValue('Dataflow SDK version missing.')

  def Create(self):
    """See base class."""
    pass

  def Delete(self):
    """See base class."""
    pass

  def SubmitJob(self, jarfile, classname, job_poll_interval=None,
                job_arguments=None, job_stdout_file=None,
                job_type=None):
    """See base class."""

    if job_type == self.BEAM_JOB_TYPE:
      full_cmd, base_dir = beam_benchmark_helper.BuildBeamCommand(
          self.spec, classname, job_arguments)
      stdout, _, retcode = vm_util.IssueCommand(full_cmd, cwd=base_dir,
                                                timeout=FLAGS.beam_it_timeout)
      assert retcode == 0, "Integration Test Failed."
      return

    worker_machine_type = self.spec.worker_group.vm_spec.machine_type
    num_workers = self.spec.worker_count
    max_num_workers = self.spec.worker_count
    if self.spec.worker_group.disk_spec and \
            self.spec.worker_group.disk_spec.disk_size:
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

    cmd.append('--workerMachineType={}'.format(worker_machine_type))
    cmd.append('--numWorkers={}'.format(num_workers))
    cmd.append('--maxNumWorkers={}'.format(max_num_workers))

    if disk_size_gb:
      cmd.append('--diskSizeGb={}'.format(disk_size_gb))
    cmd.append('--defaultWorkerLogLevel={}'.format(FLAGS.dpb_log_level))
    stdout, _, _ = vm_util.IssueCommand(cmd)

  def SetClusterProperty(self):
    pass

  def GetMetadata(self):
    """Return a dictionary of the metadata for this cluster."""
    basic_data = super(GcpDpbDataflow, self).GetMetadata()
    basic_data['dpb_dataflow_runner'] = FLAGS.dpb_dataflow_runner
    basic_data['dpb_dataflow_sdk'] = FLAGS.dpb_dataflow_sdk
    return basic_data

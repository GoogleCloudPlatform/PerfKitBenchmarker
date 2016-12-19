# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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
"""

from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS

GCP_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

DATAFLOW_WC_JAR = ('/Users/saksena/dev/first-dataflow/target/'
                   'first-dataflow-bundled-0.1.jar')

DATAFLOW_BLOCKING_RUNNER = 'BlockingDataflowPipelineRunner'


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

  def Create(self):
    pass

  def Delete(self):
    pass

  def SubmitJob(self, jarfile, classname, job_poll_interval=None,
                job_arguments=None, job_stdout_file=None,
                job_type=None):
    workerMachineType = self.spec.worker_group.vm_spec.machine_type
    numWorkers = self.spec.worker_count
    maxNumWorkers = self.spec.worker_count
    if self.spec.worker_group.disk_spec and \
            self.spec.worker_group.disk_spec.disk_size:
      diskSizeGb = self.spec.worker_group.disk_spec.disk_size
    elif self.spec.worker_group.vm_spec.boot_disk_size:
      diskSizeGb = self.spec.worker_group.vm_spec.boot_disk_size
    else:
      diskSizeGb = None

    cmd = []

    """Verify java executable is on the path"""
    dataflow_executable = 'java'
    if not vm_util.ExecutableOnPath(dataflow_executable):
      raise errors.Setup.MissingExecutableError(
          'Could not find required executable "%s"', dataflow_executable)
    cmd.append(dataflow_executable)

    cmd.append('-cp')

    """Verify the presence of executable jar file"""
    if not vm_util.FilePresent(jarfile):
      raise errors.Setup.MissingExecutableError(
          'Could not find required jarfile "%s"', jarfile)
    cmd.append(jarfile)

    cmd.append(classname)
    cmd += job_arguments

    cmd.append('--workerMachineType={workerMachineType}'.format(
        workerMachineType=workerMachineType))
    cmd.append('--numWorkers={numWorkers}'.format(
        numWorkers=numWorkers))
    cmd.append('--maxNumWorkers={maxNumWorkers}'.format(
        maxNumWorkers=maxNumWorkers))

    if diskSizeGb:
      cmd.append('--diskSizeGb={diskSizeGb}'.format(diskSizeGb=diskSizeGb))
    stdout, _, _ = vm_util.IssueCommand(cmd)

  def SetClusterProperty(self):
    pass

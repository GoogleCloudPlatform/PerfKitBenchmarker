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

from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS

GCP_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

DATAFLOW_BLOCKING_RUNNER = 'DataflowRunner'

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

  def Create(self):
    """See base class."""
    pass

  def Delete(self):
    """See base class."""
    pass

  def SubmitJob(self, classname, job_poll_interval=None,
                job_arguments=None, job_stdout_file=None,
                job_type=None):
    """See base class."""
    # TODO: Reenable when Dataflow ITs support speccing machines.
    # worker_machine_type = self.spec.worker_group.vm_spec.machine_type
    # num_workers = self.spec.worker_count
    # max_num_workers = self.spec.worker_count
    # if self.spec.worker_group.disk_spec and \
    #         self.spec.worker_group.disk_spec.disk_size:
    #   disk_size_gb = self.spec.worker_group.disk_spec.disk_size
    # elif self.spec.worker_group.vm_spec.boot_disk_size:
    #   disk_size_gb = self.spec.worker_group.vm_spec.boot_disk_size
    # else:
    #   disk_size_gb = None

    cmd = []

    # Needed to verify java executable is on the path
    dataflow_executable = self.mvn_command

    if not vm_util.ExecutableOnPath(dataflow_executable):
      raise errors.Setup.MissingExecutableError(
          'Could not find required executable "%s"' % dataflow_executable)
    cmd.append(dataflow_executable)

    cmd.append('-e')
    cmd.append('verify')
    cmd.append('-Dit.test={}'.format(classname))
    cmd.append('-DskipITs=false')
    cmd.append('-Pdataflow-runner')

    if FLAGS.dpb_beam_it_module:
      cmd.append('-pl {}'.format(FLAGS.dpb_beam_it_module))

    if FLAGS.dpb_beam_it_profile:
      cmd.append('-P{}'.format(FLAGS.dpb_beam_it_profile))

    beam_args = job_arguments if job_arguments else []

    # cmd.append('--workerMachineType={}'.format(worker_machine_type))
    # cmd.append('--numWorkers={}'.format(num_workers))
    # cmd.append('--maxNumWorkers={}'.format(max_num_workers))

    # if disk_size_gb:
    #   cmd.append('--diskSizeGb={}'.format(disk_size_gb))
    beam_args.append('"--runner=org.apache.beam.runners.'
                     'dataflow.testing.TestDataflowRunner"')
    beam_args.append('"--defaultWorkerLogLevel={}"'.format(FLAGS.dpb_log_level))
    cmd.append("-DintegrationTestPipelineOptions="
               "'[{}]'".format(','.join(beam_args)))
    full_cmd = ' '.join(cmd)
    stdout, _, _ = vm_util.IssueCommand([full_cmd],
                                        cwd=self.beam_dir,
                                        use_shell=True, timeout=600)

  def SetClusterProperty(self):
    pass

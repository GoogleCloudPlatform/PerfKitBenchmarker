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
"""Module containing class for GCP's spark service.

Spark clusters can be created and deleted.
"""

import datetime
import json
import re
import os

from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker.providers.gcp import util
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS

GCP_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

DATAFLOW_WC_JAR = ('/Users/saksena/dev/first-dataflow/target/'
                   'first-dataflow-bundled-0.1.jar')

class GcpDpbDataflow(dpb_service.BaseDpbService):
  """Object representing GCP Dataflow Service."""

  CLOUD = providers.GCP
  SERVICE_TYPE = 'dataflow'

  def __init__(self, dpb_service_spec):
    super(GcpDpbDataflow, self).__init__(dpb_service_spec)
    self.project =  None # self.spec.master_group.vm_spec.project

  @staticmethod
  def _GetStats(stdout):
    """
    TODO: Hook up the metrics API of dataflow to retrieve performance metrics
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

    full_cmd = 'java -cp {jarfile} {classname} ' \
               '--stagingLocation=gs://saksena-df/staging/ ' \
               '--output=gs://saksena-df/output ' \
               '--runner=BlockingDataflowPipelineRunner ' \
               '--workerMachineType={workerMachineType} ' \
               '--numWorkers={numWorkers} --maxNumWorkers={maxNumWorkers} ' \
               '--diskSizeGb=500'.\
      format(jarfile = jarfile, classname = classname,
             workerMachineType = workerMachineType, numWorkers = numWorkers,
             maxNumWorkers = maxNumWorkers)

    print 'Full Command:', full_cmd
    os.system(full_cmd)

  def SetClusterProperty(self):
    pass

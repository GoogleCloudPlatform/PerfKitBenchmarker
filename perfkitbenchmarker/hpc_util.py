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

"""HPC utility functions."""

from absl import flags
from perfkitbenchmarker import vm_util

flags.DEFINE_boolean('mpirun_allow_run_as_root', False,
                     'Whether to allow mpirun to be run by the root user.')


def CreateMachineFile(vms,
                      num_slots=lambda vm: vm.NumCpusForBenchmark(),
                      remote_path='MACHINEFILE',
                      mpi_vendor='openmpi'):
  """Create a file with the IP of each machine in the cluster on its own line.

  The file is then pushed to the provided path on the master vm.

  Pass in "num_slots=lambda vm: 0" to create a machine file without a defined
  number of slots.

  OpenMPI's format: "<host> slots=<slots>"
    https://www.open-mpi.org/faq/?category=running#mpirun-hostfile
  IntelMPI's format: "<host>:<slots>"
    https://software.intel.com/content/www/us/en/develop/articles/controlling-process-placement-with-the-intel-mpi-library.html

  Args:
    vms: The list of vms which will be in the cluster.
    num_slots: The function to use to calculate the number of slots
      for each vm. Defaults to vm.NumCpusForBenchmark()
    remote_path: remote path of the machine file. Defaults to MACHINEFILE
    mpi_vendor: Implementation of MPI.  Can be openmpi or intel.
  """

  def Line(vm, vm_name=None):
    vm_name = vm_name or vm.internal_ip
    slots = num_slots(vm)
    if not slots:
      return vm_name
    if mpi_vendor == 'intel':
      return f'{vm_name}:{slots}'
    return f'{vm_name} slots={slots}'

  with vm_util.NamedTemporaryFile(mode='w') as machine_file:
    master_vm = vms[0]
    machine_file.write(Line(master_vm, 'localhost') + '\n')
    for vm in vms[1:]:
      machine_file.write(Line(vm) + '\n')
    machine_file.close()
    master_vm.PushFile(machine_file.name, remote_path)

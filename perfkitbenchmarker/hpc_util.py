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

"""HPC utility functions"""

from perfkitbenchmarker import vm_util


def CreateMachineFile(vms, num_slots=lambda vm: vm.num_cpus,
                      remote_path='MACHINEFILE'):
  """Create a file with the IP of each machine in the cluster on its own line.

  The file is then pushed to the provided path on the master vm.

  Args:
    vms: The list of vms which will be in the cluster.
    num_slots: The function to use to calculate the number of slots
      for each vm. Defaults to vm.num_cpus
    remote_path: remote path of the machine file. Defaults to MACHINEFILE
  """
  with vm_util.NamedTemporaryFile() as machine_file:
    master_vm = vms[0]
    machine_file.write('localhost slots=%d\n' % num_slots(master_vm))
    for vm in vms[1:]:
      machine_file.write('%s slots=%d\n' % (vm.internal_ip,
                                            num_slots(vm)))
    machine_file.close()
    master_vm.PushFile(machine_file.name, remote_path)

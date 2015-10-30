# Copyright 2015 Canonical, Ltd. All rights reserved.
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

"""Class to represent a Juju Virtual Machine object.
"""

from perfkitbenchmarker import flags
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util

from perfkitbenchmarker.juju import utils

FLAGS = flags.FLAGS

# TODO: Support the virtual machine object to allow manual creation of machines
# for running Perfkit's built-in benchmarks?
# TODO: Make a dummy package of dummy objects to inherit from?


class JujuVirtualMachine(virtual_machine.BaseVirtualMachine):
    """A dummy object representing a Juju Virtual Machine."""

    CLOUD = 'Juju'
    # Subclasses should override the default image.
    DEFAULT_IMAGE = None

    CONTROLLER = FLAGS.controller

    ssh_port = 22
    remote_access_ports = [22]

    def __init__(self, vm_spec, network, firewall):
        """Initialize a Juju virtual machine.

        Args:
          vm_spec: virtual_machine.BaseVirtualMachineSpec object of the vm.
          network: network.BaseNetwork object corresponding to the VM.
          firewall: network.BaseFirewall object corresponding to the VM.
        """
        super(JujuVirtualMachine, self).__init__(vm_spec, network, firewall)

        if not utils.is_bootstrapped(self.CONTROLLER):
            utils.bootstrap(self.CONTROLLER)

        self.max_local_disks = 1
        self.local_disk_counter = 0

    def _Create(self):
        """Create a VM instance."""
        # TODO: use instance type/machine specs
        # utils.add_machine(self.CONTROLLER)

    @vm_util.Retry()
    def _PostCreate(self):
        """Get the instance's data."""
        # Call juju status --format=json and parse
        # status = utils.status(self.CONTROLLER)
        # logging.warn(status)

    def _Delete(self):
        """Delete a VM instance"""

    def WaitForBootCompletion(self):
        pass

    def OnStartup(self):
        pass

    def PrepareVMEnvironment(self):
        pass

    def AuthenticateVm(self):
        # """Authenticate a remote machine to access all peers."""
        pass

    def Install(self, package_name):
        """Installs a PerfKit package on the VM."""
        pass

# Copyright 2015 Google Inc. All rights reserved.
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

import clc
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.centurylink import util
from perfkitbenchmarker.centurylink import clc_disk


class CenturylinkVirtualMachine(virtual_machine.BaseVirtualMachine):
    """Object representing a Centurylink Virtual Machine."""

    DEFAULT_ZONE = 'NY1'
    DEFAULT_MACHINE_TYPE = 'UBUNTU-12-64-TEMPLATE'
    DEFAULT_IMAGE = 'ubuntu-14-04-x64'

    def __init__(self, vm_spec):
        """Initialize a Centurylink virtual machine."""

        super(CenturylinkVirtualMachine, self).__init__(vm_spec)
        self.server_name = ''
        self.disk_size = 1
        self.max_local_disks = 1
        self.local_disk_counter = 0
        self.location = self.zone

        env = util.GetDefaultCenturylinkEnv()
        self.user_name = env['USERNAME']
        self.password = env['PASSWORD']

    @classmethod
    def SetVmSpecDefaults(cls, vm_spec):
        """Updates the VM spec with cloud specific defaults."""

        if vm_spec.machine_type is None:
            vm_spec.machine_type = cls.DEFAULT_MACHINE_TYPE
        if vm_spec.zone is None:
            vm_spec.zone = cls.DEFAULT_ZONE
        if vm_spec.image is None:
            vm_spec.image = cls.DEFAULT_IMAGE

    def _Create(self):
        """Create a Centurylink VM instance."""

        print "*" * 25 + " Creating server " + "*" * 25
        env = util.GetDefaultCenturylinkEnv()
        group_name = env['GROUP']
        vm_name = env['VM_NAME']
        vm_desc = env['VM_DESC']
        cpu = int(env['CPU'])
        ram = int(env['RAM'])

        clc.v2.SetCredentials(self.user_name, self.password)
        alias = clc.v2.Account.GetAlias()
        d = clc.v2.Datacenter(self.location)

        clc.v2.Server.Create(name=vm_name, cpu=cpu, memory=ram, description=vm_desc,
                         group_id=d.Groups().Get(group_name).id,
                         template=d.Templates().Search(self.machine_type)[0].id,
                         network_id=d.Networks().networks[0].id).WaitUntilComplete()

        self.server_name = self.location + alias + vm_name + "01"

    @vm_util.Retry()
    def _PostCreate(self):
        """Get the instance's data."""

        # Add Public IP and assign ports
        print "*" * 25 + " Creating Public IP and assigning port 22 to it " + "*" * 25
        p = clc.v2.Server(self.server_name).PublicIPs()
        p.Add(ports=[{"protocol": "TCP", "port": 22}, {"protocol": "ICMP", "port": 0}]).WaitUntilComplete()
        print "*" * 25 + " Updating Public IP " + "*" * 25
        p.public_ips[0].Update().WaitUntilComplete()
        print "Public IP: " + p.public_ips[0].id

        self.ip_address = p.public_ips[0].id
        self.hostname = p.public_ips[0].id

    def _Delete(self):
        print "*" * 20 + " Deleting server " + "*" * 20
        c = clc.v2.Server(self.server_name)
        c.Delete().WaitUntilComplete()

    def _Exists(self):
        print "*" * 25 + " Checking whether server exists " + "*" * 25

        s = clc.v2.Server(self.server_name).status
        if s == "active":
            return True
        else:
            return False

    def CreateScratchDisk(self, disk_spec):
        print "*" * 25 + " Creating ScratchDisk " + "*" * 25
        clc_disk.CenturylinkDisk(self, disk_spec)


class DebianBasedCenturylinkVirtualMachine(
        CenturylinkVirtualMachine, linux_virtual_machine.DebianMixin):
    pass

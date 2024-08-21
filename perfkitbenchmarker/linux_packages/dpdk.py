# Copyright 2023 PerfKitBenchmarker Authors. All rights reserved.
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

"""Module containing hugepage allocation, dpdk installation, and nic binding to dpdk driver."""

import re
from perfkitbenchmarker import errors

DPDK_GIT_REPO = 'https://github.com/DPDK/dpdk.git'
DPDK_GCP_DRIVER_GIT_REPO = (
    'https://github.com/google/compute-virtual-ethernet-dpdk'
)
DPDK_AWS_DRIVER_GIT_REPO = 'https://github.com/amzn/amzn-drivers'
DPDK_AWS_VFIO_DRIVER_DIR = 'amzn-drivers/userspace/dpdk/enav2-vfio-patch'


def _Install(vm):
  """Install DPDK after installing dependencies and applying patches."""
  vm.Install('pip')
  vm.RobustRemoteCommand(f'git clone {DPDK_GIT_REPO}')
  if vm.CLOUD == 'GCP':
    # Get out of tree driver
    vm.RobustRemoteCommand(f'git clone {DPDK_GCP_DRIVER_GIT_REPO}')
    vm.RemoteCommand(
        'cp -r compute-virtual-ethernet-dpdk/* dpdk/drivers/net/gve'
    )
  # Installs DPDK.
  # Adding /usr/local/bin to PATH needed for amazonlinux2.
  vm.RobustRemoteCommand(
      'cd dpdk && sudo env PATH=$PATH:/usr/local/bin meson setup'
      ' -Dexamples=l3fwd,l2fwd build'
  )
  vm.RemoteCommand(
      'cd dpdk && yes | sudo env PATH=$PATH:/usr/local/bin ninja install -C'
      ' build && sudo ldconfig'
  )
  _InsertVFIO(vm)
  _AllocateHugePages(vm)
  _BindNICToDPDKDriver(vm)
  vm.has_dpdk = True


def AptInstall(vm):
  """Install DPDK using apt. Tested on Ubuntu20."""
  vm.InstallPackages(
      'build-essential ninja-build meson git pciutils pkg-config'
      ' python3-pyelftools libnuma-dev'
  )
  # https://github.com/amzn/amzn-drivers/tree/master/userspace/dpdk#6-vfio-pci-and-igb_uio
  # Downgrade the kernel to the version where vfio-pci is distributed as a
  # module so the AWS vfio patch can be applied.
  if vm.CLOUD == 'AWS':
    vm.InstallPackages(
        'linux-image-5.4.0-1060-aws linux-headers-5.4.0-1060-aws'
        ' linux-tools-5.4.0-1060-aws'
    )
    vm.RemoteCommand(
        """sudo sed -i 's/GRUB_DEFAULT=0/GRUB_DEFAULT="1>2"/g' /etc/default/grub"""
    )
    vm.RemoteCommand('sudo update-grub')
    vm.Reboot()
    vm.RemoteCommand(f'git clone {DPDK_AWS_DRIVER_GIT_REPO}')
    vm.RemoteCommand(
        'sudo sed -i "s/# deb-src/deb-src/g" /etc/apt/sources.list'
    )
    vm.RemoteCommand('sudo apt update')
    vm.RemoteCommand(
        'sudo sed -i "s/linux-image-unsigned-/linux-image-/g"'
        f' {DPDK_AWS_VFIO_DRIVER_DIR}/get-vfio-with-wc.sh'
    )
    vm.RobustRemoteCommand(
        f'cd {DPDK_AWS_VFIO_DRIVER_DIR} && sudo ./get-vfio-with-wc.sh'
    )
  _Install(vm)


def YumInstall(vm):
  """Install DPDK using yum. Tested on Amazonlinux2023."""
  vm.Install('pip')
  vm.InstallPackages('git kernel-devel kernel-headers numactl-devel')
  vm.RemoteCommand('sudo pip3 install meson ninja pyelftools')
  if vm.CLOUD == 'AWS':
    vm.RemoteCommand(f'git clone {DPDK_AWS_DRIVER_GIT_REPO}')
    vm.RemoteCommand(
        'sudo sed -i "s/linux-image-unsigned-/linux-image-/g"'
        f' {DPDK_AWS_VFIO_DRIVER_DIR}/get-vfio-with-wc.sh'
    )
    vm.RobustRemoteCommand(
        f'cd {DPDK_AWS_VFIO_DRIVER_DIR} && sudo ./get-vfio-with-wc.sh'
    )
  _Install(vm)


def _InsertVFIO(vm):
  # Insert vfio-pci kernel module
  vm.RemoteCommand('sudo modprobe vfio-pci')
  # Disable IOMMU for VFIO
  vm.RemoteCommand(
      'echo 1 | sudo tee /sys/module/vfio/parameters/enable_unsafe_noiommu_mode'
  )


# TODO(user): Make generic in linux VM
def _AllocateHugePages(vm):
  """Allocates Huge Pages required for DPDK.

  Args:
    vm: The VM on which to install DPDK.
  """
  # Hugepage Allocation
  vm.RemoteCommand('sudo mkdir /mnt/huge')
  vm.RemoteCommand('sudo mount -t hugetlbfs -o pagesize=1G none /mnt/huge')
  if vm.numa_node_count == 1:
    vm.RemoteCommand(
        'echo 2048 | sudo tee'
        ' /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages'
    )
    vm.RemoteCommand(
        'echo 4 | sudo tee'
        ' /sys/kernel/mm/hugepages/hugepages-1048576kB/nr_hugepages'
    )
  else:
    for numa_node_num in range(vm.numa_node_count):
      vm.RemoteCommand(
          f'echo {int(2048/vm.numa_node_count)} | sudo tee'
          f' /sys/devices/system/node/node{numa_node_num}/hugepages/hugepages-2048kB/nr_hugepages'
      )
      vm.RemoteCommand(
          f'echo {int(4/vm.numa_node_count)} | sudo tee'
          f' /sys/devices/system/node/node{numa_node_num}/hugepages/hugepages-1048576kB/nr_hugepages'
      )


def _BindNICToDPDKDriver(vm):
  """Binds NIC's to the DPDK driver.

  Args:
    vm: The VM on which to install DPDK.
  """
  # Get non-primary NIC
  stdout, _ = vm.RemoteCommand('sudo ip addr')
  nic_match = re.search('3: ([a-zA-Z0-9]+):', stdout)
  if not nic_match:
    raise errors.VirtualMachine.VmStateError(
        'No secondary network interface. Make sure the VM has at least 2 NICs.'
    )
  secondary_nic = nic_match.group(1)

  # Get non-primary MAC Address
  mac_match = re.findall(r'link/ether ([a-z0-9:]*)', stdout)[1]
  if not mac_match:
    raise errors.VirtualMachine.VmStateError('No secondary MAC address.')
  vm.secondary_mac_addr = mac_match

  # Find bus info for secondary nic
  stdout, _ = vm.RemoteCommand(f'sudo ethtool -i {secondary_nic}')
  bus_match = re.search('bus-info: (0000:[0-9]+:[0-9]+.0)', stdout)
  if not bus_match:
    raise errors.VirtualMachine.VmStateError('No bus info for secondary NIC.')
  vm.secondary_nic_bus_info = bus_match.group(1)

  # Set secondary interface down
  vm.RemoteCommand(f'sudo ip link set {secondary_nic} down')

  # Bind secondary device to VFIO kernel module
  vm.RobustRemoteCommand(
      'sudo dpdk/usertools/dpdk-devbind.py -b vfio-pci'
      f' {vm.secondary_nic_bus_info}'
  )

  # Show bind status of NICs
  # Should see 1 NIC using kernel driver and 1 NIC using DPDK-compatible driver
  stdout, _ = vm.RemoteCommand('sudo dpdk/usertools/dpdk-devbind.py --status')
  match = re.search(f'{vm.secondary_nic_bus_info}.*drv=vfio-pci', stdout)
  if not match:
    raise errors.VirtualMachine.VmStateError(
        'No network device is using a DPDK-compatible driver.'
    )

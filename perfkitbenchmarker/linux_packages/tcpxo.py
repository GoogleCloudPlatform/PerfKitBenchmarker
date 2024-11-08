"""Module containing GPUDirect-TCPXO installation."""

import time


def Install(vm):
  """Install TCPXO on the VM."""
  # Install tcpxo, based on:
  # https://raw.githubusercontent.com/GoogleCloudPlatform/slurm-gcp/master/tools/prologs-epilogs/receive-data-path-manager-mega
  vm.RemoteCommand('sudo modprobe import-helper')
  vm.RemoteCommand('gcloud auth configure-docker --quiet us-docker.pkg.dev')
  vm.RemoteCommand(
      'docker run --rm --name nccl-installer --network=host '
      '-v /var/lib:/var/lib '
      'us-docker.pkg.dev/gce-ai-infra/gpudirect-tcpxo/'
      'nccl-plugin-gpudirecttcpx-dev:v1.0.4 install'
  )
  vm.RemoteCommand(
      'sudo sed -i '
      '"s|NCCL_FASTRAK_IFNAME=eth1,eth2,eth3,eth4,eth5,eth6,eth7,eth8|'
      'NCCL_FASTRAK_IFNAME=enp6s0,enp7s0,enp13s0,enp14s0,enp134s0,enp135s0,'
      'enp141s0,enp142s0|g" '
      '/var/lib/tcpxo/lib64/nccl-env-profile.sh'
  )
  vm.RemoteCommand(
      'sudo sed -i "s|NCCL_SOCKET_IFNAME=eth0|NCCL_SOCKET_IFNAME=enp0s12|g" '
      '/var/lib/tcpxo/lib64/nccl-env-profile.sh'
  )
  vm.RemoteCommand(
      'sudo sed -i "s|NCCL_FASTRAK_CTRL_DEV=eth0|'
      'NCCL_FASTRAK_CTRL_DEV=enp0s12|g" '
      '/var/lib/tcpxo/lib64/nccl-env-profile.sh'
  )
  vm.RemoteCommand(
      'echo "export NCCL_FASTRAK_LLCM_DEVICE_DIRECTORY=/dev/aperture_devices"'
      ' | sudo tee -a /var/lib/tcpxo/lib64/nccl-env-profile.sh'
  )
  vm.RemoteCommand(
      'docker run --detach --pull=always --rm '
      '--name receive-datapath-manager '
      '--cap-add=NET_ADMIN '
      '--network=host '
      '--privileged '
      '--gpus all '
      '--volume /var/lib/nvidia/lib64:/usr/local/nvidia/lib64 '
      '--volume /dev/dmabuf_import_helper:/dev/dmabuf_import_helper '
      '--env LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu '
      'us-docker.pkg.dev/gce-ai-infra/gpudirect-tcpxo/'
      'tcpgpudmarxd-dev:v1.0.10 '
      '--num_hops=2 --num_nics=8 --uid= --alsologtostderr'
  )
  time.sleep(20)
  vm.RemoteCommand('docker logs receive-datapath-manager')


def AptInstall(vm):
  Install(vm)

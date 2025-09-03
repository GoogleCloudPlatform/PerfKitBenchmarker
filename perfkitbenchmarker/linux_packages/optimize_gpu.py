"""Module containing GPU optimization logic."""

from absl import flags
from absl import logging
from perfkitbenchmarker.linux_packages import nvidia_driver

FLAGS = flags.FLAGS

_GPUS_PER_NODE = 8
TUNER = flags.DEFINE_boolean('enable_ofi_tuner', False, 'Enable aws ofi tuner')


def _CheckSupported(vm):
  """Check if the GPU type is supported."""
  # TODO(yuyanting): Add support for other GPU types.
  if nvidia_driver.GetGpuType(vm) not in (
      nvidia_driver.NVIDIA_H100, nvidia_driver.NVIDIA_H200):
    logging.warn('Skipping GPU optimization for non-H100, H200 GPU.')
    return False
  return True


def Install(vm):
  """Optimize GPU settings on the VM."""
  if not _CheckSupported(vm):
    return
  # Following:
  # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/optimize_gpu.html
  nvidia_driver.EnablePersistenceMode(vm)
  if nvidia_driver.GetGpuType(vm) == nvidia_driver.NVIDIA_H100:
    vm.RemoteCommand('sudo nvidia-smi -ac 2619,1980')
  elif nvidia_driver.GetGpuType(vm) == nvidia_driver.NVIDIA_H200:
    vm.RemoteCommand('sudo nvidia-smi -ac 3201,1980')
  # Consider moving driver installation to this module.


def GetContainerMounts(vm):
  if FLAGS.cloud == 'GCP' and vm.machine_type == 'a3-megagpu-8g':
    return [
        '/var/tmp:/var/tmp',
        '/var/lib/tcpxo/lib64:/var/lib/tcpxo/lib64',
        '/dev/aperture_devices:/dev/aperture_devices',
    ]
  return []


def MountFuse(vm, bucket, path):
  """Mount fuse for the container."""
  if FLAGS.cloud == 'GCP':
    vm.RemoteCommand(
        'sudo mount -t gcsfuse -o'
        ' allow_other,uid=$USER,gid=$USER,dir_mode=777,file_mode=777,implicit_dirs'
        f' {bucket} {path}'
    )
  elif FLAGS.cloud == 'AWS':
    vm.RemoteCommand(
        'sudo sed -i "s/#user_allow_other/user_allow_other/g" /etc/fuse.conf'
    )
    vm.RemoteCommand(
        'mount-s3 --allow-delete --allow-other --allow-overwrite '
        f'{bucket} {path}'
    )

  else:
    raise NotImplementedError()


def SetContainerEnv(vm):
  """Set container environment to use optimized network stack.

  For AWS p5, this method assumes EFA is installed inside the container
  (rather than through mount).
  For GCP a3-mega, tcpxo is installed following the guide and passed to the
  container through mount.

  Args:
    vm: VirtualMachine object.

  Returns:
    String of commands that can be run inside the container to set environment.
  """
  if not _CheckSupported(vm):
    return ''
  tuner = ''
  if TUNER.value:
    tuner = (
        'export NCCL_TUNER_PLUGIN=/opt/aws-ofi-nccl/lib/libnccl-ofi-tuner.so; '
        'export FI_EFA_FORK_SAFE=1; '
        'export NCCL_SOCKET_IFNAME=^docker,lo,veth_def_agent,eth; '
        'export NCCL_BUFFSIZE=8388608; '
        'export NCCL_P2P_NET_CHUNKSIZE=524288;')
  if FLAGS.cloud == 'AWS':
    return (
        'export LD_LIBRARY_PATH='
        # pylint: disable=anomalous-backslash-in-string
        r'/opt/aws-ofi-nccl/lib:/opt/amazon/efa:\$LD_LIBRARY_PATH; '
        # pylint: disable=anomalous-backslash-in-string
        r'export PATH=/opt/amazon/openmpi/bin/:\$PATH; '
        'export FI_PROVIDER=efa; export FI_EFA_USE_DEVICE_RDMA=1;'
    ) + tuner
  if FLAGS.cloud == 'GCP' and vm.machine_type == 'a3-megagpu-8g':
    return (
        'NCCL_LIB_DIR=/var/lib/tcpxo/lib64; '
        'source /var/lib/tcpxo/lib64/nccl-env-profile.sh; '
        'export NCCL_NET=FasTrak; '  # enforce using FasTrak
        'export NCCL_FASTRAK_CTRL_DEV=enp0s12; '
        # pylint: disable=line-too-long
        'export'
        ' NCCL_FASTRAK_IFNAME=enp6s0,enp7s0,enp13s0,enp14s0,enp134s0,enp135s0,enp141s0,enp142s0; '
        'export NCCL_SOCKET_IFNAME=enp0s12; '
        'export NCCL_FASTRAK_USE_SNAP=1; '
        'export NCCL_FASTRAK_USE_LLCM=1; '
        'export NCCL_FASTRAK_LLCM_DEVICE_DIRECTORY=/dev/aperture_devices; '
        # pylint: disable=anomalous-backslash-in-string
        r'export LD_LIBRARY_PATH=/var/lib/tcpxo/lib64:\$LD_LIBRARY_PATH; '
    )


def _BuildGCPTopoAwareHostFile(controller, nnodes):
  """Build topo aware hostfile.

  https://cloud.google.com/cluster-toolkit/docs/machine-learning/a3-mega-enable-gpudirect-tcpxo#run_nccl_test
  Args:
    controller: The controller VM
    nnodes: The number of nodes in the cluster
  """
  controller.RemoteCommand(
      'srun --mpi=pmi2 -n'
      f' {nnodes * _GPUS_PER_NODE} --ntasks-per-node={_GPUS_PER_NODE} bash -c'
      " 'curl -s"
      ' "http://metadata.google.internal/computeMetadata/v1/instance/attributes/physical_host"'
      ' -H "Metadata-Flavor: Google"; echo /$SLURMD_NODENAME\' | sort -t / -s'
      ' -k 1,4 | awk -F "/" \'{print $NF}\' >/var/tmp/hostfile'
  )


def BuildHostFile(controller, nnodes):
  # pylint: disable=protected-access
  if controller._RemoteFileExists('/var/tmp/hostfile'):
    return
  elif FLAGS.cloud == 'GCP':
    _BuildGCPTopoAwareHostFile(controller, nnodes)

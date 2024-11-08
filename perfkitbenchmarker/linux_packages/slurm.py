"""Module containing slurm installation and cleanup function."""

import os
import re
from perfkitbenchmarker import data
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import vm_util

SLURM_CONF_DIR = '/etc/slurm'


def AptInstall(vm):
  """Installs slurm."""
  # Tested on AWS AmazonLinux DLAMI
  # The following command replicate a similar environment on GCP DLVM image.

  @vm_util.Retry(
      poll_interval=30,
      timeout=-1,
      max_retries=5,
  )
  def _InstallAnsible(vm):
    vm.RemoteCommand('sudo pip3 install ansible')
    vm.RemoteCommand(
        'ansible-galaxy role install '
        'googlecloudplatform.google_cloud_ops_agents')
    vm.RemoteCommand(
        'ansible-pull -U https://github.com/GoogleCloudPlatform/slurm-gcp '
        '-C 5.9.1 -i localhost, --limit localhost --connection=local '
        '''--extra-vars "{'slurm_version':'22.05.9','reboot':false,'install_cuda':false,'install_ompi':false,'install_lustre':false,'install_gcsfuse':false}" '''
        'ansible/playbook.yml')
  _InstallAnsible(vm)
  vm.RemoteCommand(
      'arch=$(dpkg --print-architecture); '
      'curl -fSsL -O https://github.com/NVIDIA/enroot/releases/download/'
      'v3.4.1/enroot_3.4.1-1_${arch}.deb; '
      'curl -fSsL -O https://github.com/NVIDIA/enroot/releases/download/'
      'v3.4.1/enroot+caps_3.4.1-1_${arch}.deb; '
      'sudo apt-get update ; sudo apt-get install --assume-yes ./*.deb')
  vm.RemoteCommand('sudo mkdir /run/enroot; sudo chmod 755 /run/enroot')
  enroot_path = os.path.join(vm.GetScratchDir(), '${UID}/enroot/')
  vm.RemoteCommand(
      f'echo "ENROOT_RUNTIME_PATH    {enroot_path}/runtime" | '
      'sudo tee -a /etc/enroot/enroot.conf'
  )
  vm.RemoteCommand(
      f'echo "ENROOT_CACHE_PATH    {enroot_path}/cache" | '
      'sudo tee -a /etc/enroot/enroot.conf'
  )
  vm.RemoteCommand(
      f'echo "ENROOT_DATA_PATH    {enroot_path}/data" | '
      'sudo tee -a /etc/enroot/enroot.conf'
  )
  # Install pyxis
  vm.RemoteCommand(
      f'cd {linux_packages.INSTALL_DIR};'
      'git clone --depth 1 https://github.com/NVIDIA/pyxis.git && '
      'cd pyxis && sudo make install'
  )


def ConfigureSlurm(vms):
  """Configures slurm cluster."""
  cfg_path = data.ResourcePath('slurm.conf.j2')
  controller = vms[0]  # using 1st vm as controller host
  workers = vms
  tmp_slurm_cfg = os.path.join(linux_packages.INSTALL_DIR, 'slurm.conf')
  slurm_cfg = os.path.join(SLURM_CONF_DIR, 'slurm.conf')
  for vm in workers:
    lscpu = vm.CheckLsCpu()
    vm.RemoteCommand(
        'echo "required /usr/local/lib/slurm/spank_pyxis.so '
        'container_scope=global" | '
        'sudo tee /etc/slurm/plugstack.conf'
    )
    vm.RemoteCommand(
        f'mkdir -p {linux_packages.INSTALL_DIR}/slurmd', ignore_failure=True
    )
    vm.RenderTemplate(
        cfg_path,
        tmp_slurm_cfg,
        {
            'controller': controller.hostname,
            'workers': ','.join(worker.hostname for worker in vms[1:]),
            'cpus': vm.num_cpus,
            'user': vm.user_name,
            'sockets': lscpu.socket_count,
            'cores_per_socket': lscpu.cores_per_socket,
            'threads_per_core': lscpu.threads_per_core,
        },
    )
    cgroup_path = data.ResourcePath('cgroup.conf')
    vm.PushFile(cgroup_path, linux_packages.INSTALL_DIR)
    tmp_cgroup_cfg = os.path.join(linux_packages.INSTALL_DIR, 'cgroup.conf')
    vm.RemoteCommand(f'sudo mkdir {SLURM_CONF_DIR}', ignore_failure=True)
    vm.RemoteCommand(f'sudo cp {tmp_slurm_cfg} {SLURM_CONF_DIR}')
    # Do not overwrite the cgroup.conf file if already exists.
    vm.RemoteCommand(f'sudo cp -n {tmp_cgroup_cfg} {SLURM_CONF_DIR}')
    vm.RemoteCommand('sudo systemctl stop slurmd.service', ignore_failure=True)
    vm.RemoteCommand(f'sudo chmod 755 {slurm_cfg}')
    vm.RemoteCommand(f'sudo slurmd -f {slurm_cfg}')
  controller.RemoteCommand(f'sudo chmod 755 {linux_packages.INSTALL_DIR}')
  controller.RemoteCommand(
      f'sudo chown {controller.user_name} {linux_packages.INSTALL_DIR}')
  controller.RemoteCommand('sudo systemctl stop slurmctld.service',
                           ignore_failure=True)
  controller.RemoteCommand(f'sudo slurmctld -f {slurm_cfg}')

  # Setting up munge
  controller.RemoteCommand(
      f'sudo dd if=/dev/urandom of={linux_packages.INSTALL_DIR}/munge.key '
      'bs=1 count=1024')
  src_munge_key = f'{linux_packages.INSTALL_DIR}/munge.key'
  # Distribute key from controller to the rest
  for vm in vms[1:]:
    controller.MoveFile(vm, src_munge_key, linux_packages.INSTALL_DIR)
  # Setup permission and (re)start munged
  for vm in vms:
    vm.RemoteCommand(
        f'sudo cp {src_munge_key} /etc/munge')
    vm.RemoteCommand('sudo chmod 400 /etc/munge/munge.key')
    vm.RemoteCommand('sudo chown munge:munge /etc/munge/munge.key')
    vm.RemoteCommand('sudo pkill munged', ignore_failure=True)
    vm.RemoteCommand('sudo mkdir /run/munge', ignore_failure=True)
    vm.RemoteCommand('sudo munged --force')


def Running(vm):
  """Check if any slurm job is running."""
  output, _ = vm.RemoteCommand('sinfo')
  for line in output.splitlines():
    if not line:
      continue
    status = line.split()[4]
    if status in ('alloc', 'mix'):
      return True
  return False


# TODO(yuyanting) Figure out how to set slurm node priority, so the output
# always land on node0.
def GetController(vms):
  """Return the controller vm.

  e.g.
  ip-10-0-0-[28,175], return vm with hostname ip-10-0-0-28
  pkb-46b6c6e7-[0-1], return vm with hostname pkb-46b6c6e7-0
  Args:
    vms: List of virtual machine objects.

  Returns:
    VirtualMachine object representing controller vm.

  Raises:
    RuntimeError: if cannot find the controller vm.
  """
  output, _ = vms[0].RemoteCommand('sinfo')
  node_list = output.strip().split()[-1]
  prefix = node_list.split('[')[0]
  suffix = re.split(',|-', node_list.split('[')[1])[0]
  for vm in vms:
    if vm.hostname == prefix + suffix:
      return vm
  raise RuntimeError(f'Not able to find controller vm in {output}')

"""Install gcsfuse package."""

MNT = '/gcs'


# https://cloud.google.com/storage/docs/cloud-storage-fuse/quickstart-mount-bucket
def AptInstall(vm):
  """Installs the gcsfuse package and mounts gcsfuse.

  Args:
    vm: BaseVirtualMachine. VM to receive the scripts.
  """
  vm.RemoteCommand(
      'export GCSFUSE_REPO=gcsfuse-`lsb_release -c -s` && echo "deb'
      ' https://packages.cloud.google.com/apt $GCSFUSE_REPO main" | sudo tee'
      ' /etc/apt/sources.list.d/gcsfuse.list'
  )
  vm.RemoteCommand(
      'curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo'
      ' apt-key add -'
  )
  vm.RemoteCommand('sudo apt-get update')
  vm.InstallPackages('fuse gcsfuse')


# https://docs.cloud.google.com/storage/docs/cloud-storage-fuse/install#centosred-hatrocky-linux
def YumInstall(vm):
  """Installs the gcsfuse package and mounts gcsfuse.

  Args:
    vm: BaseVirtualMachine. VM to receive the scripts.
  """
  vm.RemoteCommand(
      'sudo tee /etc/yum.repos.d/gcsfuse.repo > /dev/null <<EOF\n'
      '[gcsfuse]\n'
      'name=gcsfuse (packages.cloud.google.com)\n'
      'baseurl=https://packages.cloud.google.com/yum/repos/gcsfuse-el7-x86_64\n'
      'enabled=1\n'
      'gpgcheck=1\n'
      'repo_gpgcheck=0\n'
      'gpgkey=https://packages.cloud.google.com/yum/doc/yum-key.gpg\n'
      '       https://packages.cloud.google.com/yum/doc/rpm-package-key.gpg\n'
      'EOF\n'
  )
  vm.InstallPackages('fuse')
  vm.RemoteCommand('sudo dnf install -y gcsfuse --nogpgcheck')

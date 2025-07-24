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

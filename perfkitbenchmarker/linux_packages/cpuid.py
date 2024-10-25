"""Module containing cpuid installation."""


def YumInstall(vm):
  """Installs CPUID on the VM."""
  vm.InstallPackages('sudo dnf install cpuid')


def AptInstall(vm):
  """Installs CPUID on the VM."""
  vm.RemoteCommand('sudo apt update')
  vm.RemoteCommand('sudo apt install cpuid')

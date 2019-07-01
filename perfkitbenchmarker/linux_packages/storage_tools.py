"""Module containing storage_tools installation.

This provides nvme-cli among other useful storage tools.
"""

def _Install(vm):
  vm.InstallPackages('nvme-cli')

def YumInstall(vm):
  _Install(vm)

def AptInstall(vm):
  _Install(vm)

def SwupdInstall(vm):
  vm.InstallPackages('storage-utils')

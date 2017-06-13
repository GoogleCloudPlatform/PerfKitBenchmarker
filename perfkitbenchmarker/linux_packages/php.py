"""Contains php-cli installation functions."""


def YumInstall(vm):
  vm.InstallPackages('php5-cli')


def AptInstall(vm):
  vm.InstallPackages('php5-cli')

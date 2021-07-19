"""Installs the ubuntu toolchain PPA."""

from perfkitbenchmarker import errors

# Key(s) that need to be added in order to use the toolchain repo
_REQUIRED_KEYS = ('1E9377A2BA9EF27F',)


def YumInstall(vm):
  del vm
  raise errors.Setup.InvalidConfigurationError('Only supported on debian')


def AptInstall(vm):
  # sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys
  vm.RemoteCommand('sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test')
  for key in _REQUIRED_KEYS:
    vm.RemoteCommand(
        'curl -sL '
        f'"http://keyserver.ubuntu.com/pks/lookup?op=get&search=0x{key}"'
        ' | sudo apt-key add')
  vm.RemoteCommand('sudo apt update')

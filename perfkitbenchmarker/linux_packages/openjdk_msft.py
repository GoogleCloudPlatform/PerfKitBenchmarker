# linux_packages/openjdk_msft.py


def AptInstall(vm):
  """
  Installs Microsoft Build of OpenJDK on an Ubuntu-based VM.

  Args:
      vm: The VM instance where the OpenJDK should be installed.
  """
  vm.RemoteCommand('ubuntu_release=$(lsb_release -rs)')

  vm.RemoteCommand(
      'wget https://packages.microsoft.com/config/ubuntu/${ubuntu_release}/packages-microsoft-prod.deb'
      ' -O packages-microsoft-prod.deb'
  )
  vm.RemoteCommand('sudo dpkg -i packages-microsoft-prod.deb')

  vm.RemoteCommand('sudo apt-get install -y apt-transport-https')
  vm.RemoteCommand('sudo apt-get update')
  vm.RemoteCommand('sudo apt-get install -y msopenjdk-21')

  vm.RemoteCommand('java -version')


def YumInstall(vm):
  """
  Installs Microsoft Build of OpenJDK on a RHEL/CentOS-based VM.

  Args:
      vm: The VM instance where the OpenJDK should be installed.
  """
  vm.RemoteCommand('sudo yum install -y wget')
  vm.RemoteCommand(
      'sudo rpm --import https://packages.microsoft.com/keys/microsoft.asc'
  )
  vm.RemoteCommand(
      'wget https://packages.microsoft.com/config/rhel/7/prod.repo -O'
      ' /etc/yum.repos.d/microsoft.repo'
  )
  vm.RemoteCommand('sudo yum update -y')

  vm.RemoteCommand('sudo yum install -y msopenjdk-17')

  vm.RemoteCommand('java -version')

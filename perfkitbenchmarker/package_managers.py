# Copyright 2014 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Module containing mixin classes for package management.

These classes allow installation on both Debian and RHEL based linuxes.
They also handle some intial setup (especially on RHEL based linuxes
since by default sudo commands without a tty don't work) and
can restore the VM to the state it was in before packages were
installed.

To install a package on a VM, just call vm.Install(package_name).
The package name is just the name of the package module (i.e. the
file name minus .py). The framework will take care of all cleanup
for you.
"""

import logging
import re

from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import packages
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS

RHEL = 'rhel'
DEBIAN = 'debian'

EPEL6_RPM = ('http://dl.fedoraproject.org/pub/epel/'
             '6/x86_64/epel-release-6-8.noarch.rpm')
EPEL7_RPM = ('http://dl.fedoraproject.org/pub/epel/'
             '7/x86_64/e/epel-release-7-5.noarch.rpm')

UPDATE_RETRIES = 5

FLAGS = flags.FLAGS

flags.DEFINE_enum('os_type', DEBIAN,
                  [DEBIAN, RHEL],
                  'The version of linux that the VM\'s os is based on. '
                  'This will determine the package manager used, among '
                  'other things.')
flags.DEFINE_bool('install_packages', True,
                  'Override for determining whether packages should be '
                  'installed. If this is false, no packages will be installed '
                  'on any VMs. This option should probably only ever be used '
                  'if you have already created an image with all relevant '
                  'packages installed.')


class BasePackageMixin(object):

  def __init__(self):
    self._installed_packages = set()
    super(BasePackageMixin, self).__init__()

  def Startup(self):
    """Performs any necessary setup on the VM specific to the OS."""
    pass

  def SnapshotPackages(self):
    """Grabs a snapshot of the currently installed packages."""
    pass

  def RestorePackages(self):
    """Restores the currently installed packages to those snapshotted."""
    pass

  def PackageCleanup(self):
    """Cleans up all installed packages.

    Deletes the temp directory, restores packages, and uninstalls all
    PerfKit packages.
    """
    for package_name in self._installed_packages:
      self.Uninstall(package_name)
    self.RestorePackages()
    self.RemoteCommand('rm -rf %s' % vm_util.VM_TMP_DIR)

  def Install(self, package_name):
    """Installs a PerfKit package on the VM."""
    pass

  def Uninstall(self, package_name):
    """Uninstalls a PerfKit package on the VM."""
    pass

  def GetPathToConfig(self, package_name):
    """Returns the path to the config file for PerfKit packages.

    This function is mostly useful when config files locations
    don't match across distributions (such as mysql). Packages don't
    need to implement it if this is not the case.
    """
    pass

  def GetServiceName(self, package_name):
    """Returns the service name of a PerfKit package.

    This function is mostly useful when service names don't
    match across distributions (such as mongodb). Packages don't
    need to implement it if this is not the case.
    """
    pass

  def SetupProxy(self):
    """Sets up proxy configuration variables for the cloud environment."""

    env_file = "/etc/environment"
    commands = []

    if FLAGS.http_proxy:
        commands.append("echo 'http_proxy=%s' | sudo tee -a %s" % (
            FLAGS.http_proxy, env_file))


    if FLAGS.https_proxy:
        commands.append("echo 'https_proxy=%s' | sudo tee -a %s" % (
            FLAGS.https_proxy, env_file))

    if FLAGS.ftp_proxy:
        commands.append("echo 'ftp_proxy=%s' | sudo tee -a %s" % (
            FLAGS.ftp_proxy, env_file))

    self.RemoteCommand(";".join(commands))


class YumMixin(BasePackageMixin):

  def Startup(self):
    """Eliminates the need to have a tty to run sudo commands."""
    if FLAGS.http_proxy or FLAGS.https_proxy or FLAGS.ftp_proxy:
        self.SetupProxy()
    self.RemoteCommand('echo \'Defaults:%s !requiretty\' | '
                       'sudo tee /etc/sudoers.d/pkb' % self.user_name,
                       login_shell=True)
    self.RemoteCommand('mkdir -p %s' % vm_util.VM_TMP_DIR)

  def InstallEpelRepo(self):
    """Installs the Extra Packages for Enterprise Linux repository."""
    try:
      self.InstallPackages('epel-release')
    except errors.VmUtil.SshConnectionError as e:
      stdout, _ = self.RemoteCommand('cat /etc/redhat-release')
      major_version = int(re.search('release ([0-9])', stdout).group(1))
      if major_version == 6:
        epel_rpm = EPEL6_RPM
      elif major_version == 7:
        epel_rpm = EPEL7_RPM
      else:
        raise e
      self.RemoteCommand('sudo rpm -ivh --force %s' % epel_rpm)

  def PackageCleanup(self):
    """Cleans up all installed packages.

    Performs the normal package cleanup, then deletes the file
    added to the /etc/sudoers.d directory during startup.
    """
    super(YumMixin, self).PackageCleanup()
    self.RemoteCommand('sudo rm /etc/sudoers.d/pkb')

  def SnapshotPackages(self):
    """Grabs a snapshot of the currently installed packages."""
    self.RemoteCommand('rpm -qa > %s/rpm_package_list' % vm_util.VM_TMP_DIR)

  def RestorePackages(self):
    """Restores the currently installed packages to those snapshotted."""
    self.RemoteCommand(
        'rpm -qa | grep --fixed-strings --line-regexp --invert-match --file '
        '%s/rpm_package_list | xargs --no-run-if-empty sudo rpm -e' %
        vm_util.VM_TMP_DIR,
        ignore_failure=True)

  def InstallPackages(self, packages):
    """Installs packages using the yum package manager."""
    self.RemoteCommand('sudo yum install -y %s' % packages)

  def InstallPackageGroup(self, package_group):
    """Installs a 'package group' using the yum package manager."""
    self.RemoteCommand('sudo yum groupinstall -y "%s"' % package_group)

  def Install(self, package_name):
    """Installs a PerfKit package on the VM."""
    if ((self.is_static and not self.install_packages) or
        not FLAGS.install_packages):
      return
    if package_name not in self._installed_packages:
      package = packages.PACKAGES[package_name]
      package.YumInstall(self)
      self._installed_packages.add(package_name)

  def Uninstall(self, package_name):
    """Uninstalls a PerfKit package on the VM."""
    package = packages.PACKAGES[package_name]
    if hasattr(package, 'YumUninstall'):
      package.YumUninstall(self)

  def GetPathToConfig(self, package_name):
    """Returns the path to the config file for PerfKit packages.

    This function is mostly useful when config files locations
    don't match across distributions (such as mysql). Packages don't
    need to implement it if this is not the case.
    """
    package = packages.PACKAGES[package_name]
    return package.YumGetPathToConfig(self)

  def GetServiceName(self, package_name):
    """Returns the service name of a PerfKit package.

    This function is mostly useful when service names don't
    match across distributions (such as mongodb). Packages don't
    need to implement it if this is not the case.
    """
    package = packages.PACKAGES[package_name]
    return package.YumGetServiceName(self)

  def SetupProxy(self):
    """Sets up proxy configuration variables for the cloud environment."""
    super(YumMixin, self).SetupProxy()
    yum_proxy_file = "/etc/yum.conf"

    if FLAGS.http_proxy:
        self.RemoteCommand("echo -e 'proxy= \"%s\";' | sudo tee -a %s" % (
            FLAGS.http_proxy, yum_proxy_file))


class AptMixin(BasePackageMixin):

  def Startup(self):
    """Runs apt-get update so InstallPackages shouldn't need to."""
    if FLAGS.http_proxy or FLAGS.https_proxy or FLAGS.ftp_proxy:
        self.SetupProxy()
    self.AptUpdate()
    self.RemoteCommand('mkdir -p %s' % vm_util.VM_TMP_DIR)

  @vm_util.Retry(max_retries=UPDATE_RETRIES)
  def AptUpdate(self):
    """Updates the package lists on VMs using apt."""
    try:
      self.RemoteCommand('sudo apt-get update')
    except errors.VmUtil.SshConnectionError as e:
      # If there is a problem, remove the lists in order to get rid of
      # "Hash Sum mismatch" errors (the files will be restored when
      # apt-get update is run again).
      self.RemoteCommand('sudo rm -r /var/lib/apt/lists/*')
      raise e

  def SnapshotPackages(self):
    """Grabs a snapshot of the currently installed packages."""
    self.RemoteCommand(
        'dpkg --get-selections > %s/dpkg_selections' % vm_util.VM_TMP_DIR)

  def RestorePackages(self):
    """Restores the currently installed packages to those snapshotted."""
    self.RemoteCommand('sudo dpkg --clear-selections')
    self.RemoteCommand(
        'sudo dpkg --set-selections < %s/dpkg_selections' % vm_util.VM_TMP_DIR)
    self.RemoteCommand('sudo DEBIAN_FRONTEND=\'noninteractive\' '
                       'apt-get --purge -y dselect-upgrade')

  @vm_util.Retry()
  def InstallPackages(self, packages):
    """Installs packages using the apt package manager."""
    try:
      install_command = ('sudo DEBIAN_FRONTEND=\'noninteractive\' '
                         '/usr/bin/apt-get -y install %s' % (packages))
      self.RemoteCommand(install_command)
    except errors.VmUtil.SshConnectionError as e:
      # TODO(user): Remove code below after Azure fix their package repository,
      # or add code to recover the sources.list
      self.RemoteCommand(
          'sudo sed -i.bk "s/azure.archive.ubuntu.com/archive.ubuntu.com/g" '
          '/etc/apt/sources.list')
      logging.info('Installing "%s" failed on %s. This may be transient. '
                   'Updating package list.', packages, self)
      self.AptUpdate()
      raise e

  def Install(self, package_name):
    """Installs a PerfKit package on the VM."""
    if package_name not in self._installed_packages:
      package = packages.PACKAGES[package_name]
      package.AptInstall(self)
      self._installed_packages.add(package_name)

  def Uninstall(self, package_name):
    """Uninstalls a PerfKit package on the VM."""
    package = packages.PACKAGES[package_name]
    if hasattr(package, 'AptUninstall'):
      package.AptUninstall(self)

  def GetPathToConfig(self, package_name):
    """Returns the path to the config file for PerfKit packages.

    This function is mostly useful when config files locations
    don't match across distributions (such as mysql). Packages don't
    need to implement it if this is not the case.
    """
    package = packages.PACKAGES[package_name]
    return package.AptGetPathToConfig(self)

  def GetServiceName(self, package_name):
    """Returns the service name of a PerfKit package.

    This function is mostly useful when service names don't
    match across distributions (such as mongodb). Packages don't
    need to implement it if this is not the case.
    """
    package = packages.PACKAGES[package_name]
    return package.AptGetServiceName(self)

  def SetupProxy(self):
    """Sets up proxy configuration variables for the cloud environment."""
    super(AptMixin, self).SetupProxy()
    apt_proxy_file = "/etc/apt/apt.conf"

    commands = []

    if FLAGS.http_proxy:
        commands.append("echo -e 'Acquire::http::proxy \"%s\";' |"
                        'sudo tee -a %s' % (FLAGS.http_proxy, apt_proxy_file))

    if FLAGS.https_proxy:
        commands.append("echo -e 'Acquire::https::proxy \"%s\";' |"
                        'sudo tee -a %s' % (FLAGS.https_proxy, apt_proxy_file))

    self.RemoteCommand(";".join(commands))

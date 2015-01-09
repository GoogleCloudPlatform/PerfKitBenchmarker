# Copyright 2014 Google Inc. All rights reserved.
# #
# # Licensed under the Apache License, Version 2.0 (the "License");
# # you may not use this file except in compliance with the License.
# # You may obtain a copy of the License at
# #
# #   http://www.apache.org/licenses/LICENSE-2.0
# #
# # Unless required by applicable law or agreed to in writing, software
# # distributed under the License is distributed on an "AS IS" BASIS,
# # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# # See the License for the specific language governing permissions and
# # limitations under the License.

"""Module containing mixin classes for package management.

These classes allow installation on both Debian and RHEL based linuxes.
They also handle some intial setup (especially on RHEL based linuxes
since by default sudo commands without a tty don't work) and
can restore the VM to the state it was in before packages were
installed.
"""

import gflags as flags

from perfkitbenchmarker import errors
from perfkitbenchmarker import packages
from perfkitbenchmarker import vm_util


RHEL = 'rhel'
DEBIAN = 'debian'

flags.DEFINE_enum('os_type', DEBIAN,
                  [DEBIAN, RHEL],
                  'The version of linux that the VM\'s os is based on. '
                  'This will determine the package manager used, among '
                  'other things.')


class BasePackageMixin(object):

  def Startup(self):
    pass

  def SnapshotPackages(self):
    pass

  def RestorePackages(self):
    pass

  def Install(self, package_name):
    pass

  def GetPathToConfig(self, package_name):
    pass

  def GetServiceName(self, package_name):
    pass


class YumMixin(BasePackageMixin):

  def Startup(self):
    self.RemoteCommand('sudo sed -i "/requiretty/d" /etc/sudoers',
                       login_shell=True)
    self.RemoteCommand('sudo rpm -ivh --force '
                       'http://dl.fedoraproject.org/pub/epel'
                       '/6/x86_64/epel-release-6-8.noarch.rpm')

  def InstallPackages(self, packages):
    self.RemoteCommand('sudo yum install -y %s' % packages)

  def InstallPackageGroup(self, package_group):
    self.RemoteCommand('sudo yum groupinstall -y "%s"' % package_group)

  def Install(self, package_name):
    package = packages.PACKAGES[package_name]
    package.YumInstall(self)

  def GetPathToConfig(self, package_name):
    package = packages.PACKAGES[package_name]
    return package.YumGetPathToConfig(self)

  def GetServiceName(self, package_name):
    package = packages.PACKAGES[package_name]
    return package.YumGetServiceName(self)


class AptMixin(BasePackageMixin):

  def Startup(self):
    self.AptUpdate()

  def AptUpdate(self):
    self.RemoteCommand('sudo apt-get update')

  @vm_util.Retry()
  def InstallPackages(self, packages):
    try:
      install_command = ('sudo DEBIAN_FRONTEND=\'noninteractive\' '
                         '/usr/bin/apt-get -y install %s' % (packages))
      self.RemoteCommand(install_command)
    except errors.VmUtil.SshConnectionError as e:
      self.AptUpdate()
      raise e

  def Install(self, package_name):
    package = packages.PACKAGES[package_name]
    package.AptInstall(self)

  def GetPathToConfig(self, package_name):
    package = packages.PACKAGES[package_name]
    return package.AptGetPathToConfig(self)

  def GetServiceName(self, package_name):
    package = packages.PACKAGES[package_name]
    return package.AptGetServiceName(self)


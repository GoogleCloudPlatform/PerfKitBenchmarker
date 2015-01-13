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


"""Module containing EPEL installation and cleanup functions."""

EPEL_RPM = ('http://dl.fedoraproject.org/pub/epel/'
            '6/x86_64/epel-release-6-8.noarch.rpm')


def YumInstall(vm):
  """Installs the Extra Packages for Enterprise Linux repository."""
  vm.RemoteCommand('sudo rpm -ivh --force %s' % EPEL_RPM)


def AptInstall(vm):
  """Debian based VMs don't have an EPEL package."""
  pass

# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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
"""Supported types of operating systems that a VM may host."""

from perfkitbenchmarker import flags


CENTOS7 = 'centos7'
DEBIAN = 'debian'
DEBIAN9 = 'debian9'
JUJU = 'juju'
RHEL = 'rhel'
UBUNTU_CONTAINER = 'ubuntu_container'
UBUNTU1404 = 'ubuntu1404'
UBUNTU1604 = 'ubuntu1604'
UBUNTU1604_CUDA9 = 'ubuntu1604_cuda9'
UBUNTU1710 = 'ubuntu1710'
WINDOWS = 'windows'

LINUX_OS_TYPES = [
    CENTOS7,
    DEBIAN,
    DEBIAN9,
    JUJU,
    RHEL,
    UBUNTU_CONTAINER,
    UBUNTU1404,
    UBUNTU1604,
    UBUNTU1604_CUDA9,
    UBUNTU1710]
WINDOWS_OS_TYPES = [WINDOWS]
ALL = LINUX_OS_TYPES + WINDOWS_OS_TYPES
BASE_OS_TYPES = [
    DEBIAN,
    RHEL,
    WINDOWS]

flags.DEFINE_enum(
    'os_type', DEBIAN, ALL,
    'The VM\'s OS type. Ubuntu\'s os_type can also be specified as "debian" '
    'because it is largely built on Debian and uses the same package manager. '
    'Likewise, CentOS\'s os_type can be "rhel". In general if two OS\'s use '
    'the same package manager, and are otherwise very similar, the same '
    'os_type may work on both of them.  However, more specific os_types (and '
    'associated VirtualMachine subclasses can be developed.')

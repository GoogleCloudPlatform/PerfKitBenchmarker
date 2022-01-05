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

from absl import flags

AMAZONLINUX2 = 'amazonlinux2'
CENTOS7 = 'centos7'
CENTOS8 = 'centos8'
CLEAR = 'clear'
COS = 'cos'
CORE_OS = 'core_os'
DEBIAN9 = 'debian9'
DEBIAN10 = 'debian10'
DEBIAN11 = 'debian11'
JUJU = 'juju'
RHEL7 = 'rhel7'
RHEL8 = 'rhel8'
UBUNTU_CONTAINER = 'ubuntu_container'
UBUNTU1604 = 'ubuntu1604'
UBUNTU1604_CUDA9 = 'ubuntu1604_cuda9'
UBUNTU1804 = 'ubuntu1804'
UBUNTU2004 = 'ubuntu2004'
WINDOWS2012_CORE = 'windows2012_core'
WINDOWS2016_CORE = 'windows2016_core'
WINDOWS2019_CORE = 'windows2019_core'
WINDOWS2012_DESKTOP = 'windows2012_desktop'
WINDOWS2016_DESKTOP = 'windows2016_desktop'
WINDOWS2019_DESKTOP = 'windows2019_desktop'
WINDOWS2019_SQLSERVER_2017_STANDARD = 'windows2019_desktop_sqlserver_2017_standard'
WINDOWS2019_SQLSERVER_2017_ENTERPRISE = 'windows2019_desktop_sqlserver_2017_enterprise'
WINDOWS2019_SQLSERVER_2019_STANDARD = 'windows2019_desktop_sqlserver_2019_standard'
WINDOWS2019_SQLSERVER_2019_ENTERPRISE = 'windows2019_desktop_sqlserver_2019_enterprise'
# Base-only OS types
DEBIAN = 'debian'
RHEL = 'rhel'
WINDOWS = 'windows'

# These operating systems have SSH like other Linux OSes, but no package manager
# to run Linux benchmarks without Docker.
# Because they cannot install packages, they only support VM life cycle
# benchmarks like cluster_boot.
CONTAINER_OS_TYPES = [
    CORE_OS,
    COS,
]

LINUX_OS_TYPES = CONTAINER_OS_TYPES + [
    AMAZONLINUX2,
    CENTOS7,
    CENTOS8,
    CLEAR,
    DEBIAN9,
    DEBIAN10,
    DEBIAN11,
    JUJU,
    RHEL7,
    RHEL8,
    UBUNTU_CONTAINER,
    UBUNTU1604,  # deprecated
    UBUNTU1604_CUDA9,
    UBUNTU1804,
    UBUNTU2004,
]
WINDOWS_OS_TYPES = [
    WINDOWS2012_CORE,
    WINDOWS2016_CORE,
    WINDOWS2019_CORE,
    WINDOWS2012_DESKTOP,
    WINDOWS2016_DESKTOP,
    WINDOWS2019_DESKTOP,
    WINDOWS2019_SQLSERVER_2017_STANDARD,
    WINDOWS2019_SQLSERVER_2017_ENTERPRISE,
    WINDOWS2019_SQLSERVER_2019_STANDARD,
    WINDOWS2019_SQLSERVER_2019_ENTERPRISE
]
ALL = LINUX_OS_TYPES + WINDOWS_OS_TYPES
BASE_OS_TYPES = [CLEAR, CORE_OS, DEBIAN, RHEL, WINDOWS]

# May change from time to time.
DEFAULT = UBUNTU1804

flags.DEFINE_enum('os_type', DEFAULT, ALL, 'The VM\'s OS type.')

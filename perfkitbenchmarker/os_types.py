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

AMAZONLINUX2 = 'amazonlinux2'
CENTOS7 = 'centos7'
CLEAR = 'clear'
COS = 'cos'
CORE_OS = 'core_os'
DEBIAN9 = 'debian9'
DEBIAN10 = 'debian10'
JUJU = 'juju'
RHEL = 'rhel'
UBUNTU_CONTAINER = 'ubuntu_container'
UBUNTU1604 = 'ubuntu1604'
UBUNTU1604_CUDA9 = 'ubuntu1604_cuda9'
UBUNTU1710 = 'ubuntu1710'
UBUNTU1804 = 'ubuntu1804'
WINDOWS = 'windows'
WINDOWS2012_CORE = 'windows2012'
WINDOWS2016_CORE = 'windows2016'
WINDOWS2019_CORE = 'windows2019'
WINDOWS2012_BASE = 'windows2012_base'
WINDOWS2016_BASE = 'windows2016_base'
WINDOWS2019_BASE = 'windows2019_base'

# Base-only OS types
DEBIAN = 'debian'

LINUX_OS_TYPES = [
    AMAZONLINUX2,
    CENTOS7,
    CLEAR,
    CORE_OS,
    COS,
    DEBIAN9,
    DEBIAN10,
    JUJU,
    RHEL,
    UBUNTU_CONTAINER,
    UBUNTU1604,
    UBUNTU1604_CUDA9,
    UBUNTU1710,
    UBUNTU1804,
]
WINDOWS_OS_TYPES = [
    WINDOWS,
    WINDOWS2012_CORE,
    WINDOWS2016_CORE,
    WINDOWS2019_CORE,
    WINDOWS2012_BASE,
    WINDOWS2016_BASE,
    WINDOWS2019_BASE,
]
ALL = LINUX_OS_TYPES + WINDOWS_OS_TYPES
BASE_OS_TYPES = [CLEAR, CORE_OS, DEBIAN, RHEL, WINDOWS]

# May change from time to time.
DEFAULT = UBUNTU1604

flags.DEFINE_enum('os_type', DEFAULT, ALL, 'The VM\'s OS type.')

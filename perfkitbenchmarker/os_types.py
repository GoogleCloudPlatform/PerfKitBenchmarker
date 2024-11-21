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
AMAZONLINUX2023 = 'amazonlinux2023'
AMAZON_NEURON = 'amazon_neuron'
CENTOS_STREAM9 = 'centos_stream9'
CLEAR = 'clear'
COS = 'cos'
COS_DEV = 'cos_dev'
COS109 = 'cos109'
COS105 = 'cos105'
CORE_OS = 'core_os'
DEBIAN11 = 'debian11'
DEBIAN11_BACKPORTS = 'debian11_backports'
DEBIAN12 = 'debian12'
FEDORA36 = 'fedora36'
FEDORA37 = 'fedora37'
RHEL8 = 'rhel8'
RHEL9 = 'rhel9'
ROCKY_LINUX8 = 'rocky_linux8'
ROCKY_LINUX8_OPTIMIZED = 'rocky_linux8_optimized'
ROCKY_LINUX9 = 'rocky_linux9'
ROCKY_LINUX9_OPTIMIZED = 'rocky_linux9_optimized'
UBUNTU2004 = 'ubuntu2004'
UBUNTU2004_EFA = 'ubuntu2004_efa'
UBUNTU2004_DL = 'ubuntu2004_dl'
DEBIAN12_DL = 'debian12_dl'
AMAZONLINUX2_DL = 'amazonlinux2_dl'
UBUNTU2204 = 'ubuntu2204'
UBUNTU2404 = 'ubuntu2404'
WINDOWS2016_CORE = 'windows2016_core'
WINDOWS2019_CORE = 'windows2019_core'
WINDOWS2022_CORE = 'windows2022_core'
WINDOWS2016_DESKTOP = 'windows2016_desktop'
WINDOWS2019_DESKTOP = 'windows2019_desktop'
WINDOWS2022_DESKTOP = 'windows2022_desktop'
WINDOWS2019_SQLSERVER_2017_STANDARD = (
    'windows2019_desktop_sqlserver_2017_standard'
)
WINDOWS2019_SQLSERVER_2017_ENTERPRISE = (
    'windows2019_desktop_sqlserver_2017_enterprise'
)
WINDOWS2019_SQLSERVER_2019_STANDARD = (
    'windows2019_desktop_sqlserver_2019_standard'
)
WINDOWS2019_SQLSERVER_2019_ENTERPRISE = (
    'windows2019_desktop_sqlserver_2019_enterprise'
)
WINDOWS2022_SQLSERVER_2019_STANDARD = (
    'windows2022_desktop_sqlserver_2019_standard'
)
WINDOWS2022_SQLSERVER_2019_ENTERPRISE = (
    'windows2022_desktop_sqlserver_2019_enterprise'
)
WINDOWS2022_SQLSERVER_2022_STANDARD = (
    'windows2022_desktop_sqlserver_2022_standard'
)
WINDOWS2022_SQLSERVER_2022_ENTERPRISE = (
    'windows2022_desktop_sqlserver_2022_enterprise'
)

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
    COS109,
    COS105,
    COS_DEV,
]

LINUX_OS_TYPES = CONTAINER_OS_TYPES + [
    AMAZONLINUX2,
    AMAZONLINUX2_DL,
    AMAZONLINUX2023,
    AMAZON_NEURON,
    CENTOS_STREAM9,
    CLEAR,
    DEBIAN11,
    DEBIAN11_BACKPORTS,
    DEBIAN12,
    DEBIAN12_DL,
    FEDORA36,
    FEDORA37,
    RHEL8,
    RHEL9,
    ROCKY_LINUX8,
    ROCKY_LINUX8_OPTIMIZED,
    ROCKY_LINUX9,
    ROCKY_LINUX9_OPTIMIZED,
    UBUNTU2004,
    UBUNTU2004_EFA,
    UBUNTU2004_DL,
    UBUNTU2204,
    UBUNTU2404,
]

WINDOWS_CORE_OS_TYPES = [
    WINDOWS2016_CORE,
    WINDOWS2019_CORE,
    WINDOWS2022_CORE,
]

WINDOWS_DESKOP_OS_TYPES = [
    WINDOWS2016_DESKTOP,
    WINDOWS2019_DESKTOP,
    WINDOWS2022_DESKTOP,
]

WINDOWS_SQLSERVER_OS_TYPES = [
    WINDOWS2019_SQLSERVER_2017_STANDARD,
    WINDOWS2019_SQLSERVER_2017_ENTERPRISE,
    WINDOWS2019_SQLSERVER_2019_STANDARD,
    WINDOWS2019_SQLSERVER_2019_ENTERPRISE,
    WINDOWS2022_SQLSERVER_2019_STANDARD,
    WINDOWS2022_SQLSERVER_2019_ENTERPRISE,
    WINDOWS2022_SQLSERVER_2022_STANDARD,
    WINDOWS2022_SQLSERVER_2022_ENTERPRISE,
]

CENTOS_TYPES = [CENTOS_STREAM9]
WINDOWS_OS_TYPES = (
    WINDOWS_CORE_OS_TYPES + WINDOWS_DESKOP_OS_TYPES + WINDOWS_SQLSERVER_OS_TYPES
)

AMAZONLINUX_TYPES = [
    AMAZONLINUX2,
    AMAZONLINUX2023,
    AMAZONLINUX2_DL,
    AMAZON_NEURON,
]

EL_OS_TYPES = [
    CENTOS_TYPES,
    RHEL8,
    RHEL9,
    ROCKY_LINUX8,
    ROCKY_LINUX8_OPTIMIZED,
    ROCKY_LINUX9,
    ROCKY_LINUX9_OPTIMIZED,
]

ALL = LINUX_OS_TYPES + WINDOWS_OS_TYPES
BASE_OS_TYPES = [CLEAR, CORE_OS, DEBIAN, RHEL, WINDOWS]

# May change from time to time.
DEFAULT = UBUNTU2404

flags.DEFINE_enum('os_type', DEFAULT, ALL, "The VM's OS type.")

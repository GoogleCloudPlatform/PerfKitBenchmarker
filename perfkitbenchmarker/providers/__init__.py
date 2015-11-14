# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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

from perfkitbenchmarker import import_util

from perfkitbenchmarker.providers.alicloud import ali_virtual_machine  # NOQA
from perfkitbenchmarker.providers.aws import aws_virtual_machine  # NOQA
from perfkitbenchmarker.providers.azure import azure_virtual_machine  # NOQA
from perfkitbenchmarker.providers.cloudstack import cloudstack_virtual_machine  # NOQA
from perfkitbenchmarker.providers.digitalocean import digitalocean_virtual_machine  # NOQA
from perfkitbenchmarker.providers.gcp import gce_virtual_machine  # NOQA
from perfkitbenchmarker.providers.kubernetes import kubernetes_virtual_machine  # NOQA
from perfkitbenchmarker.providers.openstack import os_virtual_machine  # NOQA
from perfkitbenchmarker.providers.rackspace import rackspace_virtual_machine  # NOQA


import_util.LoadModulesWithName(__path__, __name__, 'flags')

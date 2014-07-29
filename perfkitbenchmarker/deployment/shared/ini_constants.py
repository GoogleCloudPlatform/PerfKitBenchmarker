#!/usr/bin/env python
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

"""Constants used to parse the deployment .ini file."""



# Network section, option names.
SECTION_NETWORK = 'network'
OPTION_NET_NAME = 'name'
OPTION_TCP = 'tcp'
OPTION_UDP = 'udp'

# Cluster section, option names.
SECTION_CLUSTER = 'cluster'
OPTION_CLUSTER_TYPE = 'type'
OPTION_PROJECT = 'project'
OPTION_SETUP_MODULES = 'setup_modules'
OPTION_ADMIN_USER = 'admin_user'
OPTION_ZONE = 'zone'

# Node section, option names.
SECTION_NODE_PREFIX = 'node:'
OPTION_PACKAGE_PREFIX = 'package.'
OPTION_PD_PREFIX = 'pd.'
OPTION_STATIC_REF_PREFIX = '@'
OPTION_RUNTIME_REF_PREFIX = '$'
OPTION_COUNT = 'count'
OPTION_ENTRYPOINT = 'entrypoint'
OPTION_IMAGE = 'image'
OPTION_VM_TYPE = 'vm_type'

# Helpful collections.
ALL_OPTIONS = [
    OPTION_NET_NAME, OPTION_TCP, OPTION_UDP, OPTION_CLUSTER_TYPE,
    OPTION_SETUP_MODULES, OPTION_PROJECT, OPTION_ZONE, OPTION_ADMIN_USER,
    OPTION_COUNT, OPTION_IMAGE, OPTION_VM_TYPE, OPTION_ENTRYPOINT]

NODE_OPTIONS = [
    OPTION_PD_PREFIX, OPTION_COUNT, OPTION_IMAGE, OPTION_VM_TYPE,
    OPTION_ENTRYPOINT]

ALL_OPTION_PREFIXES = [
    OPTION_PACKAGE_PREFIX, OPTION_PD_PREFIX, OPTION_STATIC_REF_PREFIX]

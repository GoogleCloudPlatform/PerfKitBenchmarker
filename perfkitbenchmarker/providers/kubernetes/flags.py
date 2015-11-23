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

from perfkitbenchmarker import flags

flags.DEFINE_string('ceph_secret', None,
                    'Name of the Ceph Secret used by Kubernetes in order to '
                    'authenticate with Ceph. If provided, overrides keyring.')

flags.DEFINE_string('ceph_keyring', '/etc/ceph/keyring',
                    'Path to the Ceph keyring file.')

flags.DEFINE_string('rbd_pool', 'rbd',
                    'Name of RBD pool for Ceph volumes.')

flags.DEFINE_string('rbd_user', 'admin',
                    'Name of RADOS user.')

flags.DEFINE_list('ceph_monitors', [],
                  'IP addresses and ports of Ceph Monitors. '
                  'Must be provided when Ceph scratch disk is required. '
                  'Example: "127.0.0.1:6789,192.168.1.1:6789"')

flags.DEFINE_string('kubeconfig', '',
                    'Path to kubeconfig to be used by kubectl')

flags.DEFINE_string('kubectl', 'kubectl',
                    'Path to kubectl tool')

flags.DEFINE_string('username', 'root',
                    'User name that Perfkit will attempt to use in order to '
                    'SSH into Docker instance.')

flags.DEFINE_boolean('docker_in_privileged_mode', True,
                     'If set to True, will attempt to create Docker containers '
                     'in a privileged mode. Note that some benchmarks execute '
                     'commands which are only allowed in privileged mode.')

flags.DEFINE_list('kubernetes_nodes', [],
                  'IP addresses of Kubernetes Nodes. These need to be '
                  'accessible from the machine running Perfkit '
                  'benchmarker. Example: "10.20.30.40,10.20.30.41"')

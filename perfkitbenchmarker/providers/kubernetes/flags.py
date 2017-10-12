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

flags.DEFINE_string('username', 'root',
                    'User name that Perfkit will attempt to use in order to '
                    'SSH into Docker instance.')

flags.DEFINE_boolean('docker_in_privileged_mode', True,
                     'If set to True, will attempt to create Docker containers '
                     'in a privileged mode. Note that some benchmarks execute '
                     'commands which are only allowed in privileged mode.')
flags.DEFINE_boolean('kubernetes_anti_affinity', True,
                     'If set to True, PKB pods will not be scheduled on the '
                     'same nodes as other PKB pods.')
flags.DEFINE_multi_string('k8s_volume_parameters', None,
                          'A colon separated key-value pair that will be '
                          'added to Kubernetes storage class parameters.')
_K8S_PROVISIONERS = [
    'kubernetes.io/azure-disk', 'kubernetes.io/gce-pd', 'kubernetes.io/aws-ebs'
]
flags.DEFINE_enum('k8s_volume_provisioner', None, _K8S_PROVISIONERS,
                  'The name of the provisioner to use for K8s storage '
                  'classes.')

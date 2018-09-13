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
"""Module containing flags applicable across benchmark run on AWS."""

from perfkitbenchmarker import flags
from perfkitbenchmarker.providers.aws import util

flags.DEFINE_string('aws_user_name', 'ubuntu',
                    'This determines the user name that Perfkit will '
                    'attempt to use. This must be changed in order to '
                    'use any image other than ubuntu.')
flags.DEFINE_integer('aws_provisioned_iops', None,
                     'IOPS for Provisioned IOPS (SSD) volumes in AWS.')

flags.DEFINE_string('aws_emr_loguri', None,
                    'The log-uri parameter to pass to AWS when creating a '
                    'cluster.  If not set, a bucket will be created.')
flags.DEFINE_integer('aws_emr_job_wait_time', None,
                     'The time to wait for an EMR job to finish, in seconds')

flags.DEFINE_string('s3_custom_endpoint', None,
                    'If given, a custom endpoint to use for S3 transfers. If '
                    'this flag is not given, use the standard endpoint for the '
                    'storage region.')
flags.DEFINE_boolean('aws_spot_instances', False,
                     'Whether to use AWS spot instances for any AWS VMs.')
flags.DEFINE_float('aws_spot_price', None,
                   'The spot price to bid for AWS spot instances. Defaults '
                   'to on-demand price when left as None.')
flags.DEFINE_integer('aws_boot_disk_size', None,
                     'The boot disk size in GiB for AWS VMs.')
flags.DEFINE_string('kops', 'kops',
                    'The path to the kops binary.')
flags.DEFINE_string('aws_image_name_filter', None,
                    'The filter to use when searching for an image for a VM. '
                    'See usage details in aws_virtual_machine.py around '
                    'IMAGE_NAME_FILTER.')
flags.DEFINE_string('aws_image_name_regex', None,
                    'The Python regex to use to further filter images for a '
                    'VM. This applies after the aws_image_name_filter. See '
                    'usage details in aws_virtual_machine.py around '
                    'IMAGE_NAME_REGEX.')
flags.DEFINE_string('aws_preprovisioned_data_bucket', None,
                    'AWS bucket where pre-provisioned data has been copied.')
flags.DEFINE_string('redis_node_type',
                    'cache.m4.large',
                    'The AWS node type to use for cloud redis')
flags.DEFINE_string('aws_elasticache_failover_zone',
                    None,
                    'AWS elasticache failover zone')
flags.DEFINE_string('aws_efs_token', None,
                    'The creation token used to create the EFS resource. '
                    'If the file system already exists, it will use that '
                    'instead of creating a new one.')
flags.DEFINE_boolean('aws_delete_file_system', True,
                     'Whether to delete the EFS file system.')
flags.DEFINE_list('eks_zones', ['us-east-1a', 'us-east-1c'],
                  'The zones into which the EKS cluster will be deployed. '
                  'There must be at least two zones and all zones must be '
                  'from the same region.')
flags.register_validator('eks_zones',
                         util.EksZonesValidator)
flags.DEFINE_boolean('eks_verify_ssl', True,
                     'Whether to verify the ssl certificate when communicating '
                     'with the EKS service. This requires SNI support which is '
                     'not available in the SSL modules of Python < 2.7.9.')
flags.DEFINE_enum('efs_throughput_mode', 'provisioned',
                  ['provisioned', 'bursting'],
                  'The throughput mode to use for EFS.')
flags.DEFINE_float('efs_provisioned_throughput', 1024.0,
                   'The throughput limit of EFS (in MiB/s) when run in '
                   'provisioned mode.')

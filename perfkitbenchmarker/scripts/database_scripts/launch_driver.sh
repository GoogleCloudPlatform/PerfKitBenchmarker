#!/bin/bash

# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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

# This script drives launch_my_sql service which is a wrapper class for
# mysql_service_benchmark. 
# Driver provisions a new instance approximately every three days.
# The run phase is completed several times daily with results uploaded to
# a designated Google Cloud Storage Bucket.

# If desired, per second data graphs can be created and uploaded to a designated
# Google Cloud Storage Bucket.

# The values below are provisioned for realistic benchmarking.

# This script is designed for benchmarking purposes. Therefore, a value for
# a Google Cloud Storage Bucket must be passed in when called.

# Example call:
#    ./perfkitbenchmarker/scripts/database_scripts/launch_driver.sh bucket_name

# To utilize per second data graphs, include a storage bucket as a second input.
# Example:
#     ./perfkitbenchmarker/scripts/database_scripts/launch_driver.sh log_bucket_name graph_bucket_name


if [ -z $1 ]; then
  echo "Must pass in a cloud storage bucket."
  exit 1;
fi

while true
  do
  # provision, prepare phase of mysql_service
  run_uri=$(python perfkitbenchmarker/scripts/database_scripts/launch_mysql_service.py --sysbench_run_seconds="1200" --run_stage=provision,prepare --mysql_svc_db_instance_cores="16" --mysql_svc_oltp_table_size="12000000" --mysql_svc_oltp_tables_count="100" --mysql_instance_storage_size=1000 --additional_flags='"'"--cloud_storage_bucket=${1}"'"')
  # for 3 days
  for day in day1 day2 day3
  do
    # run phase for loop. 4 times daily
    for timeofday in morning afternoon evening latenight
    do
      # run only
      echo "BASH: In for loop. Executing run."
      if [ -z $2 ]; then
        python perfkitbenchmarker/scripts/database_scripts/launch_mysql_service.py --sysbench_run_seconds="1200" --run_stage=run --run_uri=${run_uri} --thread_count_list=1,2,4,8,16,32,64,128,256,512 --additional_flags='"'"--cloud_storage_bucket=${1}"'"'
      else
        python perfkitbenchmarker/scripts/database_scripts/launch_mysql_service.py --sysbench_run_seconds="1200" --run_stage=run --run_uri=${run_uri} --thread_count_list=1,2,4,8,16,32,64,128,256,512 --additional_flags='"'"--cloud_storage_bucket=${1}"'"' --per_second_graphs=${2}
      fi  
      # recalculate or use different method
      sleep 21600
    done
  done
  echo "BASH: Left for loop. Executing Cleanup."
  # cleanup, teardown
  python perfkitbenchmarker/scripts/database_scripts/launch_mysql_service.py --run_uri=${run_uri} --run_stage=cleanup,teardown --additional_flags='"'"--cloud_storage_bucket=${1}"'"'
  echo "BASH: Finished teardown."
done

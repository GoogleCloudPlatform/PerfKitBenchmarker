#!/bin/bash

# shellcheck disable=9002
# shellcheck disable=2086
# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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

# Runner to execute a generic sql script on a Big Query cluster.
# Arguments:
#   1. Connection details
#   2. SQL Script
#   3. Output destination details


# Script to execute on the cluster
export SCRIPT=$1

# Connection details
export REGION=$2
export DATABASE=$3
export TIMEOUT=$4

# Output location
export OUTPUT_BUCKET=$5

# Output and Error Log files
export SCRIPT_OUTPUT=$6
export SCRIPT_ERROR=$7

pid=""

output=$( aws athena --output=json --region=$REGION start-query-execution --query-string "$( cat $SCRIPT )" --query-execution-context Database=$DATABASE --result-configuration OutputLocation=$OUTPUT_BUCKET 2>${SCRIPT_ERROR} )
echo $output >> $SCRIPT_OUTPUT
execution_id=$( cut -d '"' -f4 <<< $(echo $output) )
state='RUNNING'
# Control the timeout via file last modified times, set the end file modified
# time to timeout seconds in the future.
touch -d "$TIMEOUT seconds" end_file
touch cur_file
while [ "$state" == 'RUNNING' ]
do
  output=$( aws athena --output=json --region=$REGION get-query-execution --query-execution-id=$execution_id) 2>${SCRIPT_ERROR}
  echo $output >> $SCRIPT_OUTPUT
  state=$( cut -d '"' -f10 <<< $(echo $output) )
  if [ "$state" == 'FAILED' ] || [ "$state" == 'CANCELLED' ]
  then
    error=$( cut -d '"' -f16 <<< $(echo $output) )
    echo $error > $SCRIPT_ERROR
    exit 126
  fi
  # Once the end_file is older than the cur_file, the timeout has been reached.
  if [ end_file -ot cur_file ]
  then
    aws athena --region=$REGION stop-query-execution --query-execution-id=$execution_id 2>${SCRIPT_ERROR}
    echo "Query timeout" > $SCRIPT_ERROR
    exit 1
  fi
  sleep 1
  touch cur_file
done

pid=$!

wait $pid

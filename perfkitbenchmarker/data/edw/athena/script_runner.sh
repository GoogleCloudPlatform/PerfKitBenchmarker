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

# Output location
export OUTPUT_BUCKET=$4

# Output and Error Log files
export SCRIPT_OUTPUT=$5
export SCRIPT_ERROR=$6

pid=""

output=$( aws athena --output=json --region=$REGION start-query-execution --query-string "$( cat $SCRIPT )" --query-execution-context Database=$DATABASE --result-configuration OutputLocation=$OUTPUT_BUCKET 2>${SCRIPT_ERROR} )
echo $output >> $SCRIPT_OUTPUT
execution_id=$( cut -d '"' -f4 <<< $(echo $output) )
state='RUNNING'
while [ "$state" == 'RUNNING' ]
do
  output=$( aws athena --output=json --region=$REGION get-query-execution --query-execution-id=$execution_id) 2>${SCRIPT_ERROR}
  echo $output >> $SCRIPT_OUTPUT
  state=$( cut -d '"' -f10 <<< $(echo $output) )
  sleep 1
done

pid=$!

wait $pid

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

# The driver script to launch arbitrary sql workloads on a Redshift cluster.
# In addition to the Authorization credentials, the scripts are passed as
# arguments
export REDSHIFT_HOST=$1
export REDSHIFT_PORT=5439
export REDSHIFT_DB=$2
export REDSHIFT_USER=$3
export REDSHIFT_PASSWORD=$4
shift 4
export REDSHIFT_SCRIPT_LIST=( "$@" )

pids=""
START_TIME=$SECONDS

for REDSHIFT_SCRIPT in "${REDSHIFT_SCRIPT_LIST[@]}"
do
  PGPASSWORD=$REDSHIFT_PASSWORD psql -h $REDSHIFT_HOST -p 5439 -d $REDSHIFT_DB -U $REDSHIFT_USER -f $REDSHIFT_SCRIPT > /dev/null &
  pid=$!
  pids="$pids $pid"
done

wait $pids

ELAPSED_TIME=$(($SECONDS - $START_TIME))

echo ${ELAPSED_TIME}

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


# Connection details
export BQ_PROJECT_ID=$1
export BQ_DATASET_ID=$2

# Script to execute on the cluster
export SCRIPT=$3

# Output and Error Log files
export SCRIPT_OUTPUT=$4
export SCRIPT_ERROR=$5
export COLLECT_OUTPUT=$6
export OUTPUT_TABLE=$7

pid=""

if [ "$COLLECT_OUTPUT" = true ] ; then
    cat $SCRIPT | bq --project_id=$BQ_PROJECT_ID --dataset_id=$BQ_DATASET_ID query --nouse_cache --nouse_legacy_sql --destination_table=$OUTPUT_TABLE 1>${SCRIPT_OUTPUT} 2>${SCRIPT_ERROR} &
else
    cat $SCRIPT | bq --project_id=$BQ_PROJECT_ID --dataset_id=$BQ_DATASET_ID query --nouse_cache --nouse_legacy_sql 1>${SCRIPT_OUTPUT} 2>${SCRIPT_ERROR} &
fi

pid=$!

wait $pid

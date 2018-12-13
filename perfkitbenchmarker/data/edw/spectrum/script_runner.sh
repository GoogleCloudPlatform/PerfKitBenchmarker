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

# Runner to execute a generic sql script on a Redshift cluster.
# Arguments:
#   1. Connection and Authentication details
#   2. SQL Script
#   3. Output destination details

#TODO(saksena) Duplicate code can be reused
# Connection and Authentication details
export SPECTRUM_HOST=$1
export SPECTRUM_PORT=5439
export SPECTRUM_DB=$2
export SPECTRUM_USER=$3
export SPECTRUM_PASSWORD=$4

# Script to execute on the cluster
export SPECTRUM_SCRIPT=$5

# Output and Error Log files
export SCRIPT_OUTPUT=$6
export SCRIPT_ERROR=$7


pids=""

PGPASSWORD=$SPECTRUM_PASSWORD psql -h $SPECTRUM_HOST -p 5439 -d $SPECTRUM_DB -U $SPECTRUM_USER -f $SPECTRUM_SCRIPT -v ON_ERROR_STOP=1 1>${SCRIPT_OUTPUT} 2>${SCRIPT_ERROR} &

pid=$!

wait $pid

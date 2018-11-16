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

# Runner to execute a generic sql script on a Azure SQL data warehouse cluster.
# Arguments:
#   1. Connection and Authentication details
#   2. SQL Script
#   3. Output destination details

# Connection and Authentication details
export SQL_SERVER=$1
export SQL_DB=$2
export SQL_USER=$3
export SQL_PASSWORD=$4

# Script to execute on the cluster
export SQL_SCRIPT=$5

# Output and Error Log files
export SCRIPT_OUTPUT=$6
export SCRIPT_ERROR=$7


pids=""

/opt/mssql-tools/bin/sqlcmd -S ${SQL_SERVER}.database.windows.net -d $SQL_DB -U $SQL_USER -P $SQL_PASSWORD -I -i $SQL_SCRIPT 1>${SCRIPT_OUTPUT} 2>${SCRIPT_ERROR} &

pid=$!

wait $pid

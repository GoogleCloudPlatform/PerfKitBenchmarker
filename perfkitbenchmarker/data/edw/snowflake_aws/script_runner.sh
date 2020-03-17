#!/bin/bash

# shellcheck disable=9002
# shellcheck disable=2086
# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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

# Runner to execute a generic sql script on a Snowflake cluster.
# Arguments:
#   1. Snowflake Named Connection
#   2. SQL Script

# Snowflake Named Connection
export CONNECTION=$1

# Script to execute on the cluster
export SNOWFLAKE_SQL_SCRIPT=$2

pids=""

~/bin/snowsql --connection $CONNECTION --filename $SNOWFLAKE_SQL_SCRIPT &

pid=$!

wait $pid

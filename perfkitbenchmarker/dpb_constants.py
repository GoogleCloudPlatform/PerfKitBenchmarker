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
"""Constants for Data Processing Backend Services.

In order to benchmark Data Processing Backend services such as Google
Cloud Platform's Dataproc and Dataflow or Amazon's EMR, we create a
BaseDpbService class.  Classes to wrap specific backend services are in
the corresponding provider directory as a subclass of BaseDpbService.
"""

# Job types that are supported on the dpb service backends
PYSPARK_JOB_TYPE = 'pyspark'
SPARKSQL_JOB_TYPE = 'spark-sql'
SPARK_JOB_TYPE = 'spark'
HADOOP_JOB_TYPE = 'hadoop'
DATAFLOW_JOB_TYPE = 'dataflow'
BEAM_JOB_TYPE = 'beam'
FLINK_JOB_TYPE = 'flink'

# List of supported data processing backend services
DATAPROC = 'dataproc'
DATAPROC_FLINK = 'dataproc_flink'
DATAPROC_GKE = 'dataproc_gke'
DATAPROC_SERVERLESS = 'dataproc_serverless'
DATAFLOW = 'dataflow'
DATAFLOW_TEMPLATE = 'dataflow_template'
EMR = 'emr'
EMR_SERVERLESS = 'emr_serverless'
GLUE = 'glue'
UNMANAGED_DPB_SVC_YARN_CLUSTER = 'unmanaged_dpb_svc_yarn_cluster'
UNMANAGED_SPARK_CLUSTER = 'unmanaged_spark_cluster'
KUBERNETES_SPARK_CLUSTER = 'kubernetes_spark_cluster'
KUBERNETES_FLINK_CLUSTER = 'kubernetes_flink_cluster'
UNMANAGED_SERVICES = [
    UNMANAGED_DPB_SVC_YARN_CLUSTER,
    UNMANAGED_SPARK_CLUSTER,
]

# Supported Dataproc GCE Cluster tiers
DATAPROC_STANDARD_TIER = 'standard'
DATAPROC_PREMIUM_TIER = 'premium'

# Supported Dataproc GCE Cluster Engine
DATAPROC_DEFAULT_ENGINE = 'default'
DATAPROC_LIGHTNING_ENGINE = 'lightningEngine'

# Supported Dataproc Lightning Engine Runtime
DATAPROC_LIGHTNING_ENGINE_DEFAULT_RUNTIME = 'default'
DATAPROC_LIGHTNING_ENGINE_NATIVE_RUNTIME = 'native'

# Default number of workers to be used in the dpb service implementation
DEFAULT_WORKER_COUNT = 2

# List of supported applications that can be enabled on the dpb service
FLINK = 'flink'
HIVE = 'hive'

# Metrics and Status related metadata
# TODO(pclay): Remove these after migrating all callers to SubmitJob
SUCCESS = 'success'
RUNTIME = 'running_time'
WAITING = 'pending_time'

HDFS_FS = 'hdfs'
GCS_FS = 'gs'
S3_FS = 's3'

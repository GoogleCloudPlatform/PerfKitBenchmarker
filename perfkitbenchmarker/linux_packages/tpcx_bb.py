# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing TPCx-BB kit installation functions and helpers.

TPCx-BB (TPC Express Benchmark BB, a.k.a. BigBench) is a big data analytics
benchmark of 30 SQL, UDF/UDTF, natural language processing and machine
learning queries over a retail data model. See https://www.tpc.org/tpcx-bb/.

The kit is distributed by the TPC behind an EULA click-through and cannot be
downloaded automatically. Before running benchmarks that depend on this package
you must download the kit zip from
https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp
and either:

* place it in a PKB data directory (e.g. perfkitbenchmarker/data/ or a path
  passed via --data_search_paths), or
* upload it to the preprovisioned data bucket of your cloud provider (see
  --gcp_preprovisioned_data_bucket and friends) as
  tpcx_bb/TPCX-BB-Tool.zip.

The kit is installed with vm.InstallPreprovisionedPackageData, which verifies
the sha256 of the stock v1.7.0 kit (PREPROVISIONED_DATA) unless
--preprovision_ignore_checksum is passed.

Besides the kit, this module installs the engine stack the kit's `spark_sql`
engine needs on top of a PKB Hadoop (HDFS + YARN) cluster:

* Apache Spark with Hive support (the `spark-sql` CLI), run on YARN in client
  mode, as the kit invokes `spark-sql --master yarn --deploy-mode client`.
* A Hive metastore that supports concurrent sessions (the throughput test runs
  several `spark-sql` processes at once): PostgreSQL on the leader, accessed
  through Spark's built-in Hive client, with the full Hive schema created up
  front. Letting DataNucleus create the schema lazily (autoCreateAll) can
  deadlock on PostgreSQL when two sessions create tables concurrently.
* A YARN configuration that lets concurrent queries share the cluster:
  capacity scheduler with fair ordering and the Spark external shuffle service
  for Spark dynamic allocation.

Only Debian based images are supported (PostgreSQL is installed with apt).
"""

import collections
import csv
import dataclasses
import json
import logging
import math
import posixpath
import re
import shlex

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import hadoop

FLAGS = flags.FLAGS

PACKAGE_NAME = 'tpcx_bb'

# File name of the TPCx-BB kit zip, in a PKB data directory or in the
# preprovisioned data bucket under PACKAGE_NAME/.
KIT_FILE = 'TPCX-BB-Tool.zip'
SPARK_VERSION = flags.DEFINE_string(
    'tpcx_bb_spark_version',
    '3.1.3',
    'Apache Spark 3.x version installed for the TPCx-BB spark_sql engine. The '
    'default matches the Spark 3.1 machine learning jar shipped with the kit '
    '(Scala 2.12). Downloaded from the Apache archive with Hive support.',
)
flags.register_validator(
    'tpcx_bb_spark_version',
    lambda v: re.fullmatch(r'3\.\d+\.\d+', v) is not None,
    message='Must be a Spark 3.x.y version.',
)

# sha256 of the unmodified TPCx-BB v1.7.0 kit zip as published by the TPC.
PREPROVISIONED_DATA: dict[str, str] = {
    KIT_FILE: (
        'cdccbfd3af98d0ffb5d004220d2aab6c7f6aca76f2430e9b5b27552a0c703da0'
    ),
}

TPCX_BB_DIR = posixpath.join(linux_packages.INSTALL_DIR, 'tpcx_bb')
# Spark build used by the kit. Kept apart from the spark package's directory,
# which holds a Hadoop-free build without Hive support.
SPARK_DIR = posixpath.join(TPCX_BB_DIR, 'spark')
SPARK_BIN = posixpath.join(SPARK_DIR, 'bin')
SPARK_CONF_DIR = posixpath.join(SPARK_DIR, 'conf')
SPARK_SQL = posixpath.join(SPARK_BIN, 'spark-sql')
# Sourced before running anything from the kit; puts hadoop, hdfs, yarn and
# spark-sql on the PATH, which the kit's scripts expect.
ENV_FILE = posixpath.join(TPCX_BB_DIR, 'env.sh')
HDFS_SPARK_ARCHIVE = '/spark/lib/spark-jars.zip'
HDFS_WAREHOUSE_DIR = '/user/hive/warehouse'

SPARK_DOWNLOAD_URL = (
    'https://archive.apache.org/dist/spark/spark-{version}/'
    'spark-{version}-bin-{hadoop}.tgz'
)

# Full Hive 2.3 metastore schema for PostgreSQL. Spark 3.x's built-in Hive
# client is 2.3.x and accepts this schema.
HIVE_SCHEMA_URL_BASE = (
    'https://raw.githubusercontent.com/apache/hive/rel/release-2.3.9/'
    'metastore/scripts/upgrade/postgres'
)
# The main schema file includes the transaction schema with \i, so both are
# downloaded into the same directory. Values are the files' sha256.
HIVE_SCHEMA_MAIN_FILE = 'hive-schema-2.3.0.postgres.sql'
HIVE_SCHEMA_FILES = {
    HIVE_SCHEMA_MAIN_FILE: (
        '4ec98b7e2fcd2d6d4e5c141b025d82c3eb7b9083fd0ed4d743b4d997e743c0aa'
    ),
    'hive-txn-schema-2.3.0.postgres.sql': (
        '162a0c852427f0bf0c364ba43e201e6c5261faf3ab31b5cf966caf91badbfa1f'
    ),
}
METASTORE_DB = 'metastore'
METASTORE_USER = 'hive'
# Only reachable from the leader itself (localhost).
METASTORE_PASSWORD = 'hive'

# The kit's spark_sql engine for the only publishable stack (cdp/7.1).
KIT_ENGINE = 'spark_sql'
KIT_ENGINE_DIR = 'distributions/cdp/7.1/spark_sql'
# The kit's spark_sql scripts only reference the Spark 2.4 (Scala 2.11) ML
# jar, which cannot be loaded by Spark 3. The kit also ships a Spark 3.1 build
# of the same classes, which is swapped in. This is a kit modification, so
# results are not compliant.
KIT_ML_JAR = 'bigbench-ml-spark-2.4.jar'
SPARK3_ML_JAR = 'bigbench-ml-spark-3.1.jar'

# Phases of one benchmark run, as in the kit's bin/TPCxBB_Benchmarkrun.sh.
BENCHMARK_PHASES = (
    'CLEAN_DATA',
    'DATA_GENERATION',
    'BENCHMARK_START',
    'LOAD_TEST',
    'POWER_TEST',
    'THROUGHPUT_TEST_1',
    'BENCHMARK_STOP',
    'VALIDATE_POWER_TEST',
    'VALIDATE_THROUGHPUT_TEST_1',
)
# Number of queries in TPCx-BB; the M in the BBQpm formula.
NUM_QUERIES = 30
# Scale factors (GB of raw data) allowed by the TPCx-BB specification for a
# Benchmark Run. SF 1 is only used for engine validation.
VALID_SCALE_FACTORS = (1000, 3000, 10000, 30000, 100000, 300000, 1000000)

# Phase names in the kit's times.csv.
POWER_TEST = 'power_test'
THROUGHPUT_TEST = 'throughput_test_1'

# Spark executor sizing: 4 cores per executor, as on Dataproc.
SPARK_EXECUTOR_CORES = 4
# Share of an executor's YARN container given to the JVM heap; the rest is
# memory overhead (Python TRANSFORM scripts, off-heap).
SPARK_EXECUTOR_HEAP_FRACTION = 0.85
# Memory per YARN node reserved for application masters (as hadoop.py does).
YARN_AM_RESERVED_MB = 1024
SPARK_MAX_DRIVER_MEMORY_MB = 16384
SPARK_MIN_DRIVER_MEMORY_MB = 1024
# Share of the leader's memory available to the concurrent spark-sql drivers.
LEADER_DRIVER_MEMORY_FRACTION = 0.6

YARN_NODE_WAIT_SECONDS = 300
_YARN_NODE_POLL_SECONDS = 10


def AptInstall(vm) -> None:
  """Installs the TPCx-BB kit and the tools its scripts use on the VM."""
  # pssh provides parallel-ssh, used by the kit to collect worker information.
  vm.InstallPackages('zip unzip pssh python3')
  vm.RemoteCommand(f'mkdir -p {TPCX_BB_DIR}')
  vm.InstallPreprovisionedPackageData(PACKAGE_NAME, [KIT_FILE], TPCX_BB_DIR)
  kit_path = posixpath.join(TPCX_BB_DIR, KIT_FILE)
  vm.RemoteCommand(f'unzip -o -q {kit_path} -d {TPCX_BB_DIR}')
  vm.RemoteCommand(
      f'find {TPCX_BB_DIR} -path "*/bin/*" -type f -exec chmod +x {{}} +'
  )


def YumInstall(vm) -> None:
  del vm  # unused
  raise errors.Setup.InvalidSetupError(
      'The TPCx-BB package only supports Debian based images (the metastore '
      'is installed with apt).'
  )


def Uninstall(vm) -> None:
  vm.RemoteCommand(f'rm -rf {TPCX_BB_DIR}', ignore_failure=True)


def GetKitDir(vm) -> str:
  """Returns the kit's root directory (BIG_BENCH_HOME) on the VM.

  Args:
    vm: VM where the tpcx_bb package has been installed.

  Raises:
    errors.Setup.InvalidSetupError: if the kit's bin/bigBench is not found.
  """
  stdout, _ = vm.RemoteCommand(
      f'find {TPCX_BB_DIR} -maxdepth 3 -path "*/bin/bigBench" | sort | '
      'head -n 1'
  )
  driver = stdout.strip()
  if not driver:
    raise errors.Setup.InvalidSetupError(
        f'Could not find bin/bigBench under {TPCX_BB_DIR}. Check that '
        f'{KIT_FILE} is a TPCx-BB kit.'
    )
  return posixpath.dirname(posixpath.dirname(driver))


def _WriteRemoteFile(vm, path: str, content: str) -> None:
  """Writes content to path on the VM (overwriting it)."""
  vm.RemoteCommand(
      f"cat > {path} <<'PKB_TPCX_BB_EOF'\n{content}PKB_TPCX_BB_EOF"
  )


def _XmlProperties(properties: dict[str, str]) -> str:
  return ''.join(
      f'  <property><name>{k}</name><value>{v}</value></property>\n'
      for k, v in properties.items()
  )


# Replaces (or adds) properties of a Hadoop XML configuration file in place.
# Arguments: file path, JSON object of properties.
_SET_XML_PROPERTIES_SCRIPT = """
import json, sys
import xml.etree.ElementTree as ET
path, props = sys.argv[1], json.loads(sys.argv[2])
tree = ET.parse(path)
root = tree.getroot()
for prop in root.findall('property'):
  name = prop.find('name')
  if name is not None and (name.text or '').strip() in props:
    root.remove(prop)
for key, value in props.items():
  prop = ET.SubElement(root, 'property')
  ET.SubElement(prop, 'name').text = key
  ET.SubElement(prop, 'value').text = value
tree.write(path)
"""


def SetXmlProperties(vm, path: str, properties: dict[str, str]) -> None:
  """Sets properties in a Hadoop XML configuration file on the VM.

  Existing definitions of the properties are removed first, so the result
  does not depend on Hadoop's handling of duplicate properties.

  Args:
    vm: VM holding the file. Needs python3.
    path: Path of the XML configuration file.
    properties: Properties to add or override.
  """
  vm.RemoteCommand(
      f'python3 -c {shlex.quote(_SET_XML_PROPERTIES_SCRIPT)} {path} '
      f'{shlex.quote(json.dumps(properties))}'
  )


def SparkDownloadUrl(version: str) -> str:
  # Spark 3.3+ renamed the Hadoop 3 build from bin-hadoop3.2 to bin-hadoop3.
  major, minor = (int(x) for x in version.split('.')[:2])
  hadoop_build = 'hadoop3' if (major, minor) >= (3, 3) else 'hadoop3.2'
  return SPARK_DOWNLOAD_URL.format(version=version, hadoop=hadoop_build)


def YarnShuffleJar() -> str:
  return posixpath.join(
      SPARK_DIR, 'yarn', f'spark-{SPARK_VERSION.value}-yarn-shuffle.jar'
  )


@vm_util.Retry(poll_interval=30, max_retries=5)
def InstallSpark(vm) -> None:
  """Installs Spark with Hive support for the kit and its YARN shuffle service.

  Must run on every node after Hadoop is installed: the leader runs the
  spark-sql drivers, and every YARN NodeManager needs the shuffle service jar
  (copied into the NodeManager's classpath; picked up by ConfigureYarn's
  restart). Retried because the Apache archive throttles downloads.

  Args:
    vm: VM to install Spark on.
  """
  vm.Install('curl')
  # python3 runs the kit's TRANSFORM scripts on executors and SetXmlProperties.
  vm.InstallPackages('python3')
  url = SparkDownloadUrl(SPARK_VERSION.value)
  vm.RemoteCommand(
      f'mkdir -p {SPARK_DIR} && curl -sSfL {url} | '
      f'tar -C {SPARK_DIR} --strip-components=1 -xzf -'
  )
  vm.RemoteCommand(
      f'cp -f {YarnShuffleJar()} '
      f'{posixpath.join(hadoop.HADOOP_DIR, "share", "hadoop", "yarn", "lib")}/'
  )


CAPACITY_SCHEDULER = (
    'org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.'
    'CapacityScheduler'
)
YARN_SITE_PROPERTIES = {
    'yarn.resourcemanager.scheduler.class': CAPACITY_SCHEDULER,
    'yarn.nodemanager.aux-services': 'mapreduce_shuffle,spark_shuffle',
    'yarn.nodemanager.aux-services.spark_shuffle.class': (
        'org.apache.spark.network.yarn.YarnShuffleService'
    ),
    # Python TRANSFORM scripts make executors' virtual memory large.
    'yarn.nodemanager.vmem-check-enabled': 'false',
}
CAPACITY_SCHEDULER_PROPERTIES = {
    'yarn.scheduler.capacity.root.default.ordering-policy': 'fair',
    # Room for one application master per concurrent stream.
    'yarn.scheduler.capacity.maximum-am-resource-percent': '0.25',
}


def ConfigureYarn(leader, workers) -> None:
  """Reconfigures and restarts YARN so concurrent Spark queries share it.

  hadoop.py configures a FIFO scheduler and only the MapReduce shuffle
  service, under which each query of a throughput test stream would wait for
  the previous application. This switches to the capacity scheduler with fair
  ordering inside the default queue and adds the Spark external shuffle
  service needed for Spark dynamic allocation, then restarts YARN with
  hadoop.py's scripts and SSH options and checks the effective scheduler.

  Args:
    leader: The YARN ResourceManager VM.
    workers: The YARN NodeManager VMs. InstallSpark must have run on them.

  Raises:
    errors.Setup.InvalidSetupError: if not all NodeManagers rejoin or the
      capacity scheduler is not active.
  """
  yarn_site = posixpath.join(hadoop.HADOOP_CONF_DIR, 'yarn-site.xml')
  yarn_properties = dict(
      YARN_SITE_PROPERTIES,
      **{
          # The Hadoop default (4) would reject executors with more cores.
          'yarn.scheduler.maximum-allocation-vcores': str(
              workers[0].NumCpusForBenchmark()
          ),
      },
  )
  background_tasks.RunThreaded(
      lambda vm: SetXmlProperties(vm, yarn_site, yarn_properties),
      [leader] + list(workers),
  )
  SetXmlProperties(
      leader,
      posixpath.join(hadoop.HADOOP_CONF_DIR, 'capacity-scheduler.xml'),
      CAPACITY_SCHEDULER_PROPERTIES,
  )
  leader.RemoteCommand(
      f'{hadoop.HADOOP_SBIN}/stop-yarn.sh; {hadoop.HADOOP_SBIN}/start-yarn.sh'
  )
  WaitForYarnNodes(leader, len(workers))
  stdout, _ = leader.RemoteCommand(
      f'curl -sSf http://{leader.internal_ip}:8088/ws/v1/cluster/scheduler'
  )
  if 'capacityScheduler' not in stdout:
    raise errors.Setup.InvalidSetupError(
        f'YARN is not running the capacity scheduler: {stdout[:500]!r}'
    )


def KillYarnApplications(leader) -> None:
  """Kills all unfinished YARN applications, e.g. after a driver timeout."""
  leader.RemoteCommand(
      f'{hadoop.YARN_CMD} application -list -appStates'
      " SUBMITTED,ACCEPTED,RUNNING 2>/dev/null | awk '/^application_/{print"
      f" $1}}' | xargs -r -n 1 {hadoop.YARN_CMD} application -kill",
      ignore_failure=True,
  )


def WaitForYarnNodes(leader, expected: int) -> None:
  """Waits until `expected` YARN NodeManagers are RUNNING.

  Args:
    leader: The YARN ResourceManager VM.
    expected: Number of NodeManagers.

  Raises:
    errors.Setup.InvalidSetupError: if they are not running in time.
  """
  try:
    _CheckYarnNodes(leader, expected)
  except vm_util.RetryError as e:
    raise errors.Setup.InvalidSetupError(str(e.__cause__)) from e


class _YarnNodesNotRunningError(Exception):
  """Fewer YARN NodeManagers than expected are RUNNING."""


@vm_util.Retry(
    poll_interval=_YARN_NODE_POLL_SECONDS,
    max_retries=-1,
    timeout=YARN_NODE_WAIT_SECONDS,
    retryable_exceptions=(_YarnNodesNotRunningError,),
)
def _CheckYarnNodes(leader, expected: int) -> None:
  """Raises _YarnNodesNotRunningError unless `expected` NodeManagers run."""
  stdout, _ = leader.RemoteCommand(
      f'{hadoop.YARN_CMD} node -list -states RUNNING', ignore_failure=True
  )
  running = len(re.findall(r'\bRUNNING\b', stdout))
  if running < expected:
    raise _YarnNodesNotRunningError(
        f'Only {running} of {expected} YARN NodeManagers are running after '
        'the restart.'
    )
  logging.info('YARN running on %d nodes.', running)


def InstallMetastore(leader) -> None:
  """Installs a PostgreSQL backed Hive metastore schema on the leader.

  Spark's built-in Hive client connects to it directly over JDBC (no Hive
  metastore service), which supports the concurrent sessions of the
  throughput test.

  Args:
    leader: VM that runs the spark-sql drivers.
  """
  leader.InstallPackages('postgresql libpostgresql-jdbc-java')
  # Idempotent, so that Prepare can be rerun on a kept cluster.
  psql = 'sudo -u postgres psql'
  find_user = f"SELECT 1 FROM pg_roles WHERE rolname='{METASTORE_USER}'"
  create_user = (
      f"CREATE USER {METASTORE_USER} WITH PASSWORD '{METASTORE_PASSWORD}';"
  )
  leader.RemoteCommand(
      f'{psql} -c {shlex.quote(f"DROP DATABASE IF EXISTS {METASTORE_DB};")}'
  )
  leader.RemoteCommand(
      f'{psql} -tAc {shlex.quote(find_user)} | grep -q 1 || '
      f'{psql} -c {shlex.quote(create_user)}'
  )
  create_db = f'CREATE DATABASE {METASTORE_DB} OWNER {METASTORE_USER};'
  leader.RemoteCommand(f'{psql} -c {shlex.quote(create_db)}')
  schema_dir = posixpath.join(TPCX_BB_DIR, 'hive-schema')
  leader.RemoteCommand(f'mkdir -p {schema_dir}')
  _DownloadHiveSchema(leader, schema_dir)
  # Run from schema_dir: the schema includes the transaction schema with \i.
  leader.RemoteCommand(
      f'cd {schema_dir} && PGPASSWORD={METASTORE_PASSWORD} psql -v '
      f'ON_ERROR_STOP=1 -q -h localhost -U {METASTORE_USER} -d {METASTORE_DB} '
      f'-f {HIVE_SCHEMA_MAIN_FILE}'
  )
  leader.RemoteCommand(
      f'cp -f /usr/share/java/postgresql.jar {SPARK_DIR}/jars/'
  )


@vm_util.Retry(max_retries=5, poll_interval=10)
def _DownloadHiveSchema(leader, schema_dir: str) -> None:
  """Downloads the Hive schema files and verifies their sha256."""
  for file_name, sha256 in HIVE_SCHEMA_FILES.items():
    leader.RemoteCommand(
        f'cd {schema_dir} && curl -sSfL -o {file_name} '
        f'{HIVE_SCHEMA_URL_BASE}/{file_name} && '
        f'echo "{sha256}  {file_name}" | sha256sum -c -'
    )


def GetHiveSite(leader_ip: str) -> str:
  """Returns the hive-site.xml used by Spark's built-in Hive client."""
  return (
      '<configuration>\n'
      + _XmlProperties({
          'javax.jdo.option.ConnectionURL': (
              f'jdbc:postgresql://localhost:5432/{METASTORE_DB}'
          ),
          'javax.jdo.option.ConnectionDriverName': 'org.postgresql.Driver',
          'javax.jdo.option.ConnectionUserName': METASTORE_USER,
          'javax.jdo.option.ConnectionPassword': METASTORE_PASSWORD,
          # The schema is created up front by InstallMetastore.
          'datanucleus.schema.autoCreateAll': 'false',
          'hive.metastore.schema.verification': 'false',
          'hive.metastore.warehouse.dir': (
              f'hdfs://{leader_ip}{HDFS_WAREHOUSE_DIR}'
          ),
      })
      + '</configuration>\n'
  )


def GetSparkProperties(
    leader_ip: str,
    worker_vcpus: int,
    worker_yarn_memory_mb: int,
    num_workers: int,
    leader_memory_mb: int,
    num_streams: int,
    overrides: dict[str, str] | None = None,
) -> dict[str, str]:
  """Returns spark-defaults.conf properties for the kit's spark-sql runs.

  Executors have SPARK_EXECUTOR_CORES cores and split a NodeManager's memory
  (minus an application master reserve) evenly. Spark dynamic allocation lets
  the concurrent queries of the throughput test share the cluster instead of
  each claiming all executors.

  Args:
    leader_ip: Internal IP of the HDFS NameNode.
    worker_vcpus: vCPUs per worker.
    worker_yarn_memory_mb: Memory YARN schedules per worker.
    num_workers: Number of workers.
    leader_memory_mb: Total memory of the leader, which runs the drivers.
    num_streams: Number of concurrent throughput test streams.
    overrides: Properties overriding the computed ones.
  """
  executor_cores = max(1, min(SPARK_EXECUTOR_CORES, worker_vcpus))
  executors_per_node = max(1, worker_vcpus // executor_cores)
  container_mb = (
      worker_yarn_memory_mb - YARN_AM_RESERVED_MB
  ) // executors_per_node
  executor_memory_mb = int(container_mb * SPARK_EXECUTOR_HEAP_FRACTION)
  overhead_mb = max(384, container_mb - executor_memory_mb)
  driver_memory_mb = int(
      leader_memory_mb * LEADER_DRIVER_MEMORY_FRACTION / max(1, num_streams)
  )
  driver_memory_mb = max(
      SPARK_MIN_DRIVER_MEMORY_MB,
      min(SPARK_MAX_DRIVER_MEMORY_MB, driver_memory_mb),
  )
  properties = {
      'spark.master': 'yarn',
      'spark.submit.deployMode': 'client',
      'spark.yarn.archive': f'hdfs://{leader_ip}{HDFS_SPARK_ARCHIVE}',
      'spark.sql.catalogImplementation': 'hive',
      'spark.sql.warehouse.dir': f'hdfs://{leader_ip}{HDFS_WAREHOUSE_DIR}',
      'spark.driver.memory': f'{driver_memory_mb}m',
      'spark.driver.maxResultSize': '4g',
      'spark.executor.cores': str(executor_cores),
      'spark.executor.memory': f'{executor_memory_mb}m',
      'spark.executor.memoryOverhead': f'{overhead_mb}m',
      'spark.dynamicAllocation.enabled': 'true',
      'spark.shuffle.service.enabled': 'true',
      'spark.dynamicAllocation.minExecutors': '1',
      'spark.dynamicAllocation.executorIdleTimeout': '60s',
      'spark.sql.shuffle.partitions': str(worker_vcpus * num_workers * 3),
      'spark.sql.adaptive.enabled': 'true',
      'spark.sql.adaptive.coalescePartitions.enabled': 'true',
      # Spark 3 defaults to ANSI store assignment, which rejects the kit's
      # implicit string to bigint INSERTs; LEGACY is the Spark 2.4 / Hive
      # behavior the kit was written for.
      'spark.sql.storeAssignmentPolicy': 'LEGACY',
  }
  properties.update(overrides or {})
  return properties


def ConfigureSpark(leader, spark_properties: dict[str, str]) -> None:
  """Writes the Spark and metastore configuration and the env file.

  Also stages the Spark jars on HDFS (spark.yarn.archive) and creates the HDFS
  directories the kit and the metastore use. HDFS and YARN must be running.

  Args:
    leader: VM that runs the spark-sql drivers.
    spark_properties: spark-defaults.conf properties (GetSparkProperties).
  """
  _WriteRemoteFile(
      leader,
      ENV_FILE,
      'export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:/bin/java::")\n'
      f'export HADOOP_HOME={hadoop.HADOOP_DIR}\n'
      f'export HADOOP_CONF_DIR={hadoop.HADOOP_CONF_DIR}\n'
      f'export SPARK_HOME={SPARK_DIR}\n'
      f'export SPARK_CONF_DIR={SPARK_CONF_DIR}\n'
      f'export PATH={hadoop.HADOOP_BIN}:{SPARK_BIN}:$PATH\n',
  )
  _WriteRemoteFile(
      leader,
      posixpath.join(SPARK_CONF_DIR, 'spark-defaults.conf'),
      ''.join(f'{k} {v}\n' for k, v in spark_properties.items()),
  )
  _WriteRemoteFile(
      leader,
      posixpath.join(SPARK_CONF_DIR, 'spark-env.sh'),
      f'export HADOOP_CONF_DIR={hadoop.HADOOP_CONF_DIR}\n',
  )
  _WriteRemoteFile(
      leader,
      posixpath.join(SPARK_CONF_DIR, 'hive-site.xml'),
      GetHiveSite(leader.internal_ip),
  )
  hdfs = hadoop.HDFS_CMD
  archive_dir = posixpath.dirname(HDFS_SPARK_ARCHIVE)
  leader.RemoteCommand(
      f'{hdfs} dfs -mkdir -p /user/$(whoami) {HDFS_WAREHOUSE_DIR} '
      f'{archive_dir} /tmp && {hdfs} dfs -chown {leader.user_name} /tmp && '
      f'{hdfs} dfs -chmod 755 /tmp'
  )
  leader.InstallPackages('zip')
  leader.RemoteCommand(
      f'cd {SPARK_DIR}/jars && rm -f /tmp/spark-jars.zip && '
      'zip -q -r /tmp/spark-jars.zip . && '
      f'{hdfs} dfs -put -f /tmp/spark-jars.zip {HDFS_SPARK_ARCHIVE}'
  )


_SMOKE_TEST_STDERR = posixpath.join(TPCX_BB_DIR, 'smoke_test.err')


def SmokeTestSpark(leader) -> None:
  """Runs a small spark-sql job on YARN against the metastore.

  Args:
    leader: VM that runs the spark-sql drivers.

  Raises:
    errors.Setup.InvalidSetupError: if the query does not return its row.
  """
  script = (
      f'source {ENV_FILE} && {SPARK_SQL} -S -e "CREATE DATABASE IF NOT EXISTS '
      'pkb_smoke; CREATE TABLE IF NOT EXISTS pkb_smoke.t(a INT) STORED AS ORC; '
      'INSERT INTO pkb_smoke.t VALUES (42); SELECT a FROM pkb_smoke.t; '
      f'DROP DATABASE pkb_smoke CASCADE;" 2>{_SMOKE_TEST_STDERR}'
  )
  stdout, _ = leader.RemoteCommand(
      f'bash -c {shlex.quote(script)}', ignore_failure=True
  )
  if '42' not in stdout.split():
    stderr, _ = leader.RemoteCommand(
        f'tail -n 30 {_SMOKE_TEST_STDERR}', ignore_failure=True
    )
    raise errors.Setup.InvalidSetupError(
        f'spark-sql smoke test on YARN failed; output: {stdout!r}; last '
        f'stderr lines:\n{stderr}'
    )


def ConfigureKit(leader, kit_dir: str) -> None:
  """Points the kit at the PKB cluster and adapts it to Spark 3.

  Args:
    leader: VM where the kit is installed.
    kit_dir: GetKitDir(leader).

  Raises:
    errors.Setup.InvalidSetupError: if the ML jar could not be swapped.
  """
  user_settings = posixpath.join(kit_dir, 'conf', 'userSettings.conf')
  leader.RemoteCommand(
      f"cat >> {user_settings} <<'PKB_TPCX_BB_EOF'\n"
      '## PerfKitBenchmarker overrides\n'
      f'export BIG_BENCH_HADOOP_CONF="{hadoop.HADOOP_CONF_DIR}"\n'
      f'export BIG_BENCH_HADOOP_LIBS_NATIVE="{hadoop.HADOOP_DIR}/lib/native"\n'
      f'export BIG_BENCH_DEFAULT_ENGINE="{KIT_ENGINE}"\n'
      'export BIG_BENCH_PSSH_BINARY="parallel-ssh"\n'
      'export BIG_BENCH_ENV_INFO_DIRECTORIES='
      f'"{hadoop.HADOOP_CONF_DIR} {SPARK_CONF_DIR}"\n'
      'PKB_TPCX_BB_EOF'
  )
  engine_dir = posixpath.join(kit_dir, KIT_ENGINE_DIR)
  old_jar = KIT_ML_JAR.replace('.', r'\.')
  leader.RemoteCommand(
      f"grep -rl '{KIT_ML_JAR}' {engine_dir} | "
      f"xargs -r sed -i 's/{old_jar}/{SPARK3_ML_JAR}/g'"
  )
  stdout, _ = leader.RemoteCommand(
      f"grep -rl '{KIT_ML_JAR}' {engine_dir} | wc -l"
  )
  if stdout.strip() != '0':
    raise errors.Setup.InvalidSetupError(
        f'{KIT_ML_JAR} is still referenced in {engine_dir}.'
    )


def GetRunCommand(
    kit_dir: str,
    scale_factor: int,
    num_streams: int,
    map_tasks: int,
    driver_log: str,
    timeout_seconds: int,
) -> str:
  """Returns the shell command running one benchmark run with the kit driver.

  The PDGF data generator asks once for an uppercase YES to accept its
  license. The driver's exit code is that of its final log zipping step, not
  the benchmark verdict, so callers must parse the result from driver_log.
  The command prints DRIVER_RC_MARKER followed by the driver's exit code (124
  or 137 if it was killed by the timeout). The kit's logs/ directory is
  emptied first because the kit appends to logs/times.csv across runs.

  Args:
    kit_dir: GetKitDir(leader).
    scale_factor: TPCx-BB scale factor.
    num_streams: Number of throughput test streams.
    map_tasks: Number of PDGF data generation map tasks.
    driver_log: File receiving the driver's stdout and stderr.
    timeout_seconds: Kill the driver after this many seconds.
  """
  script = (
      f'source {ENV_FILE} && cd {kit_dir} && rm -rf logs && mkdir -p logs && '
      f'{{ yes YES | timeout --kill-after=60 {timeout_seconds} bin/bigBench '
      f'runBenchmark -f {scale_factor} -s {num_streams} -m {map_tasks} -i '
      f'{",".join(BENCHMARK_PHASES)} > {driver_log} 2>&1; '
      # Not $?: that is the exit status of `yes` (SIGPIPE).
      f'echo "{DRIVER_RC_MARKER}${{PIPESTATUS[1]}}"; }}'
  )
  return f'bash -c {shlex.quote(script)}'


DRIVER_RC_MARKER = 'PKB_TPCX_BB_DRIVER_RC='
# Exit codes of `timeout` when it had to stop the driver.
TIMEOUT_EXIT_CODES = (124, 137)


def ParseDriverExitCode(stdout: str) -> int | None:
  """Returns the driver exit code printed by GetRunCommand's command."""
  match = re.search(rf'{DRIVER_RC_MARKER}(\d+)', stdout)
  return int(match.group(1)) if match else None


@dataclasses.dataclass
class BenchmarkResult:
  """Result block printed by the kit driver at the end of a run.

  Attributes:
    t_load: Load test time in seconds.
    t_ld: Load test contribution, 0.1 * t_load.
    t_pt: Power test metric (30 * geometric mean of the query times) in s.
    t_t_put: Elapsed time of the throughput test in seconds.
    t_tt: Throughput test metric, t_t_put / streams, in seconds.
    bbqpm: BBQpm@SF reported by the kit.
    scale_factor: SF of the BBQpm@SF line.
    valid: Whether the kit declared the result VALID.
    problems: The kit's NOK: lines.
  """

  t_load: float
  t_ld: float
  t_pt: float
  t_t_put: float
  t_tt: float
  bbqpm: float
  scale_factor: int
  valid: bool
  problems: list[str]


_NUMBER = r'([0-9]+(?:\.[0-9]+)?(?:[eE][-+]?[0-9]+)?)'
_RESULT_PATTERNS = {
    't_load': re.compile(rf'\bT_LOAD = {_NUMBER}'),
    't_ld': re.compile(rf'\bT_LD = [^:\n]*: {_NUMBER}'),
    't_pt': re.compile(rf'\bT_PT = {_NUMBER}'),
    't_t_put': re.compile(rf'\bT_T_PUT = {_NUMBER}'),
    't_tt': re.compile(rf'\bT_TT = {_NUMBER}'),
}
# \b keeps VALID from matching inside INVALID.
_VERDICT_PATTERN = re.compile(rf'\b(INVALID|VALID) BBQpm@([0-9]+) = {_NUMBER}')
_PROBLEM_PATTERN = re.compile(r'\bNOK: ([^\r\n]*)')


def ParseResult(log: str) -> BenchmarkResult | None:
  """Parses the result block of the kit driver's log.

  Args:
    log: Driver output (or the kit's BigBenchResult.log).

  Returns:
    The result, or None if the log has no complete result block (e.g. the run
    failed or required phases were skipped).
  """
  values = {}
  for key, pattern in _RESULT_PATTERNS.items():
    matches = pattern.findall(log)
    if not matches:
      return None
    values[key] = float(matches[-1])
  verdicts = _VERDICT_PATTERN.findall(log)
  if not verdicts:
    return None
  verdict, scale_factor, bbqpm = verdicts[-1]
  return BenchmarkResult(
      bbqpm=float(bbqpm),
      scale_factor=int(scale_factor),
      valid=verdict == 'VALID',
      problems=[p.strip() for p in _PROBLEM_PATTERN.findall(log)],
      **values,
  )


def ParseProblems(log: str) -> list[str]:
  """Returns the kit's NOK: lines, which explain a missing result."""
  return [p.strip() for p in _PROBLEM_PATTERN.findall(log)]


def ComputeBbqpm(
    scale_factor: float,
    t_ld: float,
    t_pt: float,
    t_tt: float,
    num_queries: int = NUM_QUERIES,
) -> float:
  """Computes BBQpm@SF = SF * 60 * M / (T_LD + sqrt(T_PT * T_TT))."""
  denominator = t_ld + math.sqrt(max(0.0, t_pt * t_tt))
  if denominator <= 0:
    logging.warning('Cannot compute BBQpm from non-positive times.')
    return 0.0
  return scale_factor * 60.0 * num_queries / denominator


_PHASE_TIME_PATTERN = re.compile(
    r'BigBench ([a-z][a-z0-9 ]*): (.+?)\s+finished\. Time:\s+'
    r'(\d+)h:(\d+)m:(\d+)s:(\d+)ms'
)


def ParsePhaseTimes(log: str) -> dict[str, float]:
  """Parses the driver's per phase elapsed times.

  Args:
    log: Driver output.

  Returns:
    Seconds by phase name (e.g. 'preparation: Data generation', 'phase 2:
    Power test'), in log order. Per query lines are skipped.
  """
  times = collections.OrderedDict()
  for match in _PHASE_TIME_PATTERN.finditer(log):
    category, name, hours, minutes, seconds, millis = match.groups()
    if 'stream' in name:
      continue
    times[f'{category}: {name.strip()}'] = (
        int(hours) * 3600
        + int(minutes) * 60
        + int(seconds)
        + int(millis) / 1000.0
    )
  return times


@dataclasses.dataclass
class QueryTime:
  """One query execution from the kit's times.csv."""

  query: str
  phase: str
  stream: int
  seconds: float
  successful: bool


def ParseTimesCsv(
    content: str,
    scale_factor: int,
    phases: tuple[str, ...] = (POWER_TEST, THROUGHPUT_TEST),
) -> list[QueryTime]:
  """Parses the per query times of the kit's pipe separated times.csv.

  The kit appends to times.csv across runs and writes some rows (e.g. data
  generation) with missing columns; rows that do not match the header, are of
  another scale factor or phase, or cannot be parsed are skipped.

  Args:
    content: times.csv content.
    scale_factor: Only rows of this SF are returned.
    phases: Only rows of these BENCHMARK_PHASE values are returned.

  Returns:
    Query times in file order.
  """
  rows = list(csv.reader(content.splitlines(), delimiter='|'))
  if not rows:
    return []
  header = [h.strip() for h in rows[0]]
  results = []
  for row in rows[1:]:
    if len(row) != len(header):
      continue
    record = dict(zip(header, (v.strip() for v in row)))
    try:
      if (
          record['BENCHMARK_PHASE'] not in phases
          or int(record['SCALE_FACTOR']) != scale_factor
      ):
        continue
      results.append(
          QueryTime(
              query=record['QUERY'],
              phase=record['BENCHMARK_PHASE'],
              stream=int(record['STREAM']),
              seconds=int(record['DURATION_MS']) / 1000.0,
              successful=record['SUCCESSFUL'] == 'SUCCESS',
          )
      )
    except (KeyError, ValueError):
      continue
  return results

"""Tests for the tpcx_bb linux package."""

import sys
import unittest
from unittest import mock

from absl import flags
from absl.testing import flagsaver
from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_packages import tpcx_bb
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

_KIT_DIR = '/opt/pkb/tpcx_bb/TPCX-BB_Tools_1.7.0'

# Excerpt in the format of the kit driver's output (stdout + java.util.logging
# on stderr, lines end with \r\r).
_DRIVER_LOG = (
    'BigBench clean: Data                                    finished. Time:'
    '          0h:00m:03s:856ms\r\r\n'
    'BigBench preparation: Data generation                   finished. Time:'
    '          0h:12m:28s:466ms\r\r\n'
    'BigBench phase 1: Load test                             finished. Time:'
    '          0h:02m:58s:994ms\r\r\n'
    'BigBench phase 2: Power test                            stream          '
    '0, query  1 ( 1/30) finished. Time:          0h:00m:45s:929ms\r\r\n'
    'BigBench phase 2: Power test                            finished. Time:'
    '          0h:34m:07s:479ms\r\r\n'
    'BigBench phase 3: First throughput test run             finished. Time:'
    '          0h:39m:47s:966ms\r\r\n'
    'T_LOAD = 178.994\r\r\n'
    'T_LD = 0.1 * T_LOAD: 17.8994\r\r\n'
    'T_PT = 1874.3939949734754\r\r\n'
    'T_T_PUT = 2387.966\r\r\n'
    'T_TT = 1193.983\r\r\n'
    '=== Checking validity of the final result ===\r\r\n'
    'OK: All required BigBench phases were performed.\r\r\n'
    '=== Final result ===\r\r\n'
    'VALID BBQpm@100 = 118.89881969280403\r\r\n'
    'Exception in thread "main" java.lang.NoClassDefFoundError: '
    'org/apache/commons/lang3/time/FastDateFormat\n'
)

_TIMES_CSV = (
    'STARTDATE_EPOCH|STOPDATE_EPOCH|DURATION_MS|STARTDATE|STOPDATE|DURATION|'
    'QUERY|SCALE_FACTOR|BENCHMARK_PHASE|STREAM|SUCCESSFUL|EXIT_CODE|HAS_RESULT|'
    'HDFS_SIZE\n'
    '1|2|697000|a|b|c|dataGen_base|100|run_query|0|SUCCESS|0||\n'
    '1|2|35000|a|b|c|dataGen_refresh|run_query|0|SUCCESS|0\n'
    '1|2|179000|a|b|c|Load data|100|Load data|0|SUCCESS|0||\n'
    '1|2|29000|a|b|c|q01|100|power_test|0|SUCCESS|0|HAS_RESULT|1636\n'
    '1|2|41000|a|b|c|q01|1|power_test|0|SUCCESS|0|HAS_RESULT|1636\n'
    '1|2|25000|a|b|c|q17|100|throughput_test_1|1|FAILED|1|HAS_RESULT|32\n'
    '1|2|bad|a|b|c|q18|100|throughput_test_1|1|SUCCESS|0|HAS_RESULT|32\n'
)


def _Commands(vm) -> list[str]:
  return [c.args[0] for c in vm.RemoteCommand.call_args_list]


class TpcxBbKitTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.vm = mock.Mock()
    self.vm.RemoteCommand.return_value = ('', '')

  def testInstallInstallsPreprovisionedKitAndUnzipsIt(self):
    tpcx_bb.AptInstall(self.vm)
    self.vm.InstallPackages.assert_called_once_with('zip unzip pssh python3')
    self.vm.InstallPreprovisionedPackageData.assert_called_once_with(
        'tpcx_bb', ['TPCX-BB-Tool.zip'], tpcx_bb.TPCX_BB_DIR
    )
    self.assertIn(
        f'unzip -o -q {tpcx_bb.TPCX_BB_DIR}/TPCX-BB-Tool.zip -d '
        f'{tpcx_bb.TPCX_BB_DIR}',
        _Commands(self.vm),
    )

  def testPreprovisionedDataHasKitChecksum(self):
    self.assertIn(tpcx_bb.KIT_FILE, tpcx_bb.PREPROVISIONED_DATA)

  def testYumInstallRaises(self):
    with self.assertRaisesRegex(errors.Setup.InvalidSetupError, 'Debian'):
      tpcx_bb.YumInstall(self.vm)

  def testUninstall(self):
    tpcx_bb.Uninstall(self.vm)
    self.vm.RemoteCommand.assert_called_once_with(
        f'rm -rf {tpcx_bb.TPCX_BB_DIR}', ignore_failure=True
    )

  def testGetKitDir(self):
    self.vm.RemoteCommand.return_value = (f'{_KIT_DIR}/bin/bigBench\n', '')
    self.assertEqual(tpcx_bb.GetKitDir(self.vm), _KIT_DIR)

  def testGetKitDirMissingRaises(self):
    with self.assertRaisesRegex(errors.Setup.InvalidSetupError, 'bigBench'):
      tpcx_bb.GetKitDir(self.vm)

  def testConfigureKitOverridesSettingsAndSwapsMlJar(self):
    self.vm.RemoteCommand.side_effect = lambda cmd, **_: (
        ('0\n', '') if 'wc -l' in cmd else ('', '')
    )
    tpcx_bb.ConfigureKit(self.vm, _KIT_DIR)
    commands = '\n'.join(_Commands(self.vm))
    self.assertIn(f'cat >> {_KIT_DIR}/conf/userSettings.conf', commands)
    self.assertIn('BIG_BENCH_DEFAULT_ENGINE="spark_sql"', commands)
    self.assertIn(
        'BIG_BENCH_HADOOP_CONF="/opt/pkb/hadoop/etc/hadoop"', commands
    )
    self.assertIn('BIG_BENCH_PSSH_BINARY="parallel-ssh"', commands)
    self.assertIn(
        r's/bigbench-ml-spark-2\.4\.jar/bigbench-ml-spark-3.1.jar/g', commands
    )
    self.assertIn(f'{_KIT_DIR}/distributions/cdp/7.1/spark_sql', commands)

  def testConfigureKitRaisesIfMlJarStillReferenced(self):
    self.vm.RemoteCommand.side_effect = lambda cmd, **_: (
        ('2\n', '') if 'wc -l' in cmd else ('', '')
    )
    with self.assertRaisesRegex(errors.Setup.InvalidSetupError, 'still'):
      tpcx_bb.ConfigureKit(self.vm, _KIT_DIR)


class TpcxBbStackTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.leader = mock.Mock(internal_ip='10.0.0.2', user_name='perfkit')
    self.leader.RemoteCommand.return_value = ('', '')
    self.workers = [mock.Mock() for _ in range(3)]
    for worker in self.workers:
      worker.RemoteCommand.return_value = ('', '')
      worker.NumCpusForBenchmark.return_value = 16
    self.enter_context(
        mock.patch.object(
            tpcx_bb.background_tasks,
            'RunThreaded',
            side_effect=lambda fn, items: [fn(i) for i in items],
        )
    )
    self.enter_context(mock.patch.object(tpcx_bb.vm_util.time, 'sleep'))

  def testSparkDownloadUrl(self):
    self.assertEqual(
        tpcx_bb.SparkDownloadUrl('3.1.3'),
        'https://archive.apache.org/dist/spark/spark-3.1.3/'
        'spark-3.1.3-bin-hadoop3.2.tgz',
    )
    self.assertEqual(
        tpcx_bb.SparkDownloadUrl('3.5.1'),
        'https://archive.apache.org/dist/spark/spark-3.5.1/'
        'spark-3.5.1-bin-hadoop3.tgz',
    )

  @flagsaver.flagsaver
  def testSparkVersionFlagRejectsSpark2(self):
    with self.assertRaises(flags.IllegalFlagValueError):
      FLAGS.tpcx_bb_spark_version = '2.4.8'

  def testInstallSpark(self):
    vm = self.workers[0]
    tpcx_bb.InstallSpark(vm)
    vm.Install.assert_called_once_with('curl')
    vm.InstallPackages.assert_called_once_with('python3')
    commands = _Commands(vm)
    self.assertIn(
        f'mkdir -p {tpcx_bb.SPARK_DIR} && curl -sSfL '
        'https://archive.apache.org/dist/spark/spark-3.1.3/'
        'spark-3.1.3-bin-hadoop3.2.tgz | '
        f'tar -C {tpcx_bb.SPARK_DIR} --strip-components=1 -xzf -',
        commands,
    )
    self.assertIn(
        f'cp -f {tpcx_bb.SPARK_DIR}/yarn/spark-3.1.3-yarn-shuffle.jar '
        '/opt/pkb/hadoop/share/hadoop/yarn/lib/',
        commands,
    )

  def testSetXmlPropertiesScriptReplacesInPlace(self):
    path = self.create_tempfile(
        content=(
            '<?xml version="1.0"?>\n<configuration>\n'
            '  <property>\n    <name>a</name>\n    <value>old</value>\n'
            '  </property>\n'
            '  <property><name>keep</name><value>1</value></property>\n'
            '</configuration>\n'
        )
    ).full_path
    with mock.patch.object(sys, 'argv', ['-c', path, '{"a": "new", "b": "2"}']):
      exec(tpcx_bb._SET_XML_PROPERTIES_SCRIPT, {})  # pylint: disable=exec-used
    with open(path) as f:
      content = f.read()
    self.assertEqual(content.count('<name>a</name>'), 1)
    self.assertIn('<value>new</value>', content)
    self.assertNotIn('old', content)
    self.assertIn('<name>b</name><value>2</value>', content)
    self.assertIn('<name>keep</name><value>1</value>', content)

  def testSetXmlPropertiesCommand(self):
    tpcx_bb.SetXmlProperties(self.leader, '/conf/yarn-site.xml', {'k': 'v'})
    command = _Commands(self.leader)[0]
    self.assertTrue(command.startswith('python3 -c '))
    self.assertIn('/conf/yarn-site.xml', command)
    self.assertIn('\'{"k": "v"}\'', command)

  def _LeaderResponses(self, scheduler: str, running: int):
    def Respond(cmd, **_):
      if 'node -list' in cmd:
        return ('Total Nodes:3\n' + 'w RUNNING w:8042 0\n' * running, '')
      if 'ws/v1/cluster/scheduler' in cmd:
        info = f'{{"schedulerInfo":{{"type":"{scheduler}"}}}}'
        return (f'{{"scheduler":{info}}}', '')
      return ('', '')

    return Respond

  def testConfigureYarn(self):
    self.leader.RemoteCommand.side_effect = self._LeaderResponses(
        'capacityScheduler', 3
    )
    tpcx_bb.ConfigureYarn(self.leader, self.workers)
    for vm in [self.leader] + self.workers:
      command = _Commands(vm)[0]
      self.assertIn('/opt/pkb/hadoop/etc/hadoop/yarn-site.xml', command)
      self.assertIn('mapreduce_shuffle,spark_shuffle', command)
      self.assertIn('YarnShuffleService', command)
      self.assertIn('capacity.CapacityScheduler', command)
      self.assertIn('"yarn.scheduler.maximum-allocation-vcores": "16"', command)
    leader_commands = '\n'.join(_Commands(self.leader))
    self.assertIn('capacity-scheduler.xml', leader_commands)
    self.assertIn(
        '"yarn.scheduler.capacity.root.default.ordering-policy": "fair"',
        leader_commands,
    )
    self.assertIn(
        '/opt/pkb/hadoop/sbin/stop-yarn.sh; /opt/pkb/hadoop/sbin/start-yarn.sh',
        leader_commands,
    )
    self.assertIn(
        'http://10.0.0.2:8088/ws/v1/cluster/scheduler', leader_commands
    )

  def testConfigureYarnRaisesIfNotCapacityScheduler(self):
    self.leader.RemoteCommand.side_effect = self._LeaderResponses(
        'fifoScheduler', 3
    )
    with self.assertRaisesRegex(errors.Setup.InvalidSetupError, 'capacity'):
      tpcx_bb.ConfigureYarn(self.leader, self.workers)

  def testWaitForYarnNodesTimesOut(self):
    self.leader.RemoteCommand.side_effect = self._LeaderResponses(
        'capacityScheduler', 2
    )
    with mock.patch.object(tpcx_bb.vm_util, 'time') as mock_time:
      mock_time.time.side_effect = [0, 1000]
      with self.assertRaisesRegex(
          errors.Setup.InvalidSetupError, 'Only 2 of 3'
      ):
        tpcx_bb.WaitForYarnNodes(self.leader, 3)

  def testWaitForYarnNodesRetries(self):
    responses = iter([2, 3])

    def Respond(cmd, **kwargs):
      return self._LeaderResponses('capacityScheduler', next(responses))(
          cmd, **kwargs
      )

    self.leader.RemoteCommand.side_effect = Respond
    with mock.patch.object(tpcx_bb.vm_util, 'time') as mock_time:
      mock_time.time.return_value = 0
      tpcx_bb.WaitForYarnNodes(self.leader, 3)
    self.assertEqual(self.leader.RemoteCommand.call_count, 2)
    mock_time.sleep.assert_called_once()

  def testKillYarnApplications(self):
    tpcx_bb.KillYarnApplications(self.leader)
    command = _Commands(self.leader)[0]
    self.assertIn(
        'application -list -appStates SUBMITTED,ACCEPTED,RUNNING', command
    )
    self.assertIn('application -kill', command)

  def testInstallMetastore(self):
    tpcx_bb.InstallMetastore(self.leader)
    self.leader.InstallPackages.assert_called_once_with(
        'postgresql libpostgresql-jdbc-java'
    )
    commands = '\n'.join(_Commands(self.leader))
    self.assertIn("psql -c 'DROP DATABASE IF EXISTS metastore;'", commands)
    self.assertIn(
        "psql -tAc 'SELECT 1 FROM pg_roles WHERE rolname='\"'\"'hive'\"'\"''"
        ' | grep -q 1 || ',
        commands,
    )
    self.assertIn("psql -c 'CREATE DATABASE metastore OWNER hive;'", commands)
    for file_name, sha256 in tpcx_bb.HIVE_SCHEMA_FILES.items():
      self.assertIn(f'{tpcx_bb.HIVE_SCHEMA_URL_BASE}/{file_name}', commands)
      self.assertIn(f'echo "{sha256}  {file_name}" | sha256sum -c -', commands)
    self.assertIn('-f hive-schema-2.3.0.postgres.sql', commands)
    self.assertIn('ON_ERROR_STOP=1', commands)
    self.assertIn(
        f'cp -f /usr/share/java/postgresql.jar {tpcx_bb.SPARK_DIR}/jars/',
        commands,
    )

  def testGetHiveSite(self):
    hive_site = tpcx_bb.GetHiveSite('10.0.0.2')
    self.assertIn(
        '<name>datanucleus.schema.autoCreateAll</name><value>false</value>',
        hive_site,
    )
    self.assertIn('jdbc:postgresql://localhost:5432/metastore', hive_site)
    self.assertIn('hdfs://10.0.0.2/user/hive/warehouse', hive_site)

  def testGetSparkProperties(self):
    properties = tpcx_bb.GetSparkProperties(
        leader_ip='10.0.0.2',
        worker_vcpus=16,
        worker_yarn_memory_mb=100000,
        num_workers=3,
        leader_memory_mb=128000,
        num_streams=2,
    )
    self.assertEqual(properties['spark.master'], 'yarn')
    self.assertEqual(properties['spark.submit.deployMode'], 'client')
    self.assertEqual(properties['spark.executor.cores'], '4')
    # (100000 - 1024) // 4 executors = 24744 MB per container.
    self.assertEqual(properties['spark.executor.memory'], '21032m')
    self.assertEqual(properties['spark.executor.memoryOverhead'], '3712m')
    self.assertEqual(properties['spark.driver.memory'], '16384m')
    self.assertEqual(properties['spark.sql.shuffle.partitions'], '144')
    self.assertEqual(properties['spark.dynamicAllocation.enabled'], 'true')
    self.assertEqual(properties['spark.shuffle.service.enabled'], 'true')
    self.assertEqual(properties['spark.sql.storeAssignmentPolicy'], 'LEGACY')
    self.assertEqual(
        properties['spark.yarn.archive'],
        'hdfs://10.0.0.2/spark/lib/spark-jars.zip',
    )

  def testGetSparkPropertiesSmallNodesAndOverrides(self):
    properties = tpcx_bb.GetSparkProperties(
        leader_ip='10.0.0.2',
        worker_vcpus=2,
        worker_yarn_memory_mb=8000,
        num_workers=3,
        leader_memory_mb=8000,
        num_streams=4,
        overrides={'spark.sql.shuffle.partitions': '10'},
    )
    self.assertEqual(properties['spark.executor.cores'], '2')
    self.assertEqual(properties['spark.driver.memory'], '1200m')
    self.assertEqual(properties['spark.sql.shuffle.partitions'], '10')

  def testConfigureSpark(self):
    tpcx_bb.ConfigureSpark(self.leader, {'spark.master': 'yarn'})
    commands = '\n'.join(_Commands(self.leader))
    self.assertIn(f'cat > {tpcx_bb.ENV_FILE} <<', commands)
    self.assertIn(
        'export PATH=/opt/pkb/hadoop/bin:/opt/pkb/tpcx_bb/spark/bin:$PATH',
        commands,
    )
    self.assertIn(
        f'cat > {tpcx_bb.SPARK_CONF_DIR}/spark-defaults.conf <<', commands
    )
    self.assertIn('spark.master yarn\n', commands)
    self.assertIn(f'cat > {tpcx_bb.SPARK_CONF_DIR}/hive-site.xml', commands)
    self.assertIn('dfs -chown perfkit /tmp', commands)
    self.assertIn('dfs -chmod 755 /tmp', commands)
    self.leader.InstallPackages.assert_called_once_with('zip')
    self.assertIn(
        'dfs -put -f /tmp/spark-jars.zip /spark/lib/spark-jars.zip', commands
    )

  def testSmokeTestSpark(self):
    self.leader.RemoteCommand.return_value = ('42\n', '')
    tpcx_bb.SmokeTestSpark(self.leader)
    command = _Commands(self.leader)[0]
    self.assertTrue(command.startswith('bash -c '))
    self.assertIn(f'source {tpcx_bb.ENV_FILE}', command)

  def testSmokeTestSparkFails(self):
    self.leader.RemoteCommand.side_effect = lambda cmd, **_: (
        ('MetaException: no route', '') if cmd.startswith('tail') else ('', '')
    )
    with self.assertRaisesRegex(
        errors.Setup.InvalidSetupError, 'smoke(.|\n)*MetaException'
    ):
      tpcx_bb.SmokeTestSpark(self.leader)


class TpcxBbRunAndParseTest(pkb_common_test_case.PkbCommonTestCase):

  def testGetRunCommand(self):
    command = tpcx_bb.GetRunCommand(
        kit_dir=_KIT_DIR,
        scale_factor=10,
        num_streams=2,
        map_tasks=48,
        driver_log='/opt/pkb/tpcx_bb/driver_1.log',
        timeout_seconds=3600,
    )
    self.assertTrue(command.startswith('bash -c '))
    self.assertIn(f'cd {_KIT_DIR} && rm -rf logs && mkdir -p logs', command)
    self.assertIn(
        'yes YES | timeout --kill-after=60 3600 bin/bigBench runBenchmark -f '
        '10 -s 2 -m 48 -i CLEAN_DATA,DATA_GENERATION,BENCHMARK_START,'
        'LOAD_TEST,POWER_TEST,THROUGHPUT_TEST_1,BENCHMARK_STOP,'
        'VALIDATE_POWER_TEST,VALIDATE_THROUGHPUT_TEST_1 > '
        '/opt/pkb/tpcx_bb/driver_1.log 2>&1',
        command,
    )
    self.assertIn('PKB_TPCX_BB_DRIVER_RC=${PIPESTATUS[1]}', command)

  def testParseDriverExitCode(self):
    self.assertEqual(
        tpcx_bb.ParseDriverExitCode('x\nPKB_TPCX_BB_DRIVER_RC=124\n'), 124
    )
    self.assertIsNone(tpcx_bb.ParseDriverExitCode(''))

  def testParseResultValid(self):
    result = tpcx_bb.ParseResult(_DRIVER_LOG)
    self.assertEqual(
        result,
        tpcx_bb.BenchmarkResult(
            t_load=178.994,
            t_ld=17.8994,
            t_pt=1874.3939949734754,
            t_t_put=2387.966,
            t_tt=1193.983,
            bbqpm=118.89881969280403,
            scale_factor=100,
            valid=True,
            problems=[],
        ),
    )

  def testParseResultInfoPrefixAndInvalid(self):
    log = (
        _DRIVER_LOG.replace('VALID BBQpm', 'INVALID BBQpm')
        .replace('\nT_', '\nINFO: T_')
        .replace(
            'OK: All required BigBench phases were performed.',
            'NOK: Not all 30 queries ran',
        )
    )
    result = tpcx_bb.ParseResult(log)
    assert result is not None
    self.assertFalse(result.valid)
    self.assertEqual(result.bbqpm, 118.89881969280403)
    self.assertEqual(result.t_tt, 1193.983)
    self.assertEqual(result.problems, ['Not all 30 queries ran'])

  def testParseResultMissing(self):
    log = (
        'NOK: Some required BigBench phases were skipped. No final result '
        'available.\r\r\n'
    )
    self.assertIsNone(tpcx_bb.ParseResult(log))
    self.assertEqual(
        tpcx_bb.ParseProblems(log),
        [
            'Some required BigBench phases were skipped. No final result '
            'available.'
        ],
    )
    self.assertIsNone(
        tpcx_bb.ParseResult(_DRIVER_LOG.replace('VALID BBQpm', 'BBQpm'))
    )

  def testComputeBbqpmMatchesKit(self):
    self.assertAlmostEqual(
        tpcx_bb.ComputeBbqpm(100, 17.8994, 1874.3939949734754, 1193.983),
        118.89881969280403,
        places=6,
    )
    self.assertEqual(tpcx_bb.ComputeBbqpm(100, 0, 0, 0), 0.0)

  def testParsePhaseTimes(self):
    self.assertEqual(
        list(tpcx_bb.ParsePhaseTimes(_DRIVER_LOG).items()),
        [
            ('clean: Data', 3.856),
            ('preparation: Data generation', 748.466),
            ('phase 1: Load test', 178.994),
            ('phase 2: Power test', 2047.479),
            ('phase 3: First throughput test run', 2387.966),
        ],
    )

  def testParseTimesCsv(self):
    self.assertEqual(
        tpcx_bb.ParseTimesCsv(_TIMES_CSV, 100),
        [
            tpcx_bb.QueryTime('q01', 'power_test', 0, 29.0, True),
            tpcx_bb.QueryTime('q17', 'throughput_test_1', 1, 25.0, False),
        ],
    )
    self.assertEqual(tpcx_bb.ParseTimesCsv('', 100), [])


if __name__ == '__main__':
  unittest.main()

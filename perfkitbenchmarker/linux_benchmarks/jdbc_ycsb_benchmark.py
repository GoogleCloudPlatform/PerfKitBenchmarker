# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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

"""Run YCSB benchmark against managed SQL databases that support JDBC.

This benchmark does not provision VMs for the corresponding SQL database
cluster. The only VM group is client group that sends requests to specified
DB.

Before running this benchmark, you have to manually create `usertable` as
specified in YCSB JDBC binding.

Tested against Azure SQL database.

"""

import posixpath

from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import ycsb


BENCHMARK_NAME = 'jdbc_ycsb'
BENCHMARK_CONFIG = """
jdbc_ycsb:
  description: >
      Run YCSB against relational databases that support JDBC.
      Configure the number of VMs via --num-vms.
  vm_groups:
    default:
      vm_spec: *default_single_core
      vm_count: 1"""

YCSB_BINDING_LIB_DIR = posixpath.join(ycsb.YCSB_DIR, 'jdbc-binding', 'lib')
CREATE_TABLE_SQL = ("CREATE TABLE usertable "
                    "(YCSB_KEY VARCHAR(255) PRIMARY KEY, "
                    "FIELD0 TEXT, FIELD1 TEXT, "
                    "FIELD2 TEXT, FIELD3 TEXT, "
                    "FIELD4 TEXT, FIELD5 TEXT, "
                    "FIELD6 TEXT, FIELD7 TEXT, "
                    "FIELD8 TEXT, FIELD9 TEXT);")
DROP_TABLE_SQL = "DROP TABLE IF EXISTS usertable;"

FLAGS = flags.FLAGS
flags.DEFINE_string('jdbc_ycsb_db_driver',
                    None,
                    'The class of JDBC driver that connects to DB.')
flags.DEFINE_string('jdbc_ycsb_db_url',
                    None,
                    'The URL that is used to connect to DB')
flags.DEFINE_string('jdbc_ycsb_db_user',
                    None,
                    'The username of target DB.')
flags.DEFINE_string('jdbc_ycsb_db_passwd',
                    None,
                    'The password of specified DB user.')
flags.DEFINE_string('jdbc_ycsb_db_driver_path',
                    None,
                    'The path to JDBC driver jar file on local machine.')
flags.DEFINE_integer('jdbc_ycsb_db_batch_size',
                     0,
                     'The batch size for doing batched insert.')
flags.DEFINE_integer('jdbc_ycsb_fetch_size',
                     10,
                     'The JDBC fetch size hinted to driver')


def GetConfig(user_config):
    config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
    if FLAGS['ycsb_client_vms'].present:
        config['vm_groups']['default']['vm_count'] = FLAGS.ycsb_client_vms
    return config


def CheckPrerequisites(benchmark_config):
    # Before YCSB Cloud Datastore supports Application Default Credential,
    # we should always make sure valid credential flags are set.
    if not FLAGS.jdbc_ycsb_db_driver:
        raise ValueError('"jdbc_ycsb_db_driver" must be set')
    if not FLAGS.jdbc_ycsb_db_driver_path:
        raise ValueError('"jdbc_ycsb_db_driver_path" must be set')
    if not FLAGS.jdbc_ycsb_db_url:
        raise ValueError('"jdbc_ycsb_db_url" must be set')
    if not FLAGS.jdbc_ycsb_db_user:
        raise ValueError('"jdbc_ycsb_db_user" must be set ')
    if not FLAGS.jdbc_ycsb_db_passwd:
        raise ValueError('"jdbc_ycsb_db_passwd" must be set ')


def Prepare(benchmark_spec):
    benchmark_spec.always_call_cleanup = True
    vms = benchmark_spec.vms

    # Install required packages and copy credential files.
    vm_util.RunThreaded(_Install, vms)

    # Create benchmark table.
    ExecuteSql(vms[0], DROP_TABLE_SQL)
    ExecuteSql(vms[0], CREATE_TABLE_SQL)
    benchmark_spec.executor = ycsb.YCSBExecutor('jdbc')


def ExecuteSql(vm, sql):
    db_args = (
        ' -p db.driver={0}'
        ' -p db.url="{1}"'
        ' -p db.user={2}'
        ' -p db.passwd={3}').format(
        FLAGS.jdbc_ycsb_db_driver,
        FLAGS.jdbc_ycsb_db_url,
        FLAGS.jdbc_ycsb_db_user,
        FLAGS.jdbc_ycsb_db_passwd)

    exec_cmd = 'java -cp "{0}/*" com.yahoo.ycsb.db.JdbcDBCli -c "{1}" ' \
        .format(YCSB_BINDING_LIB_DIR, sql)
    stdout, stderr = vm.RobustRemoteCommand(exec_cmd + db_args)

    if 'successfully executed' not in stdout and not stderr:
        raise errors.VirtualMachine.RemoteCommandError(stderr)


def Run(benchmark_spec):
    vms = benchmark_spec.vms

    run_kwargs = {
        'db.driver': FLAGS.jdbc_ycsb_db_driver,
        'db.url': '"%s"' % FLAGS.jdbc_ycsb_db_url,
        'db.user': FLAGS.jdbc_ycsb_db_user,
        'db.passwd': FLAGS.jdbc_ycsb_db_passwd,
        'db.batchsize': FLAGS.jdbc_ycsb_db_batch_size,
        'jdbc.fetchsize': FLAGS.jdbc_ycsb_fetch_size,
    }
    load_kwargs = run_kwargs.copy()
    if FLAGS['ycsb_preload_threads'].present:
        load_kwargs['threads'] = FLAGS['ycsb_preload_threads']
    samples = list(benchmark_spec.executor.LoadAndRun(
        vms, load_kwargs=load_kwargs, run_kwargs=run_kwargs))
    return samples


def Cleanup(benchmark_spec):
    # support automatic cleanup.
    ExecuteSql(benchmark_spec.vms[0], DROP_TABLE_SQL)


def _Install(vm):
    vm.Install('ycsb')

    # Copy driver jar to VM.
    vm.RemoteCopy(FLAGS.jdbc_ycsb_db_driver_path, YCSB_BINDING_LIB_DIR)

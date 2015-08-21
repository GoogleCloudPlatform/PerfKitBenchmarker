# Copyright 2014 Google Inc. All rights reserved.
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

"""Runs Cloudsuite2.0 Web Serving benchmark
"""
import time

from perfkitbenchmarker import flags

"""Below are some constants used to set up the web serving benchmark.
It is only necessary to modify BASE_DIR to be the directory you want it to be
installed in (if this isn't set, it defaults to the home directory).
We may also want to modify the options for the workload.
"""
# options for the workload
LOAD_SCALE = '25'
DEFAULT_CLUSTER_SIZE = 3
BASE_DIR = '/home/mendiola/web-release'
# environment variables
CATALINA_HOME = '%s/apache-tomcat-6.0.35' % BASE_DIR
OLIO_HOME = '%s/apache-olio-php-src-0.2' % BASE_DIR
FABAN_HOME = '%s/faban' % BASE_DIR  
JAVA_HOME = '$(readlink -f $(which java) | cut -d "/" -f 1-5)'
MYSQL_HOME = '%s/mysql-5.5.20-linux2.6-x86_64' % BASE_DIR
GEOCODER_HOME = '%s/geo' % BASE_DIR
MYSQL_CLIENT_HOME = '%s/mysql-connector-java-5.0.8' % BASE_DIR
# base directories/build directories
OLIO_BUILD = '%s/apache-olio-php-src-0.2/workload/php/trunk/' % BASE_DIR
FABAN_RUN = '%s/faban/master/bin/startup.sh' % BASE_DIR
FABAN_SHUTDOWN = '%s/faban/master/bin/shutdown.sh' % BASE_DIR
OLIO_WORKLOAD_LIB = '%s/workload/php/trunk/lib' % OLIO_HOME
CATALINA_BUILD = '%s/apache-tomcat-6.0.35/bin' % BASE_DIR
FABAN_SERVICES = '%s/samples/services' % FABAN_HOME
APACHE_HTTPD = '%s/ApacheHttpdService/build' % FABAN_SERVICES
MYSQL_SERVICE = '%s/MysqlService/build' % FABAN_SERVICES
MEMCACHED_SERVICE = '%s/MemcachedService/build' % FABAN_SERVICES
# package names
TOMCAT = 'apache-tomcat-6.0.35.tar.gz'
FABAN = 'faban-kit-022311.tar.gz'
OLIO = 'apache-olio-php-src-0.2.tar.gz'
MYSQL_CLIENT = 'mysql-connector-java-5.0.8.tar.gz'
MYSQL = 'mysql-5.5.20-linux2.6-x86_64.tar.gz'
COMMONS_DAEMON = 'commons-daemon-native.tar.gz'
# File names
APACHE_HTTPD_JAR = '%s/ApacheHttpdService.jar' % APACHE_HTTPD
MYSQL_SERVICE_JAR = '%s/MySQLService.jar' % MYSQL_SERVICE
MEMCACHED_SERVICE_JAR = '%s/MemcachedService.jar' % MEMCACHED_SERVICE
MY_CNF = '/etc/my.cnf'

FLAGS = flags.FLAGS

BENCHMARK_INFO = {'name': 'webserving',
                  'description': 'Benchmark web2.0 apps with Cloudsuite',
                  'scratch_disk': False,
                  'num_machines': DEFAULT_CLUSTER_SIZE}


# install nginx, PHP, faban (agent)
def setupWebFronted(benchmark_spec):
  return


def setupBackend(benchmark_spec):
  vms = benchmark_spec.vms
  FRONTEND_IP = vms[1].ip_address
  CLIENT_IP = vms[0].ip_address
  vms[1].RemoteCommand('sudo yum install libaio')
  # vms[1].Install('libaio')   TODO: write an Install file for libaio
  untar_command = ('cd %s && tar xzf %s')
  vms[1].RemoteCommand(untar_command % (BASE_DIR, MYSQL))
  copy_command = ('cd %s && sudo cp support-files/my-medium.cnf %s')
  vms[1].RemoteCommand(copy_command % (MYSQL_HOME, MY_CNF))
  db_install_command = ('cd %s && scripts/mysql_install_db')
  vms[1].RemoteCommand(db_install_command % (MYSQL_HOME))
  vms[1].RobustRemoteCommand('cd %s && ./bin/mysqld_safe &' % MYSQL_HOME)
  vms[1].RemoteCommand('cd '
                       '/tmp/web-release/mysql-5.5.20-linux2.6-x86_64'
                       ' && ./bin/mysql -uroot -e "create user \'olio\'@\'%\' '
                       'identified by \'olio\';"')
  vms[1].RemoteCommand('cd %s && ./bin/mysql -uroot -e "grant all privileges on'
                       ' *.* to \'olio\'@\'localhost\' identified by \'olio\' '
                       'with grant option; grant all privileges on *.* to '
                       '\'olio\'@\'n127\' identified by \'olio\' with grant '
                       'option;"' % MYSQL_HOME)
  vms[1].RemoteCommand('cd %s && ./bin/mysql -uroot -e "create database olio;'
                       'use olio; \. %s/benchmarks/OlioDriver/bin/schema.sql"' 
                       % (MYSQL_HOME, FABAN_HOME))
  populate_db_command = ('export JAVA_HOME=%s &&cd %s/benchmarks/OlioDriver/bin'
                         '&& chmod +x dbloader.sh && ./dbloader.sh localhost '
                         '%s')
  vms[1].RemoteCommand(populate_db_command
                       % (JAVA_HOME, FABAN_HOME, LOAD_SCALE))
  vms[1].RemoteCommand('cd %s && ./bin/mysqladmin shutdown' % MYSQL_HOME)
  vms[1].RemoteCommand(untar_command % (BASE_DIR, TOMCAT))
  vms[1].RemoteCommand(untar_command % (CATALINA_BUILD, COMMONS_DAEMON))
  build_tomcat = ('export JAVA_HOME=%s && cd '
                  '%s/bin/commons-daemon-1.0.7-native-src/unix && ./configure&&'
                  'make && cp jsvc ../..')
  vms[1].RemoteCommand(build_tomcat % (JAVA_HOME, CATALINA_HOME))
  copy_geocoder = ('scp -r %s:%s/geocoder %s')
  vms[0].RemoteCommand('mkdir %s' % GEOCODER_HOME)
  vms[0].RemoteCommand(copy_geocoder % (CLIENT_IP, OLIO_HOME, GEOCODER_HOME))
  vms[0].RemoteCommand('cd %s/geocoder && cp build.properties.template '
                       'build.properties' % GEOCODER_HOME)
  editor_command = ('perl -pi -e '
                    '"s/\/usr\/local\/apache-tomcat-6.0.13\/lib/%s\/lib/g"'
                    ' %s/geocoder/build.properties')
  vms[0].RemoteCommand(editor_command %
                       ('\/tmp\/web-release\/apache-tomcat-6.0.35',
                        GEOCODER_HOME))
  vms[0].RemoteCommand('cd %s/geocoder && ant all &&'
                       'cp dist/geocoder.war %s/webapps'
                       % (GEOCODER_HOME, CATALINA_HOME))
  run_tomcat = ('%s/bin/startup.sh')
  vms[0].RemoteCommand(run_tomcat % CATALINA_HOME)
  return


def setupClient(benchmark_spec):
  vms = benchmark_spec.vms
  untar_command = ('cd %s && tar xzf %s')
  vms[0].RemoteCommand(untar_command % (BASE_DIR, FABAN))
  vms[0].RemoteCommand(untar_command % (BASE_DIR, OLIO))
  vms[0].RemoteCommand(untar_command % (BASE_DIR, MYSQL_CLIENT))
  copy_command = ('cp %s %s')
  vms[0].RemoteCommand('cp %s/mysql-connector-java-5.0.8-bin.jar %s'
                       % (MYSQL_CLIENT_HOME, OLIO_WORKLOAD_LIB))
  copy_command2 = ('cp %s %s/services && cp %s %s/services &&cp %s %s/services')
  vms[0].RemoteCommand(copy_command2 % (APACHE_HTTPD_JAR, FABAN_HOME,
                       MYSQL_SERVICE_JAR, FABAN_HOME, MEMCACHED_SERVICE_JAR,
                       FABAN_HOME))
  vms[0].RemoteCommand('cd %s/workload/php/trunk &&'
                       'cp build.properties.template build.properties'
                       % OLIO_HOME)
  vms[0].RemoteCommand('perl -pi -e '
                       '"s/\/export\/home\/faban/\/home\/mendiola\/web-release'
                       '\/faban/g" %s/workload/php/trunk/build.properties'
                       % OLIO_HOME)
  vms[0].RemoteCommand('perl -pi -e "s/host.sfbay/localhost/g" '
                       '%s/workload/php/trunk/build.properties'
                       % OLIO_HOME)
  build_command = ('cd %s && ant deploy.jar')
  vms[0].RemoteCommand(build_command % OLIO_BUILD)
  vms[0].RemoteCommand('cp %s/workload/php/trunk/build/OlioDriver.jar '
                       '%s/benchmarks' % (OLIO_HOME, FABAN_HOME))
  set_java = ('export JAVA_HOME=%s && %s')
  vms[0].RemoteCommand(set_java % (JAVA_HOME, FABAN_RUN))
  time.sleep(70)
  vms[0].RemoteCommand(set_java % (JAVA_HOME, FABAN_SHUTDOWN))
  return


def GetInfo():
  return BENCHMARK_INFO


def CheckPrerequisites():
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  if FLAGS['num_vms'].present and FLAGS.num_vms < 3:
    raise ValueError('Web Serving requires at least 3 VMs')
  return


def Prepare(benchmark_spec):
  """Install Java, apache ant
     Set up the client machine, backend machine, and frontend

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  CLIENT_IP = vms[0].ip_address
  BACKEND_IP = vms[1].ip_address
  FRONTEND_IP = vms[2].ip_address
  vms[0].RemoteCommand('sudo yum install ant')
  # vm.Install('ant')  TODO: switch to this cmmd
  vms[0].Install('wget')
  vms[0].Install('openjdk7')
  vms[0].RemoteCommand('cd /tmp &&'
                       'wget parsa.epfl.ch/cloudsuite/software/web.tar.gz')
  vms[0].RemoteCommand('cd /tmp && tar xzf web.tar.gz')
  vms[1].RemoteCommand('sudo yum install ant')
  vms[1].Install('wget')
  vms[1].Install('openjdk7')
  vms[1].RemoteCommand('cd /tmp &&'
                       'wget parsa.epfl.ch/cloudsuite/software/web.tar.gz')
  vms[1].RemoteCommand('cd /tmp && tar xzf web.tar.gz')
  setupClient(benchmark_spec)
  copy_faban = ('scp -r %s:%s %s')
  vms[1].RemoteCommand(copy_faban % (CLIENT_IP, FABAN_HOME, BACKEND_IP))
  vms[2].RemoteCommand(copy_faban % (CLIENT_IP, FABAN_HOME, FRONTEND_IP))
  setupBackend(benchmark_spec)
  return


def CollectResultFile(vm, interval_op_rate_list, interval_key_rate_list,
                      latency_median_list, latency_95th_list,
                      latency_99_9th_list,
                      total_operation_time_list):
  """Collect result file on vm.

  Args:
    vm: The target vm.
    interval_op_rate_list: The list stores interval_op_rate.
    interval_key_rate_list: The list stores interval_key_rate.
    latency_median_list: The list stores latency median.
    latency_95th_list: The list stores latency 95th percentile.
    latency_99_9th_list: The list stores latency 99.9th percentile.
    total_operation_time_list: The list stores total operation time.
  """
  return


def CollectResults(benchmark_spec):
  """Run Cassandra on target vms.

  Args:
    benchmark_spec: The benchmark specification. Contains all data
        that is required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  return


def Run(benchmark_spec):
  vms = benchmark_spec.vms
  set_java = ('export JAVA_HOME=$(readlink -f $(which java)|cut -d "/" -f 1-5)'
              '&& %s')
  vms[0].RemoteCommand(set_java % (FABAN_RUN))  # run FABAN on the client
  vms[1].RobustRemoteCommand('cd %s && ./bin/mysqld_safe &' % MYSQL_HOME)
  time.sleep(200)
  return


def Cleanup(benchmark_spec):
  """Cleanup function.

  Args:
    benchmark_spec: The benchmark specification. Contains all data
        that is required to run the benchmark.
  """
   vms = benchmark_spec.vms
  # vms[0].RemoteCommand('sudo yum remove ant')
   set_java = ('export JAVA_HOME=$(readlink -f $(which java)|cut -d "/" -f 1-5)'
               '&& %s')
   vms[0].RemoteCommand(set_java % FABAN_SHUTDOWN)
   vms[1].RemoteCommand('cd %s && sudo ./bin/mysqladmin shutdown' % MYSQL_HOME)
   vms[0].RemoteCommand('rm -fr /tmp/web-release web.tar.gz')
   vms[1].RemoteCommand('rm -fr /tmp/web-release web.tar.gz')
  return

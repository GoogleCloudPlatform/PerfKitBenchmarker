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
import posixpath
import re

from perfkitbenchmarker import vm_util
from perfkitbenchmarker import flags
from perfkitbenchmarker.packages import nginx
from perfkitbenchmarker.packages import php
# TODO: from perfkitbenchmarker import sample
from perfkitbenchmarker.packages.openjdk7 import JAVA_HOME

"""Below are some constants used to set up the web serving benchmark.
It is only necessary to modify BASE_DIR to be the directory you want it to be
installed in (if this isn't set, it defaults to the home directory).
We may also want to modify the options for the workload.
"""
# options for the workload
LOAD_SCALE = '25'
DEFAULT_CLUSTER_SIZE = 3
BASE_DIR = posixpath.join(vm_util.VM_TMP_DIR, 'web-release')
# environment variables
CATALINA_HOME = '%s/apache-tomcat-6.0.35' % BASE_DIR
OLIO_HOME = '%s/apache-olio-php-src-0.2' % BASE_DIR
FABAN_HOME = '%s/faban' % BASE_DIR
MYSQL_HOME = '%s/mysql-5.5.20-linux2.6-x86_64' % BASE_DIR
GEOCODER_HOME = '%s' % BASE_DIR
APP_DIR = '%s/app' % BASE_DIR
PHPRC = posixpath.join(APP_DIR, 'etc')
# base directories/build directories
OLIO_BUILD = '%s/apache-olio-php-src-0.2/workload/php/trunk/' % BASE_DIR
FABAN_RUN = '%s/faban/master/bin/startup.sh' % BASE_DIR
FABAN_SHUTDOWN = '%s/faban/master/bin/shutdown.sh' % BASE_DIR
OLIO_WORKLOAD_LIB = '%s/workload/php/trunk/lib' % OLIO_HOME
CATALINA_BUILD = '%s/apache-tomcat-6.0.35/bin' % BASE_DIR
# package names
TOMCAT = 'apache-tomcat-6.0.35.tar.gz'
FABAN = 'faban-kit-022311.tar.gz'
OLIO = 'apache-olio-php-src-0.2.tar.gz'
MYSQL_CLIENT = 'mysql-connector-java-5.0.8.tar.gz'
MYSQL = 'mysql-5.5.20-linux2.6-x86_64.tar.gz'
COMMONS_DAEMON = 'commons-daemon-native.tar.gz'
# File names
MYSQL_CONNECTOR_JAR = '%s/mysql-connector-java-5.0.8/' % BASE_DIR
'mysql-connector-java-5.0.8-bin.jar'
APACHE_HTTPD_JAR = '%s/samples/services/ApacheHttpdService' % FABAN_HOME
'/build/ApacheHttpdService.jar'
MYSQL_SERVICE_JAR = '%s/samples/services/MysqlService' % FABAN_HOME
'/build/MySQLService.jar'
MEMCACHED_SERVICE_JAR = '%s/samples/services/MemcachedService' % FABAN_HOME
'/build/MemcachedService.jar'
MY_CNF = '/etc/my.cnf'
# TODO: change to '%s/nginx.conf' % BASE_DIR
NGINX_CONF = '/home/vstefano/web_serving/web-release/nginx.conf'

FLAGS = flags.FLAGS

BENCHMARK_INFO = {'name': 'webserving',
                  'description': 'Benchmark web2.0 apps with Cloudsuite',
                  'scratch_disk': True,
                  'num_machines': DEFAULT_CLUSTER_SIZE}


# install nginx, PHP, faban (agent)
def setupFrontend(benchmark_spec):
  frontend = benchmark_spec.vms[2]
  backend = benchmark_spec.vms[1]
  frontend.Install('nginx')
  frontend.RemoteCommand('perl -pi -e '
                         '"s/%s/%s/g"'
                         ' %s'
                         % ('APP_DIR', re.escape(APP_DIR), NGINX_CONF))
  frontend.RemoteCommand('sudo cp -f %s /usr/local/nginx/conf/'
                         % NGINX_CONF)
  frontend.RemoteCommand('mkdir -p %s'
                         % APP_DIR)
  frontend.RemoteCommand('cd %s && '
                         'tar xzf %s'
                         % (BASE_DIR, OLIO))
  frontend.RemoteCommand('cp -r %s/webapp/php/trunk/* %s'
                         % (OLIO_HOME, APP_DIR))
  frontend.RemoteCommand('cp %s/cloudstone.patch %s'
                         % (BASE_DIR, APP_DIR))
  frontend.RemoteCommand('cd %s && '
                         'patch -p1 < cloudstone.patch'
                         % APP_DIR)
  frontend.RemoteCommand('perl -pi -e '
                         '"s/%s/%s/g"'
                         ' %s/etc/config.php'
                         % ('(\$olioconfig\[\'dbTarget\'\]).*',
                            '\$1 = \'mysql:host=%s;'
                            'dbname=olio\';' % backend.ip_address,
                            APP_DIR))
  frontend.RemoteCommand('perl -pi -e '
                         '"s/%s/%s/g"'
                         ' %s/etc/config.php'
                         % ('(.*olioconfig.*cacheSystem.*MemCached.*)',
                            '\/\/\$1', APP_DIR))
  frontend.RemoteCommand('perl -pi -e '
                         '"s/%s/%s/g"'
                         ' %s/etc/config.php'
                         % ('\/\/(.*olioconfig.*cacheSystem.*NoCache.*)',
                            '\$1', APP_DIR))
  frontend.RemoteCommand('perl -pi -e '
                         '"s/%s/%s/g"'
                         ' %s/etc/config.php'
                         % ('GEOCODER_HOST', backend.ip_address, APP_DIR))
  frontend.Install('php')
  php.ConfigureAndBuild(frontend, PHPRC, True)
  frontend.RemoteCommand('mkdir -p /tmp/http_sessions && '
                         'chmod 777 /tmp/http_sessions')
  frontend.RemoteCommand('echo """extension_dir=\\"/usr/local/lib/php/'
                         'extensions/no-debug-non-zts-20090626/\\""""'
                         ' >> %s/php.ini'
                         % PHPRC)
  frontend.RemoteCommand('echo """date.timezone=\\"Europe/Zurich\\"""" >> '
                         '%s/php.ini'
                         % PHPRC)
  frontend.RemoteCommand('perl -pi -e '
                         '"s/%s/%s/g"'
                         ' %s/php.ini'
                         % ('(display_errors).*', '\$1 = Off', PHPRC))
  frontend.RemoteCommand('perl -pi -e '
                         '"s/%s/%s/g"'
                         ' %s/php.ini'
                         % ('(error_reporting).*', '\$1 = E_ALL & ~E_NOTICE',
                            PHPRC))
  php.InstallAPC(frontend)

  def setupFilestore(vm):
    filestore = posixpath.join(vm.GetScratchDir(), 'filestore')
    vm.RemoteCommand('cd %s && '
                     'patch -p1 -t < %s/cloudsuite.patch && '
                     'mkdir -p %s && '
                     'chmod a+rwx %s && '
                     'chmod +x %s/benchmarks/OlioDriver/bin/fileloader.sh'
                     % (APP_DIR, BASE_DIR, filestore, filestore,
                        FABAN_HOME))
    vm.RemoteCommand('export FILESTORE=%s && '
                     'export JAVA_HOME=%s && '
                     '%s/benchmarks/OlioDriver/bin/fileloader.sh 102 %s'
                     % (filestore, JAVA_HOME, FABAN_HOME, filestore))
    frontend.RemoteCommand('perl -pi -e '
                           '"s/%s/%s/g"'
                           ' %s/etc/config.php'
                           % ('(\$olioconfig\[\'localfsRoot\'\]).*',
                              '\$1 = \'%s\';' % re.escape(filestore),
                              APP_DIR))
    return

  frontend.RemoteCommand('cp -R /home/vstefano/web_serving/web-release/faban %s'
                         % BASE_DIR)  # TODO: remove this and scp from client vm
  setupFilestore(frontend)
  frontend.RemoteCommand('sudo cp /usr/local/etc/php-fpm.conf.default '
                         '/usr/local/etc/php-fpm.conf && '
                         'sudo /usr/local/sbin/php-fpm')
  nginx.Start(frontend, benchmark_spec.firewall)
  return


# install mysql, faban (agent), and tomcat
# configure the mysql database
def setupBackend(benchmark_spec):
  vms = benchmark_spec.vms
  # CLIENT_IP = vms[0].ip_address
  vms[0].RemoteCommand('sudo yum install libaio')
  # vms[1].Install('libaio')   TODO: write an Install file for libaio
  untar_command = ('cd %s && tar xzf %s')
  vms[0].RemoteCommand(untar_command % (BASE_DIR, MYSQL))
  copy_command = ('cd %s && sudo cp support-files/my-medium.cnf %s')
  vms[0].RemoteCommand(copy_command % (MYSQL_HOME, MY_CNF))
  db_install_command = ('cd %s && scripts/mysql_install_db')
  vms[0].RemoteCommand(db_install_command % (MYSQL_HOME))
  vms[0].RemoteCommand('cd '
                       '/home/mendiola/web-release/mysql-5.5.20-linux2.6-x86_64'
                       '&& bin/mysqld_safe&')
  vms[0].RemoteCommand('cd '
                       '/home/mendiola/web-release/mysql-5.5.20-linux2.6-x86_64'
                       '&& bin/mysql -uroot -e "create user \'olio\'@\'%\' '
                       'identified by \'olio\';"')
  vms[0].RemoteCommand('cd %s && bin/mysql -uroot -e '
                       '"grant all privileges on *.* to \'olio\'@\'localhost\' '
                       'identified by \'olio\' with grant option;grant all '
                       'privileges on *.* to \'olio\'@\'n127\' identified by'
                       ' \'olio\' with grant option;"' % MYSQL_HOME)
  vms[0].RemoteCommand('cd %s && bin/mysql -uroot -e "create database olio;'
                       'use olio;\. %s/benchmarks/OlioDriver/bin/schema.sql"'
                       % (MYSQL_HOME, FABAN_HOME))
  shutdown_mysql_command = ('cd %s && bin/mysqladmin shutdown')
  vms[0].RemoteCommand(shutdown_mysql_command % MYSQL_HOME)
  # populate_db_command = ('export JAVA_HOME=%s && '
  #                        'cd %s/benchmarks/OlioDriver/bin'
  #                       '&& chmod +x dbloader.sh && ./dbloader.sh localhost '
  #                       '%s')
  """
  vms[0].RemoteCommand(populate_db_command
                       % (JAVA_HOME, FABAN_HOME, LOAD_SCALE))
  time.sleep(370)
  vms[0].RemoteCommand(untar_command % (BASE_DIR, TOMCAT))
  vms[0].RemoteCommand(untar_command % (CATALINA_BUILD, COMMONS_DAEMON))
  build_tomcat = ('export JAVA_HOME=%s && cd '
                  '%s/bin/commons-daemon-1.0.7-native-src/unix && ./configure&&'
                  'make && cp jsvc ../..')
  vms[0].RemoteCommand(build_tomcat % (JAVA_HOME, CATALINA_HOME))
  copy_geocoder = ('scp -r %s:%s/geocoder %s')
  vms[0].RemoteCommand(copy_geocoder % (CLIENT_IP, OLIO_HOME, GEOCODER_HOME))
  vms[0].RemoteCommand('cd %s/geocoder && cp build.properties.template '
                       'build.properties' % GEOCODER_HOME)
  editor_command = ('perl -pi -e '
                    '"s/\/usr\/local\/apache-tomcat-6.0.13\/lib/%s\/lib/g"'
                    ' %s/geocoder/build.properties')
  vms[0].RemoteCommand(editor_command %
                       ('\/home\/mendiola\/web-release\/apache-tomcat-6.0.13',
                        GEOCODER_HOME))
  vms[0].RemoteCommand('cd %s/geocoder && ant &&'
                       'cp dist/geocoder.war %s/webapps'
                       % (GEOCODER_HOME, CATALINA_HOME))
  # TODO: fix the ant build here
  run_tomcat = ('%s/bin/startup.sh')
  vms[0].RemoteCommand(run_tomcat % CATALINA_HOME)
  """
  return


# setup faban driver
def setupClient(benchmark_spec):
  vms = benchmark_spec.vms
  untar_command = ('cd %s && tar xzf %s')
  vms[0].RemoteCommand(untar_command % (BASE_DIR, FABAN))
  vms[0].RemoteCommand(untar_command % (BASE_DIR, OLIO))
  vms[0].RemoteCommand(untar_command % (BASE_DIR, MYSQL_CLIENT))
  copy_command = ('cp %s %s')
  vms[0].RemoteCommand(copy_command % (MYSQL_CONNECTOR_JAR, OLIO_WORKLOAD_LIB))
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
  for vm in vms:
    download_command = ('cd %s && '
                        'wget parsa.epfl.ch/cloudsuite/software/web.tar.gz')
    vm.RemoteCommand(download_command % vm_util.VM_TMP_DIR)
    vm.Install('openjdk7')
  vms[0].RemoteCommand('sudo yum install ant')
  # vm.Install('ant')  TODO: switch to this cmmd
  vms[0].Install('wget')
  vms[1].RemoteCommand('sudo yum install ant')
  vms[1].Install('wget')
  vms[1].Install('openjdk7')
  setupClient(benchmark_spec)
  time.sleep(90)
  # TODO: scp from client to backend and frontend
  setupBackend(benchmark_spec)
  vms[2].RemoteCommand('cd %s && '
                       'tar xzf web.tar.gz'
                       % vm_util.VM_TMP_DIR)
  setupFrontend(benchmark_spec)  # TODO: RunThreaded?
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
  """Run Cloudsuite web serving benchmark on target vms.

  Args:
    benchmark_spec: The benchmark specification. Contains all data
        that is required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  return


def Run(benchmark_spec):
  client = benchmark_spec.vms[0]
  client.RobustRemoteCommand('cd %s && '
                             'cp -f /home/vstefano/web_serving/web-release/'
                             'run.sh . && '
                             'cp -f /home/vstefano/web_serving/web-release/'
                             'run.xml . && '
                             'cp -f /home/vstefano/web_serving/web-release/'
                             'driver.policy . && '
                             'export FABAN_HOME=%s && '
                             'sh run.sh'  # TODO: fix script
                             % ('/home/vstefano/web_serving/web-release/faban',
                                '/home/vstefano/web_serving/web-release/faban'))
  # TODO: real FABAN_HOME on client machine

  def ParseOutput(client):
    stdout, _ = client.RemoteCommand('cat tmp/output/summary.xml')
    # TODO: check if the output will be there once the script is fixed.
    print stdout
    return stdout  # TODO: Parse and return what matters

  results = []
  # ParseOutput(client)
  # results.append(sample.Sample('Operations per second', sum_ops_per_sec,
  #                              'ops/s'))
  return results


def Cleanup(benchmark_spec):
  """Cleanup function.

  Args:
    benchmark_spec: The benchmark specification. Contains all data
        that is required to run the benchmark.
  """
  # vms = benchmark_spec.vms
  # vms[0].RemoteCommand('sudo yum remove ant')
  # vms[0].RemoteCommand('~/web-release/faban/master/bin/shutdown.sh')
  # vms[0].RemoteCommand('rm -fr web-release web.tar.gz')
  # vms[1].RemoteCommand('rm -fr web-release web.tar.gz')
  cleanupFrontend(benchmark_spec)
  return


def cleanupFrontend(benchmark_spec):
  frontend = benchmark_spec.vms[2]
  frontend.RemoteCommand('sudo killall -9 php-fpm')
  frontend.RemoteCommand('cp -f %s.bak %s'
                         % (NGINX_CONF, NGINX_CONF))  # TODO:
  # Delete this once nginx.conf is incorporated into web_serving download
  nginx.Stop(frontend)
  filestore = posixpath.join(frontend.GetScratchDir(), 'filestore')
  frontend.RemoteCommand('rm -R %s'
                         % filestore)
  return

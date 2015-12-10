# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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
   More info: http://parsa.epfl.ch/cloudsuite/web.html
"""
import logging
import posixpath
import re
import time

from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import flags
from perfkitbenchmarker.linux_packages import nginx
from perfkitbenchmarker.linux_packages import php

# environment variables
BASE_DIR = posixpath.join(vm_util.VM_TMP_DIR, 'web-release')
CATALINA_HOME = '%s/apache-tomcat-6.0.35' % BASE_DIR
OLIO_HOME = '%s/apache-olio-php-src-0.2' % BASE_DIR
FABAN_HOME = '%s/faban' % BASE_DIR
MYSQL_HOME = '%s/mysql-5.5.20-linux2.6-x86_64' % BASE_DIR
GEOCODER_HOME = '%s/geo' % BASE_DIR
APP_DIR = '%s/app' % BASE_DIR
JAVA_HOME = '$(readlink -f $(which java) | cut -d "/" -f 1-5)'
PHPRC = posixpath.join(APP_DIR, 'etc')
OUTPUT_DIR = posixpath.join(vm_util.VM_TMP_DIR, 'out1')
ANT_HOME = posixpath.join(vm_util.VM_TMP_DIR, 'ant/bin')
OLIO_BUILD = '%s/apache-olio-php-src-0.2/workload/php/trunk/' % BASE_DIR
# File names
MY_CNF = '/etc/my.cnf'
NGINX_CONF = '%s/nginx.conf' % BASE_DIR

flags.DEFINE_integer('cloudsuite_web_serving_load_scale', 100,
                     'The maximum number of concurrent users '
                     'that can be simulated.', lower_bound=2)
FLAGS = flags.FLAGS

BENCHMARK_NAME = 'cloudsuite_web_serving'
BENCHMARK_CONFIG = """
cloudsuite_web_serving:
  description: Runs Cloudsuite Web Serving benchmark
  vm_groups:
    backend:
      vm_spec: *default_single_core
      disk_spec: *default_500_gb
    frontend:
      vm_spec: *default_single_core
      disk_spec: *default_500_gb
    client:
      vm_spec: *default_single_core
"""


def _SetupFrontend(benchmark_spec):
  frontend = benchmark_spec.vm_groups['frontend'][0]
  backend = benchmark_spec.vm_groups['backend'][0]
  frontend.Install('nginx')
  frontend.RemoteCommand('perl -pi -e "s/%s/%s/g" %s'
                         % ('APP_DIR', re.escape(APP_DIR), NGINX_CONF))
  frontend.RemoteCommand('sudo cp -f %s /usr/local/nginx/conf/' % NGINX_CONF)
  frontend.RemoteCommand('mkdir -p %s' % APP_DIR)
  frontend.RemoteCommand('cd %s && tar xzf apache-olio-php-src-0.2.tar.gz'
                         % (BASE_DIR))
  frontend.RemoteCommand('cp -r %s/webapp/php/trunk/* %s'
                         % (OLIO_HOME, APP_DIR))
  frontend.RemoteCommand('cp %s/cloudstone.patch %s' % (BASE_DIR, APP_DIR))
  frontend.RemoteCommand('cd %s && patch -p1 < cloudstone.patch' % APP_DIR)
  frontend.RemoteCommand('perl -pi -e "s/%s/%s/g" %s/etc/config.php'
                         % ('(\$olioconfig\[\'dbTarget\'\]).*',
                            '\$1 = \'mysql:host=%s;dbname=olio\';'
                            % backend.ip_address, APP_DIR))
  frontend.RemoteCommand('perl -pi -e "s/%s/%s/g" %s/etc/config.php'
                         % ('(.*olioconfig.*cacheSystem.*MemCached.*)',
                            '\/\/\$1', APP_DIR))
  frontend.RemoteCommand('perl -pi -e "s/%s/%s/g" %s/etc/config.php'
                         % ('\/\/(.*olioconfig.*cacheSystem.*NoCache.*)',
                            '\$1', APP_DIR))
  frontend.RemoteCommand('perl -pi -e "s/%s/%s/g" %s/etc/config.php'
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
  frontend.RemoteCommand('perl -pi -e "s/%s/%s/g" %s/php.ini'
                         % ('(display_errors).*', '\$1 = Off', PHPRC))
  frontend.RemoteCommand('perl -pi -e "s/%s/%s/g" %s/php.ini'
                         % ('(error_reporting).*', '\$1 = E_ALL & ~E_NOTICE',
                            PHPRC))
  php.InstallAPC(frontend)

  def _SetupFilestore(vm):
    filestore = posixpath.join(vm.GetScratchDir(), 'filestore')
    vm.RemoteCommand('cd %s && patch -p1 -t < %s/cloudsuite.patch && '
                     'mkdir -p %s && chmod a+rwx %s && '
                     'chmod +x %s/benchmarks/OlioDriver/bin/fileloader.sh'
                     % (APP_DIR, BASE_DIR, filestore, filestore,
                        FABAN_HOME))
    vm.RemoteCommand('export FILESTORE=%s && export JAVA_HOME=%s && '
                     '%s/benchmarks/OlioDriver/bin/fileloader.sh 102 %s'
                     % (filestore, JAVA_HOME, FABAN_HOME, filestore))
    frontend.RemoteCommand('perl -pi -e "s/%s/%s/g" %s/etc/config.php'
                           % ('(\$olioconfig\[\'localfsRoot\'\]).*',
                              '\$1 = \'%s\';' % re.escape(filestore),
                              APP_DIR))

  _SetupFilestore(frontend)
  frontend.RemoteCommand('sudo cp /usr/local/etc/php-fpm.conf.default '
                         '/usr/local/etc/php-fpm.conf && '
                         'sudo /usr/local/sbin/php-fpm')
  nginx.Start(frontend, frontend.firewall)


def _SetupBackend(benchmark_spec):
  frontend = benchmark_spec.vm_groups['frontend'][0]
  backend = benchmark_spec.vm_groups['backend'][0]
  client_ip = benchmark_spec.vm_groups['client'][0].ip_address
  frontend_ip = frontend.ip_address
  backend.Install('libaio')
  untar_command = ('cd %s && tar xzf %s')
  backend.RemoteCommand(untar_command %
                        (BASE_DIR, 'mysql-5.5.20-linux2.6-x86_64.tar.gz'))
  copy_command = ('cd %s && sudo cp support-files/my-medium.cnf %s')
  backend.RemoteCommand(copy_command % (MYSQL_HOME, MY_CNF))
  db_install_command = ('cd %s && scripts/mysql_install_db')
  backend.RemoteCommand(db_install_command % (MYSQL_HOME))
  backend.RobustRemoteCommand('cd %s && ./bin/mysqld_safe &' % MYSQL_HOME)

  # Wait for mysql to start
  logging.info('Sleeping 30s for mysql to come up')
  time.sleep(30)
  sql_cmd = 'create user \'olio\'@\'%\' identified by \'olio\';'
  backend.RemoteCommand('cd %s && '
                        './bin/mysql -uroot -e "%s"' % (MYSQL_HOME, sql_cmd))
  backend.RemoteCommand('cd %s && ./bin/mysql -uroot -e "grant all privileges '
                        'on *.* to \'olio\'@\'localhost\' '
                        'identified by \'olio\' with grant option; '
                        'grant all privileges on *.* to '
                        '\'olio\'@\'%s\' identified by \'olio\' with grant '
                        'option;"' % (MYSQL_HOME, frontend_ip))
  backend.RemoteCommand('cd %s && ./bin/mysql -uroot -e "create database olio;'
                        'use olio; \. %s/benchmarks/OlioDriver/bin/schema.sql"'
                        % (MYSQL_HOME, FABAN_HOME))
  populate_db_command = ('export JAVA_HOME=%s && '
                         'cd %s/benchmarks/OlioDriver/bin'
                         '&& chmod +x dbloader.sh && ./dbloader.sh localhost '
                         '%s')
  backend.RobustRemoteCommand(populate_db_command
                              % (JAVA_HOME, FABAN_HOME,
                                 FLAGS.cloudsuite_web_serving_load_scale))
  backend.RemoteCommand(untar_command %
                        (BASE_DIR, 'apache-tomcat-6.0.35.tar.gz'))
  backend.RemoteCommand('cd %s/apache-tomcat-6.0.35/bin && '
                        'tar xzf commons-daemon-native.tar.gz' % (BASE_DIR))
  build_tomcat = ('export JAVA_HOME=%s && cd '
                  '%s/bin/commons-daemon-1.0.7-native-src/unix && ./configure&&'
                  'make -j && cp jsvc ../..')
  backend.InstallPackages('gcc build-essential')
  backend.RemoteCommand(build_tomcat % (JAVA_HOME, CATALINA_HOME))
  backend.RemoteCommand('mkdir %s' % GEOCODER_HOME)
  backend.RemoteCommand('scp -r -o StrictHostKeyChecking=no %s:%s/geocoder %s' %
                        (client_ip, OLIO_HOME, GEOCODER_HOME))
  backend.RemoteCommand('cd %s/geocoder && cp build.properties.template '
                        'build.properties' % GEOCODER_HOME)
  editor_command = ('perl -pi -e '
                    '"s/\/usr\/local\/apache-tomcat-6.0.13\/lib/%s\/lib/g"'
                    ' %s/geocoder/build.properties')
  backend.RemoteCommand(editor_command %
                        ('\/tmp\/pkb\/web-release\/apache-tomcat-6.0.35',
                         GEOCODER_HOME))
  backend.RemoteCommand('cd %s/geocoder && %s/ant all &&'
                        'cp dist/geocoder.war %s/webapps'
                        % (GEOCODER_HOME, ANT_HOME, CATALINA_HOME))
  run_tomcat = ('%s/bin/startup.sh')
  backend.RemoteCommand(run_tomcat % CATALINA_HOME)


def _SetupClient(benchmark_spec):
  frontend = benchmark_spec.vm_groups['frontend'][0]
  backend = benchmark_spec.vm_groups['backend'][0]
  client = benchmark_spec.vm_groups['client'][0]
  fw = client.firewall
  fw.AllowPort(client, 9980)
  CLIENT_IP = client.ip_address
  BACKEND_IP = backend.ip_address
  FRONTEND_IP = frontend.ip_address
  untar_command = ('cd %s && tar xzf %s')
  client.RemoteCommand(untar_command % (BASE_DIR, 'faban-kit-022311.tar.gz'))
  client.RemoteCommand(untar_command %
                       (BASE_DIR, 'apache-olio-php-src-0.2.tar.gz'))
  client.RemoteCommand(untar_command %
                       (BASE_DIR, 'mysql-connector-java-5.0.8.tar.gz'))
  client.RemoteCommand('cp %s/mysql-connector-java-5.0.8/mysql-connector'
                       '-java-5.0.8-bin.jar %s/workload/php/trunk/lib'
                       % (BASE_DIR, OLIO_HOME))
  copy_command2 = ('cp %s/samples/services/ApacheHttpdService/build/'
                   'ApacheHttpdService.jar %s/services && cp %s/samples/'
                   'services/MysqlService/build/MySQLService.jar '
                   '%s/services &&cp %s/samples/services/'
                   '/MemcachedService/build/MemcachedService.jar %s/services')
  client.RemoteCommand(copy_command2 % (FABAN_HOME, FABAN_HOME,
                       FABAN_HOME, FABAN_HOME, FABAN_HOME,
                       FABAN_HOME))
  client.RemoteCommand('cd %s/workload/php/trunk &&'
                       'cp build.properties.template build.properties'
                       % OLIO_HOME)
  client.RemoteCommand('perl -pi -e '
                       '"s/\/export\/home\/faban/%s'
                       '/g" %s/workload/php/trunk/build.properties'
                       % (re.escape(FABAN_HOME), OLIO_HOME))
  client.RemoteCommand('perl -pi -e "s/host.sfbay/localhost/g" '
                       '%s/workload/php/trunk/build.properties'
                       % OLIO_HOME)
  build_command = ('cd %s && %s/ant deploy.jar')
  client.RemoteCommand(build_command % (OLIO_BUILD, ANT_HOME))
  client.RemoteCommand('cp %s/workload/php/trunk/build/OlioDriver.jar '
                       '%s/benchmarks' % (OLIO_HOME, FABAN_HOME))
  set_java = ('export JAVA_HOME=%s && %s/master/bin/startup.sh')
  client.RemoteCommand(set_java % (JAVA_HOME, FABAN_HOME))
  client.Install('curl')
  client.RemoteCommand('curl http://%s:9980/' % CLIENT_IP)
  client.RemoteCommand('cd %s '
                       '&& wget parsa.epfl.ch/cloudsuite/software/perfkit/'
                       'web_serving/webservingfiles.tgz && '
                       'tar xzf webservingfiles.tgz' % BASE_DIR)
  client.RobustRemoteCommand('cd %s && '
                             'cp -f %s'
                             '/run.sh . && '
                             'cp -f %s'
                             '/run.xml . && '
                             'cp -f %s'
                             '/driver.policy . && '
                             'cp -rf %s/build . && '
                             'mkdir -p lib_olio && '
                             'cp -rf %s/* lib_olio/ && '
                             'cp -rf %s/resources build/ && '
                             'export FABAN_HOME=%s  '
                             % (FABAN_HOME, BASE_DIR, BASE_DIR, BASE_DIR,
                                OLIO_BUILD, OLIO_BUILD, OLIO_BUILD,
                                FABAN_HOME))
  client.RemoteCommand('perl -pi -e '
                       '"s/CLIENT_IP/%s/g"'
                       ' %s/run.xml' % (CLIENT_IP, FABAN_HOME))
  client.RemoteCommand('perl -pi -e '
                       '"s/FRONTEND_IP/%s/g"'
                       ' %s/run.xml' % (FRONTEND_IP, FABAN_HOME))
  client.RemoteCommand('perl -pi -e '
                       '"s/MYSQL_DIR/%s/g" '
                       '%s/run.xml' % (re.escape(MYSQL_HOME), FABAN_HOME))
  client.RemoteCommand('perl -pi -e '
                       '"s/OUTPUT_DIR/%s/g" '
                       '%s/run.xml' % (re.escape(OUTPUT_DIR), FABAN_HOME))
  filestore = posixpath.join(frontend.GetScratchDir(), 'filestore')
  client.RemoteCommand('perl -pi -e '
                       '"s/FILESTORE_DIR/%s/g" '
                       '%s/run.xml' % (re.escape(filestore), FABAN_HOME))
  client.RemoteCommand('perl -pi -e '
                       '"s/BACKEND_IP/%s/g" '
                       '%s/run.xml' % (BACKEND_IP, FABAN_HOME))


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites():
  """Verifies that the required resources are present.
  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  if FLAGS['num_vms'].present and FLAGS.num_vms < 3:
    raise ValueError('Web Serving requires at least 3 VMs')


def PreparePrivateKey(vm):
  vm.AuthenticateVm()


def _PrepareVms(vm):
  vm.Install('wget')
  vm.Install('ant')
  vm.Install('openjdk7')
  vm.RemoteCommand('cd %s && '
                   'wget parsa.epfl.ch/cloudsuite/software/web.tar.gz && '
                   'tar xzf web.tar.gz' % vm_util.VM_TMP_DIR)


def Prepare(benchmark_spec):
  """Install Java, apache ant, authenticate vms
     Set up the client machine, backend machine, and frontend

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  frontend = benchmark_spec.vm_groups['frontend'][0]
  backend = benchmark_spec.vm_groups['backend'][0]
  client = benchmark_spec.vm_groups['client'][0]
  vms = benchmark_spec.vms
  vm_util.RunThreaded(PreparePrivateKey, vms)
  vm_util.RunThreaded(_PrepareVms, vms)
  frontend.RemoteCommand('cd %s && '
                         'wget parsa.epfl.ch/cloudsuite/software/perfkit/'
                         'web_serving/webservingfiles.tgz && '
                         'tar xzf webservingfiles.tgz' % BASE_DIR)
  _SetupClient(benchmark_spec)
  frontend.RemoteCommand('scp -r -o StrictHostKeyChecking=no %s:%s %s' %
                         (client.ip_address, FABAN_HOME, BASE_DIR))
  backend.RemoteCommand('scp -r -o StrictHostKeyChecking=no %s:%s %s' %
                        (client.ip_address, FABAN_HOME, BASE_DIR))
  setup_functions = [_SetupBackend, _SetupFrontend]
  vm_util.RunThreaded(lambda f: f(benchmark_spec), setup_functions)


def ParseOutput(client):
    stdout, _ = client.RemoteCommand('cd %s && cd $(ls -Art | tail -n 1) && '
                                     'cat summary.xml' % OUTPUT_DIR)
    ops_per_sec = re.findall(r'\<metric unit="ops/sec"\>(\d+\.?\d*)', stdout)
    sum_ops_per_sec = 0.0
    for value in ops_per_sec:
      sum_ops_per_sec += float(value)
    sum_ops_per_sec /= 2
    latency = re.findall(r'\<p90th\>(\d+\.\d*)', stdout)
    latency99 = re.findall(r'\<p99th\>(\d+\.\d*)', stdout)
    return sum_ops_per_sec, latency, latency99


def Run(benchmark_spec):
  client = benchmark_spec.vm_groups['client'][0]
  set_faban_home = ('export FABAN_HOME=%s && cd %s && ./run.sh')
  client.RobustRemoteCommand(set_faban_home % (FABAN_HOME, FABAN_HOME))
  results = []
  sum_ops_per_sec, latency, latency99 = ParseOutput(client)
  results.append(sample.Sample('Operations per second',
                 sum_ops_per_sec, 'ops/s'))
  results.append(sample.Sample('90th percentile latency HomePage ',
                 float(latency[0]), 'seconds'))
  results.append(sample.Sample('99th percentile latency HomePage',
                 float(latency99[0]), 'seconds'))
  return results


def Cleanup(benchmark_spec):
  """Cleanup function.

  Args:
    benchmark_spec: The benchmark specification. Contains all data
        that is required to run the benchmark.
  """
  backend = benchmark_spec.vm_groups['backend'][0]
  client = benchmark_spec.vm_groups['client'][0]
  set_java = ('export JAVA_HOME=%s && %s/master/bin/shutdown.sh')
  client.RemoteCommand(set_java % (JAVA_HOME, FABAN_HOME))
  backend.RemoteCommand('cd %s && sudo ./bin/mysqladmin shutdown' % MYSQL_HOME)
  _CleanupFrontend(benchmark_spec)


def _CleanupFrontend(benchmark_spec):
  frontend = benchmark_spec.vm_groups['frontend'][0]
  frontend.RemoteCommand('sudo killall -9 php-fpm')
  nginx.Stop(frontend)
  filestore = posixpath.join(frontend.GetScratchDir(), 'filestore')
  frontend.RemoteCommand('rm -R %s' % filestore)

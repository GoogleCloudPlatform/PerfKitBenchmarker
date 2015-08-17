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

import functools
import logging
import math
import os
import posixpath
import re
import time

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.packages import cassandra

"""Below are some constants used to set up the web serving benchmark. It is only necessary to modify BASE_DIR to be the directory you want it to be installed in (if this isn't set, it defaults to the home directory). We may also want to modify the options for the workload. 
"""
#options for the workload
LOAD_SCALE = '25'
DEFAULT_CLUSTER_SIZE = 2
#base directories/build directories
BASE_DIR = '/home/mendiola/web-release'
OLIO_BUILD = '%s/apache-olio-php-src-0.2/workload/php/trunk/' % BASE_DIR
FABAN_RUN= '%s/faban/master/bin/startup.sh' % BASE_DIR
FABAN_SHUTDOWN = '%s/faban/master/bin/shutdown.sh' % BASE_DIR
OLIO_WORKLOAD_LIB = '%s/apache-olio-php-src-0.2/workload/php/trunk/lib' % BASE_DIR
CATALINA_BUILD = '%s/apache-tomcat-6.0.35/bin' % BASE_DIR
#environment variables
CATALINA_HOME = '%s/apache-tomcat-6.0.35' % BASE_DIR
OLIO_HOME = '%s/apache-olio-php-src-0.2' % BASE_DIR
FABAN_HOME = '%s/faban' % BASE_DIR
JAVA_HOME='$(readlink -f $(which java) | cut -d "/" -f 1-5)'
MYSQL_HOME = '%s/mysql-5.5.20-linux2.6-x86_64'% BASE_DIR
GEOCODER_HOME = '%s' %BASE_DIR
#package names
TOMCAT = 'apache-tomcat-6.0.35.tar.gz'
FABAN = 'faban-kit-022311.tar.gz' 
OLIO = 'apache-olio-php-src-0.2.tar.gz'
MYSQL_CLIENT = 'mysql-connector-java-5.0.8.tar.gz'
MYSQL = 'mysql-5.5.20-linux2.6-x86_64.tar.gz'
COMMONS_DAEMON = 'commons-daemon-native.tar.gz'
#File names
MYSQL_CONNECTOR_JAR = '%s/mysql-connector-java-5.0.8/mysql-connector-java-5.0.8-bin.jar' % BASE_DIR
APACHE_HTTPD_JAR = '%s/samples/services/ApacheHttpdService/build/ApacheHttpdService.jar' % FABAN_HOME
MYSQL_SERVICE_JAR = '%s/samples/services/MysqlService/build/MySQLService.jar' % FABAN_HOME
MEMCACHED_SERVICE_JAR = '%s/samples/services/MemcachedService/build/MemcachedService.jar'% FABAN_HOME
MY_CNF = '/etc/my.cnf'

FLAGS= flags.FLAGS

BENCHMARK_INFO = {'name': 'webserving',
                  'description': 'Benchmark web2.0 applications with CloudStone',
                  'scratch_disk': False,
                  'num_machines': DEFAULT_CLUSTER_SIZE}


#perform necesary tasks/installation for the web fronted
#install nginx, PHP, faban (agent) 
def setupWebFronted(benchmark_spec):
  return

#install mysql, faban (agent), and tomcat
#configure the mysql database
def setupBackend(benchmark_spec):
  return

#setup faban driver
def setupClient(benchmark_spec):
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
  vms[0].RemoteCommand('sudo yum install ant')
  #vm.Install('ant')  TODO: Switch to this command - write an installation file for ant!
  vms[0].Install('wget')
  vms[0].Install('openjdk7')
  vms[0].RemoteCommand('wget parsa.epfl.ch/cloudsuite/software/web.tar.gz')
  vms[0].RemoteCommand('tar xzf web.tar.gz')
  vms[1].RemoteCommand('sudo yum install ant')
  vms[1].Install('wget')
  vms[1].Install('openjdk7')
  vms[1].RemoteCommand('wget parsa.epfl.ch/cloudsuite/software/web.tar.gz')
  vms[1].RemoteCommand('tar xzf web.tar.gz')
  setupClient(benchmark_spec)
  #TODO: scp from client to backend and frontend
  #setupBackend(benchmark_spec)
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
  vms=benchmark_spec.vms
  set_java = ('export JAVA_HOME=$(readlink -f $(which java) | cut -d "/" -f 1-5) && %s')
  #vms[0].RemoteCommand(set_java % (FABAN_RUN))  #run FABAN on the client
  time.sleep(150) #temporarily stall the faban_run to ensure that the benchmark can finish
  return

def Cleanup(benchmark_spec): 
  """Cleanup function.

  Args:
    benchmark_spec: The benchmark specification. Contains all data
        that is required to run the benchmark.
  """
  vms=benchmark_spec.vms
  #vms[0].RemoteCommand('sudo yum remove ant')
  #vms[0].RemoteCommand('~/web-release/faban/master/bin/shutdown.sh')
  vms[0].RemoteCommand('rm -fr web-release web.tar.gz')
  vms[1].RemoteCommand('rm -fr web-release web.tar.gz')
  return


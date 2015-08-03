# Copyright 2015 Google Inc. All rights reserved.
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

"""Runs CloudSuite Web Search benchmark.

Docs:
http://parsa.epfl.ch/cloudsuite/

Runs CloudSuite Web Search to collect the statistics that show
the operations completed per second and the minimum, maximum,
average, 90th, and 99th response times.
"""

import logging
import posixpath
import re
import time

from perfkitbenchmarker import vm_util
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample

FLAGS = flags.FLAGS

BENCHMARK_INFO = {'name': 'cloudsuite_web_search',
                  'description': 'Run CloudSuite Web Search',
                  'scratch_disk': True,
                  'num_machines': 3}

CLOUDSUITE_WEB_SEARCH_DIR = posixpath.join(vm_util.VM_TMP_DIR,
                                           'cloudsuite-web-search')
NUTCH_HOME_DIR = posixpath.join(CLOUDSUITE_WEB_SEARCH_DIR, 'apache-nutch-1.10')
FABAN_HOME_DIR = posixpath.join(CLOUDSUITE_WEB_SEARCH_DIR, 'faban')
FABAN_OUTPUT_DIR = posixpath.join(vm_util.VM_TMP_DIR, 'outputFaban')
SOLR_HOME_DIR = posixpath.join(CLOUDSUITE_WEB_SEARCH_DIR, 'solr-5.2.1')
# following variables should be changed to URLs and used with wget command
APACHE_NUTCH_TAR_URL = ('curl --header "Host: doc-14-b0-docs.googleusercontent'
                        '.com" --header "User-Agent: Mozilla/5.0 '
                        '(X11; Linux x86_64; rv:38.0) Gecko/20100101 '
                        'Firefox/38.0" --header "Accept: text/html,application'
                        '/xhtml+xml,application/xml;q=0.9,*/*;q=0.8" '
                        '--header "Accept-Language: en-US,en;q=0.5" '
                        '--header "Cookie: AUTH_npu93f5qtsn6732s1bkk8mg68ja3'
                        'runk=06154759484088948450|1438588800000|nkf9c7rg3rm'
                        '9b71ch7f49n78peje37j4" --header "Connection: keep-'
                        'alive" "https://doc-14-b0-docs.googleusercontent.c'
                        'om/docs/securesc/ff1mikmg3qur6cbaj9vilst0ftkfegee/'
                        't1thdu0ejdakbokb2b2kn0v0i3rljght/1438588800000/061'
                        '54759484088948450/06154759484088948450/0BzrWXFS43e'
                        'WAOG0wZFo1NUFrZlE?e=download" -o '
                        '"apache-nutch-src.tar.gz" -L')
NUTCH_SITE_URL = ('curl --header "Host: doc-0s-b0-docs.googleusercontent.com" '
                  '--header "User-Agent: Mozilla/5.0 (X11; Linux x86_64; '
                  'rv:38.0) Gecko/20100101 Firefox/38.0" --header "Accept: '
                  'text/html,application/xhtml+xml,application/xml;q=0.9,'
                  '*/*;q=0.8" --header "Accept-Language: en-US,en;q=0.5" '
                  '--header "Cookie: AUTH_npu93f5qtsn6732s1bkk8mg68ja3runk'
                  '=06154759484088948450|1438588800000|nkf9c7rg3rm9b71ch7f'
                  '49n78peje37j4" --header "Connection: keep-alive" "https:'
                  '//doc-0s-b0-docs.googleusercontent.com/docs/securesc/ff1'
                  'mikmg3qur6cbaj9vilst0ftkfegee/mskoan3n4o83tb609o2f391c64'
                  'qts8r4/1438588800000/06154759484088948450/06154759484088'
                  '948450/0BzrWXFS43eWAQzNBeUpNLWNtSW8?e=download" -o '
                  '"nutch-site.xml" -L')
CRAWLED_URL = ('curl --header "Host: doc-0s-b0-docs.googleusercontent.com" --h'
               'eader "User-Agent: Mozilla/5.0 (X11; Linux x86_64; rv:38.0) Ge'
               'cko/20100101 Firefox/38.0" --header "Accept: text/html,applica'
               'tion/xhtml+xml,application/xml;q=0.9,*/*;q=0.8" --header "Acce'
               'pt-Language: en-US,en;q=0.5" --header "Referer: https://drive.'
               'google.com/uc?export=download&id=0BzrWXFS43eWARGVmN3dnREdXcDQ"'
               ' --header "Cookie: AUTH_npu93f5qtsn6732s1bkk8mg68ja3runk=06154'
               '759484088948450|1438588800000|nkf9c7rg3rm9b71ch7f49n78peje37j4'
               '" --header "Connection: keep-alive" "https://doc-0s-b0-docs.go'
               'ogleusercontent.com/docs/securesc/ff1mikmg3qur6cbaj9vilst0ftkf'
               'egee/b912v70hbvmdc5bhotdbn1ctkplq9dq6/1438588800000/0615475948'
               '4088948450/06154759484088948450/0BzrWXFS43eWARGVmN3dnREdXcDQ?e'
               '=download" -o "crawl.tar.gz" -L')
SOLR_TAR_URL = ('curl --header "Host: doc-0c-b0-docs.googleusercontent.com" '
                '--header "User-Agent: Mozilla/5.0 (X11; Linux x86_64; rv:'
                '38.0) Gecko/20100101 Firefox/38.0" --header "Accept: text'
                '/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"'
                ' --header "Accept-Language: en-US,en;q=0.5" --header "Referer'
                ': https://drive.google.com/uc?export=download&id=0BzrWXFS43eW'
                'AWVRtQXJlVFNRbk0" --header "Cookie: AUTH_npu93f5qtsn6732s1bkk'
                '8mg68ja3runk=06154759484088948450|1438588800000|nkf9c7rg3rm9b'
                '71ch7f49n78peje37j4" --header "Connection: keep-alive" '
                '"https://doc-0c-b0-docs.googleusercontent.com/docs/securesc/'
                'ff1mikmg3qur6cbaj9vilst0ftkfegee/6rkr78ibf3et7qeonu3alqb9l3n'
                'cfq2o/1438588800000/06154759484088948450/0615475948408894845'
                '0/0BzrWXFS43eWAWVRtQXJlVFNRbk0?e=download" -o '
                '"solr.tar.gz" -L')
SCHEMA_URL = ('curl --header "Host: doc-0s-b0-docs.googleusercontent.com" '
              '--header "User-Agent: Mozilla/5.0 (X11; Linux x86_64; rv:38.0) '
              'Gecko/20100101 Firefox/38.0" --header "Accept: text/html,'
              'application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8" --header'
              ' "Accept-Language: en-US,en;q=0.5" --header "Cookie: AUTH_npu93'
              'f5qtsn6732s1bkk8mg68ja3runk=06154759484088948450|1438588800000|'
              'nkf9c7rg3rm9b71ch7f49n78peje37j4" --header "Connection: keep-al'
              'ive" "https://doc-0s-b0-docs.googleusercontent.com/docs/secures'
              'c/ff1mikmg3qur6cbaj9vilst0ftkfegee/tngplt6r7km5rovnkjsvbse9ae7d'
              'hlc6/1438588800000/06154759484088948450/06154759484088948450/0B'
              'zrWXFS43eWATzJNRk9sZG5ydkU?e=download" -o "schema.xml" -L')
FABAN_TAR_URL = ('curl --header "Host: doc-00-b0-docs.googleusercontent.com" '
                 '--header "User-Agent: Mozilla/5.0 (X11; Linux x86_64; rv:38.'
                 '0) Gecko/20100101 Firefox/38.0" --header "Accept: text/html,'
                 'application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8" --hea'
                 'der "Accept-Language: en-US,en;q=0.5" --header "Referer: htt'
                 'ps://drive.google.com/uc?export=download&id=0BzrWXFS43eWAWXJ'
                 'pb1MtWGtfM0E" --header "Cookie: AUTH_npu93f5qtsn6732s1bkk8mg'
                 '68ja3runk=06154759484088948450|1438588800000|nkf9c7rg3rm9b71'
                 'ch7f49n78peje37j4" --header "Connection: keep-alive" "https:'
                 '//doc-00-b0-docs.googleusercontent.com/docs/securesc/ff1mikm'
                 'g3qur6cbaj9vilst0ftkfegee/ldemqgpcvi61bdo5tegk9muir3rjfdcm/1'
                 '438588800000/06154759484088948450/06154759484088948450/0BzrW'
                 'XFS43eWAWXJpb1MtWGtfM0E?e=download" -o '
                 '"faban-kit-latest.tar.gz" -L')
SEARCH_DRIVER_URL = ('curl --header "Host: doc-0s-b0-docs.googleusercontent.co'
                     'm" --header "User-Agent: Mozilla/5.0 (X11; Linux x86_64;'
                     ' rv:38.0) Gecko/20100101 Firefox/38.0" --header "Accept:'
                     ' text/html,application/xhtml+xml,application/xml;q=0.9,*'
                     '/*;q=0.8" --header "Accept-Language: en-US,en;q=0.5" --h'
                     'eader "Cookie: AUTH_npu93f5qtsn6732s1bkk8mg68ja3runk=061'
                     '54759484088948450|1438588800000|nkf9c7rg3rm9b71ch7f49n78'
                     'peje37j4" --header "Connection: keep-alive" "https://doc'
                     '-0s-b0-docs.googleusercontent.com/docs/securesc/ff1mikmg'
                     '3qur6cbaj9vilst0ftkfegee/ple89p23kk31ca9q24mc8q4aoemlqil'
                     '1/1438588800000/06154759484088948450/0615475948408894845'
                     '0/0BzrWXFS43eWATm1SdnpoLTJSdUE?e=download" -o "search.ta'
                     'r.gz" -L')

java_home = ''


def GetInfo():
  return BENCHMARK_INFO


def Prepare(benchmark_spec):
  """Install cloudsuite web search and start the server on all machines.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  global java_home
  vms = benchmark_spec.vms
  fw = benchmark_spec.firewall
  vms[0].Install('ant')
  for vm in vms:
    fw.AllowPort(vm, 8983)
    fw.AllowPort(vm, 9983)
    fw.AllowPort(vm, 9980)
    vm.Install('openjdk7')
    vm.Install('lsof')
    vm.Install('curl')
    vm.Install('wget')
    vm.RobustRemoteCommand('mkdir -p {0} &&'
                           'cd {0} && '
                           '{2} && '
                           'tar -C {0} -zxf solr.tar.gz && '
                           '{3} && '
                           'cp schema.xml {1}/server/solr/configsets/'
                           'basic_configs/conf/'.format(
                               CLOUDSUITE_WEB_SEARCH_DIR,
                               SOLR_HOME_DIR, SOLR_TAR_URL,
                               SCHEMA_URL))
  vms[1].RobustRemoteCommand('cd {0} && '
                             'export PATH=$PATH:/usr/sbin && '
                             'bin/solr start -cloud && '
                             'echo $?'.format(
                                 SOLR_HOME_DIR))
  java_home, _ = vms[0].RemoteCommand('readlink -f $(which java) | '
                                      'cut -d "/" -f 1-5')
  java_home = java_home.rstrip()
  time.sleep(15)
  vms[2].RobustRemoteCommand('cd {0} && '
                             'bin/solr start -cloud -p 8983 -z {1}:9983'.format(
                                 SOLR_HOME_DIR, vms[1].ip_address))
  time.sleep(15)
  vms[1].RobustRemoteCommand('cd {0} && '
                             'bin/solr create_collection -c cloudsuite_web_'
                             'search -d basic_configs -shards 2'.format(
                                 SOLR_HOME_DIR))
  time.sleep(20)
  vms[0].RobustRemoteCommand('cd {3} && '
                             'export JAVA_HOME={6} && '
                             '{4} && '
                             'tar -C {3} -xzf apache-nutch-src.tar.gz && '
                             '{1} && '
                             'cp nutch-site.xml {0}/conf/ && '
                             'cd {0} && '
                             'sed -i "/<value>http/c\\<value>http://{5}:'
                             '8983/solr/cloudsuite_web_search</value>" '
                             'conf/nutch-site.xml && '
                             'ant && '
                             'cd runtime/local && '
                             '{2}  && '
                             'tar -zxf crawl.tar.gz && '
                             'bin/nutch index crawl/crawldb/ -linkdb '
                             'crawl/linkdb/ -dir crawl/segments/'.format(
                                 NUTCH_HOME_DIR, NUTCH_SITE_URL,
                                 CRAWLED_URL, CLOUDSUITE_WEB_SEARCH_DIR,
                                 APACHE_NUTCH_TAR_URL, vms[1].ip_address,
                                 java_home))
  vms[0].RemoteCommand('cd {0} && '
                       'export JAVA_HOME={7} && '
                       '{1} && '
                       'tar -C {0} -xzf faban-kit-latest.tar.gz && '
                       '{3} && '
                       'tar -C {2} -xzf search.tar.gz && '
                       'cd {2} && '
                       'master/bin/startup.sh && '
                       'sed -i "/faban.home/c\\faban.home={2}" '
                       'search/build.properties && '
                       'sed -i "/ant.home/c\\ant.home='
                       '/usr/share/ant" search/build.properties && '
                       'sed -i "/faban.url/c\\faban.url='
                       'http://localhost:9980/" search/build.properties && '
                       'cd search && '
                       'ant deploy && '
                       'sed -i "/<ipAddress1>/c\<ipAddress1>{4}'
                       '</ipAddress1>" deploy/run.xml && '
                       'sed -i "/<ipAddress2>/c\<ipAddress2>{5}'
                       '</ipAddress2>" deploy/run.xml && '
                       'sed -i "/<logFile>/c\<logFile>{2}/logs/queries.out'
                       '</logFile>" deploy/run.xml && '
                       'sed -i "/<outputDir>/c\<outputDir>{6}'
                       '</outputDir>" deploy/run.xml && '
                       'sed -i "/<termsFile>/c\<termsFile>{2}'
                       '/search/src/sample/searchdriver/terms_en.out'
                       '</termsFile>" deploy/run.xml && '
                       'sed -i "/<fa:rampUp>/c\<fa:rampUp>10'
                       '</fa:rampUp>" deploy/run.xml && '
                       'sed -i "/<fa:rampDown>/c\<fa:rampDown>10'
                       '</fa:rampDown>" deploy/run.xml && '
                       'sed -i "/<fa:steadyState>/c\<fa:steadyState>60'
                       '</fa:steadyState>" deploy/run.xml'.format(
                           CLOUDSUITE_WEB_SEARCH_DIR, FABAN_TAR_URL,
                           FABAN_HOME_DIR, SEARCH_DRIVER_URL,
                           vms[1].ip_address, vms[2].ip_address,
                           FABAN_OUTPUT_DIR, java_home))
  time.sleep(20)


def Run(benchmark_spec):
  """Run cloudsuite web search on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vms = benchmark_spec.vms
  vms[0].RemoteCommand('cd {0} && '
                       'export FABAN_HOME={0} && '
                       'cd search && '
                       'sh run.sh'.format(
                           FABAN_HOME_DIR, FABAN_OUTPUT_DIR))
  stdout, _ = vms[0].RemoteCommand('cat {0}/*/summary.xml'.format(
                                   FABAN_OUTPUT_DIR))
  results = []
  p90 = re.findall(r'\<p90th\>(\d+\.?\d*)', stdout)
  sum_p90 = 0.0
  for value in p90:
      sum_p90 += float(value)
  sum_p90 *= 100
  results.append(sample.Sample('p90th', sum_p90, '%'))

  logging.info('CloudSuite Web Search Results:')

  return results


def Cleanup(benchmark_spec):
  """Cleanup CloudSuite Web Search on the target vm (by uninstalling).

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vms[0].RemoteCommand('cd {0} && '
                       'export JAVA_HOME={1} && '
                       'master/bin/shutdown.sh'.format(
                           FABAN_HOME_DIR, java_home))
  vms[1].RemoteCommand('cd {0} && '
                       'bin/solr stop -p 8983'.format(
                           SOLR_HOME_DIR))
  vms[2].RemoteCommand('cd {0} && '
                       'bin/solr stop -p 8983'.format(
                           SOLR_HOME_DIR))

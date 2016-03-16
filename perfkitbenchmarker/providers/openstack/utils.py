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


import logging
import os

from perfkitbenchmarker import flags

FLAGS = flags.FLAGS


class NovaClient(object):

    def __getattribute__(self, item):
        try:
            return super(NovaClient, self).__getattribute__(item)
        except AttributeError:
            return self.__client.__getattribute__(item)

    def GetPassword(self):
      # For compatibility with Nova CLI, use 'OS'-prefixed environment value
      # if present. Also support reading the password from a file.

      error_msg = ('No OpenStack password specified. '
                   'Either set the environment variable OS_PASSWORD to the '
                   'admin password, or provide the name of a file '
                   'containing the password using the OPENSTACK_PASSWORD_FILE '
                   'environment variable or --openstack_password_file flag.')

      password = os.getenv('OS_PASSWORD')
      if password is not None:
        return password
      try:
        with open(os.path.expanduser(FLAGS.openstack_password_file)) as pwfile:
          password = pwfile.readline().rstrip()
          return password
      except IOError as e:
        raise Exception(error_msg + ' ' + str(e))
      raise Exception(error_msg)

    def __init__(self):
        from keystoneclient import session as ksc_session
        from keystoneclient.auth.identity import v2
        from novaclient import client as nova

        self.url = FLAGS.openstack_auth_url
        self.user = FLAGS.openstack_username
        self.tenant = FLAGS.openstack_tenant
        self.endpoint_type = FLAGS.openstack_nova_endpoint_type
        self.http_log_debug = FLAGS.log_level == 'debug'
        self.password = self.GetPassword()

        self.__auth = v2.Password(auth_url=self.url,
                                  username=self.user,
                                  password=self.password,
                                  tenant_name=self.tenant)
        self._session = ksc_session.Session(auth=self.__auth)
        self.__client = nova.Client(version='2', session=self._session,
                                    http_log_debug=self.http_log_debug)
        # Set requests library logging level to WARNING
        # so it doesn't spam logs with unhelpful messages,
        # such as 'Starting new HTTP Connection'.
        rq_logger = logging.getLogger('requests')
        rq_logger.setLevel(logging.WARNING)

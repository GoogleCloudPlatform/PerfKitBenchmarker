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

import functools
import os

from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.vm_util import POLL_INTERVAL


FLAGS = flags.FLAGS

flags.DEFINE_string('openstack_auth_url',
                    os.environ.get('OS_AUTH_URL', 'http://localhost:5000'),
                    ('Url for Keystone authentication service, defaults to '
                     '$OS_AUTH_URL. Required for discovery of other OpenStack '
                     'service URLs.'))

flags.DEFINE_string('openstack_username',
                    os.getenv('OS_USERNAME', 'admin'),
                    'OpenStack login username, defaults to $OS_USERNAME.')

flags.DEFINE_string('openstack_tenant',
                    os.getenv('OS_TENANT_NAME', 'admin'),
                    'OpenStack tenant name, defaults to $OS_TENANT_NAME.')

flags.DEFINE_string('openstack_password_file',
                    os.getenv('OPENSTACK_PASSWORD_FILE',
                              '~/.config/openstack-password.txt'),
                    'Path to file containing the openstack password, '
                    'defaults to $OPENSTACK_PASSWORD_FILE. Alternatively, '
                    'setting the password itself in $OS_PASSWORD is also '
                    'supported.')

flags.DEFINE_string('openstack_nova_endpoint_type',
                    os.getenv('NOVA_ENDPOINT_TYPE', 'publicURL'),
                    'OpenStack Nova endpoint type, '
                    'defaults to $NOVA_ENDPOINT_TYPE.')


class KeystoneAuth(object):
    """
        Usage example:
        auth = KeystoneAuth(auth_url, auth_tenant, auth_user, auth_password)
        token = auth.get_token()
        tenant_id = auth.get_tenant_id()

        token and tenant_id are required to use all OpenStack python clients
    """

    def __init__(self, url, tenant, user, password):
        self.__url = url
        self.__tenant = tenant
        self.__user = user
        self.__password = password
        self.__connection = None
        self.__session = None

    def GetConnection(self):
        if self.__connection is None:
            self.__authenticate()
        return self.__connection

    def __authenticate(self):
        import keystoneclient.v2_0.client as ksclient

        self.__connection = ksclient.Client(
            auth_url=self.__url,
            username=self.__user,
            password=self.__password,
            tenant=self.__tenant)
        self.__connection.authenticate()

    def get_token(self):
        return self.GetConnection().get_token(self.__session)

    def get_tenant_id(self):
        raw_token = self.GetConnection().get_raw_token_from_identity_service(
            auth_url=self.__url,
            username=self.__user,
            password=self.__password,
            tenant_name=self.__tenant
        )
        return raw_token['token']['tenant']['id']


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
        from novaclient import client as noclient

        self.url = FLAGS.openstack_auth_url
        self.user = FLAGS.openstack_username
        self.tenant = FLAGS.openstack_tenant
        self.endpoint_type = FLAGS.openstack_nova_endpoint_type
        self.password = self.GetPassword()
        self.__auth = KeystoneAuth(self.url, self.tenant,
                                   self.user, self.password)
        self.__client = noclient.Client('2',
                                        auth_url=self.url,
                                        username=self.user,
                                        auth_token=self.__auth.get_token(),
                                        tenant_id=self.__auth.get_tenant_id(),
                                        endpoint_type=self.endpoint_type,
                                        )

    def reconnect(self):
        from novaclient import client as noclient

        self.__auth = KeystoneAuth(self.url, self.tenant, self.user,
                                   self.password)
        self.__client = noclient.Client('2',
                                        auth_url=self.url,
                                        username=self.user,
                                        auth_token=self.__auth.get_token(),
                                        tenant_id=self.__auth.get_tenant_id(),
                                        endpoint_type=self.endpoint_type,
                                        )


class AuthException(Exception):
    """Wrapper for NovaClient auth exceptions."""
    pass


def retry_authorization(max_retries=1, poll_interval=POLL_INTERVAL):
    def decored(function):
        @vm_util.Retry(max_retries=max_retries,
                       poll_interval=poll_interval,
                       retryable_exceptions=AuthException,
                       log_errors=False)
        @functools.wraps(function)
        def decor(*args, **kwargs):
            from novaclient.exceptions import Unauthorized
            try:
                return function(*args, **kwargs)
            except Unauthorized as e:
                NovaClient.instance.reconnect()
                raise AuthException(str(e))

        return decor

    return decored

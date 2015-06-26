import functools

import keystoneclient.v2_0.client as ksclient
from novaclient import client as noclient
from novaclient.exceptions import Unauthorized

from perfkitbenchmarker import vm_util
from perfkitbenchmarker.vm_util import POLL_INTERVAL


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

    def __getattribute__(self, item):
        if item == '_KeystoneAuth__connection':
            if self.__dict__['_KeystoneAuth__connection'] is None:
                self.__authenticate()

        return super(KeystoneAuth, self).__getattribute__(item)

    def __authenticate(self):
        self.__connection = ksclient.Client(
            auth_url=self.__url,
            username=self.__user,
            password=self.__password,
            tenant=self.__tenant)
        self.__connection.authenticate()

    def get_token(self):
        return self.__connection.get_token(self.__session)

    def get_tenant_id(self):
        raw_token = self.__connection.get_raw_token_from_identity_service(
            auth_url=self.__url,
            username=self.__user,
            password=self.__password,
            tenant_name=self.__tenant
        )
        return raw_token['token']['tenant']['id']


class NovaClient(object):
    instance = None

    def __new__(cls, *args):
        if not NovaClient.instance:
            cls.instance = object.__new__(cls, *args)
        return cls.instance

    def __getattribute__(self, item):
        try:
            return super(NovaClient, self).__getattribute__(item)
        except AttributeError:
            return self.__client.__getattribute__(item)

    def __init__(self, url, tenant, user, password):
        self.url = url
        self.user = user
        self.tenant = tenant
        self.password = password
        self.__auth = KeystoneAuth(url, tenant, user, password)
        self.__client = noclient.Client('1.1',
                                        auth_url=url,
                                        username=user,
                                        auth_token=self.__auth.get_token(),
                                        tenant_id=self.__auth.get_tenant_id(),
                                        )

    def reconnect(self):
        self.__auth = KeystoneAuth(self.url, self.tenant, self.user,
                                   self.password)
        self.__client = noclient.Client('1.1',
                                        auth_url=self.url,
                                        username=self.user,
                                        auth_token=self.__auth.get_token(),
                                        tenant_id=self.__auth.get_tenant_id(),
                                        )


def retry_authorization(max_retries=1, poll_interval=POLL_INTERVAL):
    def decored(function):
        @vm_util.Retry(max_retries=max_retries,
                       poll_interval=poll_interval,
                       retryable_exceptions=Unauthorized,
                       log_errors=False)
        @functools.wraps(function)
        def decor(*args, **kwargs):
            try:
                return function(*args, **kwargs)
            except Unauthorized:
                NovaClient.instance.reconnect()
                raise

        return decor

    return decored

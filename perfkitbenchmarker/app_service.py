"""Module containing class for BaseAppService and BaseAppServiceSpec."""

import logging
import threading
import time
from typing import Any, Dict, List, Optional, Type, TypeVar

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import resource
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec
from perfkitbenchmarker.linux_packages import http_poller


FLAGS = flags.FLAGS
flags.DEFINE_string('appservice', None, 'Type of app service. e.g. AppEngine')
flags.DEFINE_string(
    'appservice_region', None, 'Region of deployed app service.'
)
flags.DEFINE_string(
    'appservice_backend', None, 'Backend instance type of app service uses.'
)
flags.DEFINE_string(
    'app_runtime',
    None,
    'Runtime environment of app service uses. e.g. python, java',
)
flags.DEFINE_string(
    'app_type', None, 'Type of app packages builders should built.'
)
flags.DEFINE_integer('appservice_count', 1, 'Copies of applications to launch.')
APPSERVICE_NAME = flags.DEFINE_string(
    'appservice_name',
    '',
    'Hardcode the name of the serverless app. If set, overrides an otherwise'
    ' run_uri based name, usually especially for cold starts.',
)


def GetAppServiceSpecClass(service) -> Type['BaseAppServiceSpec']:
  """Returns class of the given AppServiceSpec.

  Args:
    service: String which matches an inherited BaseAppServiceSpec's required
      SERVICE value.
  """
  return spec.GetSpecClass(BaseAppServiceSpec, SERVICE=service)


class BaseAppServiceSpec(spec.BaseSpec):
  """Spec storing various data about app service.

  That data is parsed from command lines or yaml benchmark_spec configs, using
  bunchmark_config_spec._AppServiceDecoder.

  Attributes:
    SPEC_TYPE: The class / spec name.
    SPEC_ATTRS: Required field(s) for subclasses.
    appservice_region: The region to run in.
    appservice_backend: Amount of memory to use.
    appservice: Name of the service (e.g. googlecloudfunction).
  """

  SPEC_TYPE: str = 'BaseAppServiceSpec'
  SPEC_ATTRS: List[str] = ['SERVICE']
  appservice_region: str
  appservice_backend: str
  appservice: str

  @classmethod
  def _ApplyFlags(
      cls,
      config_values: dict[str, Any],
      flag_values: flags.FlagValues,
  ):
    super()._ApplyFlags(config_values, flag_values)
    if flag_values['appservice_region'].present:
      config_values['appservice_region'] = flag_values.appservice_region
    if flag_values['appservice_backend'].present:
      config_values['appservice_backend'] = flag_values.appservice_backend
    if flag_values['appservice'].present:
      config_values['appservice'] = flag_values.appservice

  @classmethod
  def _GetOptionDecoderConstructions(cls) -> Dict[str, Any]:
    result = super(BaseAppServiceSpec, cls)._GetOptionDecoderConstructions()
    result.update({
        'appservice_region': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'appservice_backend': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'appservice': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
    })
    return result


AppServiceChild = TypeVar('AppServiceChild', bound='BaseAppService')


def GetAppServiceClass(service: str) -> Optional[Type[AppServiceChild]]:
  """Returns class of the given AppService.

  Args:
    service: String which matches an inherited BaseAppService's required SERVICE
      value.
  """
  return resource.GetResourceClass(BaseAppService, SERVICE=service)


class BaseAppService(resource.BaseResource):
  """Base class for representing an App service / instance.

  Handles lifecycle management (creation & deletion) for a given App service.

  Attributes:
    RESOURCE_TYPE: The class name.
    REQUIRED_ATTRS: Required field for subclass to override (one constant per
      subclass).
    region: Region the service should be in.
    backend: Amount of memory allocated.
    builder: A BaseAppBuilder matching the specified runtime & app type.
    samples: Samples with lifecycle metrics (e.g. create time) recorded.
    endpoint: An HTTP(s) endpoint the service can be accessed at.
    poller: An HttpPoller for querying the endpoint.
    vm: The vm which can be used to call the app service.
  """

  RESOURCE_TYPE: str = 'BaseAppService'
  REQUIRED_ATTRS: List[str] = ['SERVICE']
  SERVICE: str
  _POLL_INTERVAL: int = 1

  _appservice_counter: int = 0
  _appservice_counter_lock = threading.Lock()

  def __init__(self, base_app_service_spec: BaseAppServiceSpec):
    super().__init__()
    self.name: str = self._GenerateName()
    self.region: str = base_app_service_spec.appservice_region
    self.backend: str = base_app_service_spec.appservice_backend
    self.builder: Any = None
    self.endpoint: Optional[str] = None
    self.poller: http_poller.HttpPoller = http_poller.HttpPoller()
    self.vm: virtual_machine.BaseVirtualMachine
    # update metadata
    self.metadata.update({
        'backend': self.backend,
        'region': self.region,
        'concurrency': 'default',
    })
    self.samples: List[sample.Sample] = []

  def _GenerateName(self) -> str:
    """Generates a unique name for the AppService.

    Locking the counter variable allows for each created app service name to be
    unique within the python process.

    Returns:
      The newly generated name.
    """
    with self._appservice_counter_lock:
      self.appservice_number: int = self._appservice_counter
      if APPSERVICE_NAME.value:
        name = APPSERVICE_NAME.value
      else:
        name: str = f'pkb-{FLAGS.run_uri}'
      name += f'-{self.appservice_number}'
      BaseAppService._appservice_counter += 1
      return name

  def _UpdateDependencies(self):
    """Update dependencies for AppService."""
    self.builder.Mutate()

  def _Update(self):
    raise NotImplementedError()

  def Update(self):
    """Updates a deployed app instance."""

    @vm_util.Retry(
        poll_interval=self._POLL_INTERVAL,
        fuzz=0,
        timeout=self.READY_TIMEOUT,
        retryable_exceptions=(errors.Resource.RetryableCreationError,),
    )
    def WaitUntilReady():
      if not self._IsReady():
        raise errors.Resource.RetryableCreationError('Not yet ready')

    if self.user_managed:
      return
    self._UpdateDependencies()
    logging.info('Starting the update timer')
    self.update_start_time = time.time()
    self._Update()
    self.update_end_time = time.time()
    logging.info(
        'Ending the update timer with %.5fs elapsed. Ready still going.',
        self.update_end_time - self.update_start_time,
    )
    WaitUntilReady()
    self.update_ready_time = time.time()
    self.samples.append(
        sample.Sample(
            'update latency',
            self.update_end_time - self.update_start_time,
            'seconds',
            {},
        )
    )
    self.samples.append(
        sample.Sample(
            'update ready latency',
            self.update_ready_time - self.update_start_time,
            'seconds',
            {},
        )
    )

  def Invoke(self, args: Any = None) -> http_poller.PollingResponse:
    """Invokes a deployed app instance.

    Args:
      args: dict. Arguments passed to app.

    Returns:
      An object containing success, response string, & latency. Latency is
      also negative in the case of a failure.
    """
    return self.poller.Run(
        self.vm,
        self.endpoint,
        expected_response=self.builder.GetExpectedResponse(),
    )

  def _IsReady(self) -> bool:
    """Returns true if the underlying resource is ready.

    Supplying this method is optional.  Use it when a resource can exist
    without being ready.  If the subclass does not implement
    it then it just returns true.

    Returns:
      True if the resource was ready in time, False if the wait timed out.
    """
    response = self.Invoke()
    logging.info(
        'Polling Endpoint, success: %s, latency: %s',
        response.success,
        response.latency,
    )
    return response.latency > 0

  def _CreateDependencies(self):
    """Creates dependencies needed before _CreateResource() is called."""
    if self.builder:
      self.builder.Create()
    self.vm.Install('http_poller')

  def _DeleteDependencies(self):
    """Deletes app package."""
    if self.builder:
      self.builder.Delete()

  def SetBuilder(self, builder: Any = None, **kwargs: Any):
    """Sets builder for AppService."""
    if builder:
      self.builder = builder
    self.vm = self.builder.vm

  def GetLifeCycleMetrics(self):
    """Exports internal lifecycle metrics."""
    if self.builder:
      self.metadata.update(self.builder.GetResourceMetadata())

    for s in self.samples:
      s.metadata.update(self.metadata)
    return self.samples

  def _PostCreate(self):
    """Method called after _CreateResource."""
    if self.builder:
      self.metadata.update(self.builder.GetResourceMetadata())

  def Create(self, restore=False):
    super().Create(restore)
    self.samples.append(
        sample.Sample(
            'create latency',
            self.create_end_time - self.create_start_time,
            'seconds',
            {},
        )
    )
    self.samples.append(
        sample.Sample(
            'create ready latency',
            self.resource_ready_time - self.create_start_time,
            'seconds',
            {},
        )
    )

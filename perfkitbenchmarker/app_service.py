"""Module containing class for BaseAppService and BaseAppServiceSpec."""
import threading
import time

from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec

FLAGS = flags.FLAGS
flags.DEFINE_string('appservice', None,
                    'Type of app service. e.g. AppEngine')
flags.DEFINE_string('appservice_region', None,
                    'Region of deployed app service.')
flags.DEFINE_string('appservice_backend', None,
                    'Backend instance type of app service uses.')
flags.DEFINE_string('app_runtime', None,
                    'Runtime environment of app service uses. '
                    'e.g. python, java')
flags.DEFINE_string('app_type', None,
                    'Type of app packages builders should built.')
flags.DEFINE_integer('appservice_count', 1,
                     'Copies of applications to launch.')


def GetAppServiceSpecClass(service):
  return spec.GetSpecClass(
      BaseAppServiceSpec, SERVICE=service)


class BaseAppServiceSpec(spec.BaseSpec):
  """Storing various data about app service."""

  SPEC_TYPE = 'BaseAppServiceSpec'
  SPEC_ATTRS = ['SERVICE']

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    super(BaseAppServiceSpec, cls)._ApplyFlags(config_values, flag_values)
    if flag_values['appservice_region'] .present:
      config_values['appservice_region'] = flag_values.appservice_region
    if flag_values['appservice_backend'].present:
      config_values['appservice_backend'] = flag_values.appservice_backend
    if flag_values['appservice'].present:
      config_values['appservice'] = flag_values.appservice

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    result = super(BaseAppServiceSpec, cls)._GetOptionDecoderConstructions()
    result.update({
        'appservice_region': (option_decoders.StringDecoder, {
            'default': None, 'none_ok': True}),
        'appservice_backend': (option_decoders.StringDecoder, {
            'default': None, 'none_ok': True}),
        'appservice': (option_decoders.StringDecoder, {
            'default': None, 'none_ok': True})
    })
    return result


def GetAppServiceClass(service):
  return resource.GetResourceClass(
      BaseAppService, SERVICE=service)


class BaseAppService(resource.BaseResource):
  """Base class for representing an App instance."""

  RESOURCE_TYPE = 'BaseAppService'
  REQUIRED_ATTRS = ['SERVICE']
  POLL_INTERVAL = 1

  _appservice_counter = 0
  _appservice_counter_lock = threading.Lock()

  def __init__(self, base_app_service_spec):
    super(BaseAppService, self).__init__()
    with self._appservice_counter_lock:
      self.appservice_number = self._appservice_counter
      self.name = 'pkb-%s-%s' % (FLAGS.run_uri, self.appservice_number)
      BaseAppService._appservice_counter += 1
    self.region = base_app_service_spec.appservice_region
    self.backend = base_app_service_spec.appservice_backend
    self.builder = None
    # update metadata
    self.metadata.update({'backend': self.backend,
                          'region': self.region,
                          'concurrency': 'default'})
    self.samples = []

  def _UpdateDependencies(self):
    """Update dependencies for AppService."""
    self.builder.Mutate()

  def _Update(self):
    raise NotImplementedError()

  def Update(self):
    """Update a deployed app instance."""

    @vm_util.Retry(poll_interval=self.POLL_INTERVAL, fuzz=0,
                   timeout=self.READY_TIMEOUT,
                   retryable_exceptions=(
                       errors.Resource.RetryableCreationError,))
    def WaitUntilReady():
      if not self._IsReady():
        raise errors.Resource.RetryableCreationError('Not yet ready')

    if self.user_managed:
      return
    self._UpdateDependencies()
    self.update_start_time = time.time()
    self._Update()
    self.update_end_time = time.time()
    WaitUntilReady()
    self.update_ready_time = time.time()
    self.samples.append(
        sample.Sample('update latency',
                      self.update_end_time - self.update_start_time,
                      'seconds', {}))
    self.samples.append(
        sample.Sample('update ready latency',
                      self.update_ready_time - self.update_start_time,
                      'seconds', {}))

  def Invoke(self, args=None):
    """Invoke a deployed app instance.

    Args:
      args: dict. Arguments passed to app.
    """
    raise NotImplementedError()

  def _CreateDependencies(self):
    """Builds app package."""
    if self.builder:
      self.builder.Create()

  def _DeleteDependencies(self):
    """Delete app package."""
    if self.builder:
      self.builder.Delete()

  def SetBuilder(self, builder=None, **kwargs):
    """Set builder for AppService."""
    if builder:
      self.builder = builder

  def GetLifeCycleMetrics(self):
    """Export internal lifecycle metrics."""
    for s in self.samples:
      s.metadata.update(self.metadata)
    return self.samples

  def _PostCreate(self):
    """Method called after _CreateResource."""
    if self.builder:
      self.metadata.update(self.builder.GetResourceMetadata())

  def Create(self):
    super(BaseAppService, self).Create()
    self.samples.append(
        sample.Sample('create latency',
                      self.create_end_time - self.create_start_time,
                      'seconds', {}))
    self.samples.append(
        sample.Sample('create ready latency',
                      self.resource_ready_time - self.create_start_time,
                      'seconds', {}))

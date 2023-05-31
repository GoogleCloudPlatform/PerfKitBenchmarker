"""Provider info for OCI."""

from perfkitbenchmarker import provider_info
from perfkitbenchmarker import providers


class OCIProviderInfo(provider_info.BaseProviderInfo):
    UNSUPPORTED_BENCHMARKS = ['mysql_service']
    CLOUD = provider_info.OCI

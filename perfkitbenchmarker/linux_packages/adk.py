"""Module containing ADK installation."""


def Install(vm):
  """Installs ADK on the VM."""
  vm.Install('pip')
  vm.RemoteCommand('pip3 install "google-cloud-aiplatform[adk,agent_engine]"')

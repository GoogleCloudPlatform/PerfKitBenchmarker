# Copyright 2024 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing HAProxy installation and configuration for Redis HA."""

from absl import flags

FLAGS = flags.FLAGS

HAPROXY_CONFIG = '/etc/haproxy/haproxy.cfg'


def Install(vm):
  """Installs HAProxy on the VM."""
  vm.RemoteCommand('sudo apt-get update', ignore_failure=True)
  vm.RemoteCommand('sudo apt-get install -y haproxy')
  
  # Verify installation
  vm.RemoteCommand('haproxy -v')


def Configure(vm, primary_ip, replica_ip):
  """Configures HAProxy to proxy Redis traffic to primary and replica."""
  config_content = f"""
global
    log /dev/log local0
    log /dev/log local1 notice
    chroot /var/lib/haproxy
    stats socket /run/haproxy/admin.sock mode 660 level admin
    stats timeout 30s
    user haproxy
    group haproxy
    daemon

defaults
    log global
    mode tcp
    option tcplog
    timeout connect 5000
    timeout client  50000
    timeout server  50000

# Frontend for writes (port 6379) -> Primary
frontend redis_write
    bind *:6379
    default_backend redis_primary

# Backend for Primary
backend redis_primary
    mode tcp
    server primary {primary_ip}:6379 check

# Frontend for reads (port 6380) -> Replica
frontend redis_read
    bind *:6380
    default_backend redis_replica

# Backend for Replica
backend redis_replica
    mode tcp
    server replica {replica_ip}:6379 check
"""
  
  # Write config to file
  vm.RemoteCommand(f'echo "{config_content}" | sudo tee {HAPROXY_CONFIG}')


def Start(vm):
  """Starts HAProxy as a background process."""
  # Start HAProxy directly (not via systemctl - doesn't exist in containers)
  vm.RemoteCommand(
      'sudo haproxy -f /etc/haproxy/haproxy.cfg -D'
  )
  
  # Wait for HAProxy to start
  vm.RemoteCommand('sleep 3')
  
  # Verify HAProxy is running
  haproxy_check = vm.RemoteCommand('pgrep -f haproxy', ignore_failure=True)[0]
  if not haproxy_check:
    vm.RemoteCommand('echo "HAProxy failed to start"')
    raise Exception('HAProxy failed to start')
  
  # Check if HAProxy is listening on ports 6379 and 6380
  vm.RemoteCommand('echo "=== Checking HAProxy ports ==="')
  vm.RemoteCommand('sudo netstat -tlnp | grep haproxy || ss -tlnp | grep haproxy', ignore_failure=True)
  
  # Verify ports are open
  vm.RemoteCommand('nc -zv localhost 6379 || echo "Port 6379 not reachable"', ignore_failure=True)
  vm.RemoteCommand('nc -zv localhost 6380 || echo "Port 6380 not reachable"', ignore_failure=True)

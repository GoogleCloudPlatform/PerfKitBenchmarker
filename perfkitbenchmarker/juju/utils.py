# Copyright 2015 Canonical, Ltd. All rights reserved.
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

"""Utilities for working with Juju."""
import json
import subprocess
import shlex
import yaml
import os.path

from perfkitbenchmarker import flags

FLAGS = flags.FLAGS


def is_bootstrapped(controller=None):
    cmd = 'juju status'
    if controller:
        cmd += " -e %s" % controller
    try:
        run_command(cmd, controller=controller)
    except subprocess.CalledProcessError:
        return False
    return True


def bootstrap(controller=None):
    cmd = 'juju bootstrap'
    output = run_command(cmd, controller=controller)
    return output


def deploy(model, controller=None):
    cmd = "juju-deployer -c %s" % (os.path.expanduser(model))
    run_command(cmd, controller=controller)


def destroy_environment(controller=None):
    if controller:
        cmd = "juju destroy-environment -y"
        return run_command(cmd, controller=controller)
    return


def add_machine(controller=None):
    return run_command('juju add-machine', controller=controller)


def status(controller=None):
    cmd = 'juju status --output=json'
    return run_command(cmd, controller=controller) or {}


def get_first_unit_name(service, controller=None):
    """ Get the first unit that the service exists on"""

    cmd = "juju status --format=json"
    output = run_command(
        cmd,
        controller=FLAGS.controller
    )
    status = json.loads(output)

    # Subordinate charms have no unit, so we need to find its parent
    if 'units' not in status['services'][service]:
        host = status['services'][service]['subordinate-to'][0]
        unit = status['services'][host]['units'].keys()[0]
    else:
        unit = status['services'][service]['units'].keys()[0]
    return unit


def run_command(cmd, controller=None):
    try:
        if controller:
            cmd += ' -e %s' % controller
        output = subprocess.check_output(shlex.split(cmd))
        return output.strip()
    except subprocess.CalledProcessError:
        raise


def get_juju_environments(env=os.path.expanduser('~/.juju/environments.yaml')):
    environments = []

    if os.path.exists(env):
        f = open(env, 'r')
        raw = f.read()
        f.close()

        data = yaml.load(raw)
        for name in data['environments']:
            environments.append(name.strip())

    return environments


def juju_version():

    try:
        output = run_command('juju version')
        return output.strip()
    except:
        pass
    return None

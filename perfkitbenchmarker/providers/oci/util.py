"""Utilities for working with OracleCloud Web Services resources."""

import shlex

from absl import flags
from perfkitbenchmarker import vm_util
import six
import json
from perfkitbenchmarker import context

OCI_PREFIX = ['oci']

oci_suffix = ""

ADD_CLOUDINIT_TEMPLATE = """#!/bin/bash
echo "{user_name} ALL = NOPASSWD: ALL" >> /etc/sudoers
useradd {user_name} --home /home/{user_name} --shell /bin/bash -m
mkdir /home/{user_name}/.ssh
echo "{public_key}" >> /home/{user_name}/.ssh/authorized_keys
chown -R {user_name}:{user_name} /home/{user_name}/.ssh
chmod 700 /home/{user_name}/.ssh
chmod 600 /home/{user_name}/.ssh/authorized_keys
sudo iptables -F
"""

ADD_CLOUDINIT_ORACLE_TEMPLATE = """#!/bin/bash
echo "{user_name} ALL = NOPASSWD: ALL" >> /etc/sudoers
useradd {user_name} --home /home/{user_name} --shell /bin/bash -m
mkdir /home/{user_name}/.ssh
echo "{public_key}" >> /home/{user_name}/.ssh/authorized_keys
chown -R {user_name}:{user_name} /home/{user_name}/.ssh
chmod 700 /home/{user_name}/.ssh
chmod 600 /home/{user_name}/.ssh/authorized_keys
sudo systemctl stop firewalld
sudo systemctl disable firewalld
"""

def SetProfile(suffix):
    oci_suffix = suffix
    return suffix

def GetEncodedCmd(cmd):
#    oci_suffix1 = SetProfile(oci_suffix)
#    cmd = cmd + [f'--profile {oci_suffix1}']
#    print('********************************')
#    print(cmd)
    cmd_line = ' '.join(cmd)
    cmd_args = shlex.split(cmd_line)
    return cmd_args


def GetOciImageIdFromImage(operating_system, operating_system_version, shape, profile):
    # oci compute image list --all --operating-system "Canonical Ubuntu" --operating-system-version 18.04 --shape
    # VM.Standard.A1.Flex -c ocid1.tenancy.oc1..aaaaaaaadfogwfmgjoi35onknsnu6u5zfp43gh657appkvbghhzyhfhh5oya
    create_cmd = OCI_PREFIX + [
        'compute',
        'image',
        'list',
        '--all',
        '--operating-system \"%s\"' % operating_system,
        '--operating-system-version \"%s\"' % operating_system_version,
        '--shape %s' % shape,
        f'--profile {profile}']
    create_cmd = GetEncodedCmd(create_cmd)
    stdout, _ = vm_util.IssueRetryableCommand(create_cmd)
    image_names = json.loads(stdout)['data']
    if len(image_names) > 0:
        return image_names[0]['id']


def GetOciImageIdFromName(name, shape, profile):
    create_cmd = OCI_PREFIX + [
        'compute',
        'image',
        'list',
        '--all',
        '--display-name \"%s\"' % name,
        '--shape %s' % shape,
        f'--profile {profile}']
    create_cmd = GetEncodedCmd(create_cmd)
    stdout, _ = vm_util.IssueRetryableCommand(create_cmd)
    image_names = json.loads(stdout)['data']
    if len(image_names) > 0:
        return image_names[0]['id'], image_names[0]['operating-system'], image_names[0]['operating-system-version']


def GetAvailabilityDomainFromRegion(region):
    create_cmd = OCI_PREFIX + [
        'iam',
        'availability-domain',
        'list',
        '--region %s' % region]
    create_cmd = GetEncodedCmd(create_cmd)
    stdout, _ = vm_util.IssueRetryableCommand(create_cmd)
    availability_domains = json.loads(stdout)['data']
    availability_domains_list = []
    if len(availability_domains) == 1:
        availability_domains_list.append(availability_domains[0]['name'])
    elif len(availability_domains) == 2:
        availability_domains_list.append(availability_domains[0]['name'])
        availability_domains_list.append(availability_domains[1]['name'])
    else:
        availability_domains_list.append(availability_domains[0]['name'])
        availability_domains_list.append(availability_domains[1]['name'])
        availability_domains_list.append(availability_domains[2]['name'])
    return availability_domains_list


def GetFaultDomainFromAvailabilityDomain(availability_domain, profile):
    create_cmd = OCI_PREFIX + [
        'iam',
        'fault-domain',
        'list',
        '--availability-domain %s' % availability_domain,
        f'--profile {profile}']
    create_cmd = GetEncodedCmd(create_cmd)
    stdout, _ = vm_util.IssueRetryableCommand(create_cmd)
    fault_domains = json.loads(stdout)['data']
    fault_domains_list = []
    if len(fault_domains) == 1:
        fault_domains_list.append(fault_domains[0]['name'])
    elif len(fault_domains) == 2:
        fault_domains_list.append(fault_domains[0]['name'])
        fault_domains_list.append(fault_domains[1]['name'])
    else:
        fault_domains_list.append(fault_domains[0]['name'])
        fault_domains_list.append(fault_domains[1]['name'])
        fault_domains_list.append(fault_domains[2]['name'])
    return fault_domains_list


def GetOsFromImageFamily(operating_system):
    if "ubuntu" in operating_system:
        return 'Canonical Ubuntu'
    elif "Oracle" in operating_system:
        return 'Oracle Linux'


def GetOsVersionFromOs(operating_system_version, operating_system):
    if operating_system == 'Canonical Ubuntu':
        if "1804" in operating_system_version:
            return '18.04'
        elif "2004" in operating_system_version:
            return '20.04'
        elif "2204" in operating_system_version:
            return '22.04'
    elif operating_system == 'Oracle Linux':
        if '9' in operating_system_version:
            return '9'
        elif '8' in operating_system_version:
            return '8'
        

def GetPublicKey():
    cat_cmd = ['cat',
               vm_util.GetPublicKeyPath()]
    keyfile, _ = vm_util.IssueRetryableCommand(cat_cmd)
    return keyfile.strip()


def FormatTagsJSON(tags_dict):
    """Format a dict of tags into arguments.

  Args:
    tags_dict: Tags to be formatted.

  Returns:
    A string contains formatted tags
  """
    tags = ','.join(f'"{k}": "{v}"' for k, v in sorted(six.iteritems(tags_dict)) if k != 'owner')
    return json.dumps(tags)


def GetDefaultTags(timeout_minutes=None):
    """Get the default tags in a dictionary.

  Args:
    timeout_minutes: Timeout used for setting the timeout_utc tag.

  Returns:
    A dict of tags, contributed from the benchmark spec.
  """
    benchmark_spec = context.GetThreadBenchmarkSpec()
    if not benchmark_spec:
        return {}
    return benchmark_spec.GetResourceTags(timeout_minutes)


def MakeFormattedDefaultTags(timeout_minutes=None):
    """Get the default tags formatted.

  Args:
    timeout_minutes: Timeout used for setting the timeout_utc tag.

  Returns:
    A string contains tags, contributed from the benchmark spec.
  """
    return "{" + FormatTagsJSON(GetDefaultTags(timeout_minutes)) + "}"

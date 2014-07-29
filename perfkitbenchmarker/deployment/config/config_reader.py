#!/usr/bin/env python
# Copyright 2014 Google Inc. All rights reserved.
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

"""Deployment Configuration Reader class.

This class reads configuration parameters from an INI file and creates an
in-memory representation of a cluster and its nodes.

The following fields do not support either static or dynamic references. You
must specify a constant value:
  [cluster]: cluster_type
  [network]: tcp_ports, udp_ports

Please verify the function _Validate for details on what is checked.
"""

# TODO(user): Support reading configuration from a string (for testability).
# See http://bugs.python.org/issue9452.
# TODO(user): Name validations should be sensitive to cluster type.



import ConfigParser
import re

import config_exceptions

import logging

import perfkitbenchmarker.deployment.shared.ini_constants as ini_constants


class ConfigLoader(object):
  """Reads and parses a configuration ini file."""

  def __init__(self, filename):
    """Instantiates a config loader and parse config file if provided."""
    self._config = ConfigParser.RawConfigParser()
    if filename:
      self.LoadConfig(filename)

  def LoadConfig(self, filename):
    """Reads configuration settings from a file."""
    self._config.read(filename)
    self._sections = self._config.sections()
    if not self._sections:
      raise config_exceptions.DeployEmptyConfigError(
          'Unable to read configuration from %s' % filename)
    self._Validate()
    self.PrintConfig()

  def PrintConfig(self):
    """Prints the .ini file it is parsing as it was read."""
    logging.info('Config:')
    for section in self._config.sections():
      logging.info('  Section: [%s]', section)

  def GetSectionOptionsAsDictionary(self, section_name):
    """Reads a single section from the .ini and converts it into a dictionary.

    Option values might contain static references that are resolved before
    being added to the return dictionary.

    Args:
      section_name: string, .ini section name to be converted to a dictionary.
    Returns:
      Dictionary with (option name, option value) pairs.
    """
    options = {}
    for option in self._config.items(section_name):
      key, value = option
      options[key] = self._ResolveStaticReference(value)
    return options

  def GetSoftwarePackages(self, section):
    """Returns the ordered list of software to be installed (with parameters).

    Args:
      section: string, the name of the section
    Returns:
      Ordered list of tuples. Each tuple contains (package_name, parameters),
        where 'parameters' is a list of parameters. Static references are
        resolved here, dynamic references are left for execution time.
    """
    packages = {}
    for option in self._config.items(section):
      key, value = option
      if key.startswith(ini_constants.OPTION_PACKAGE_PREFIX):
        install_order = int(key[len(ini_constants.OPTION_PACKAGE_PREFIX):])
        package_name = value
        params = []
        if '(' in value:
          package_name, args = value.split('(')
          args = args.strip(')')
          # Resolve static references. Store runtime references for later.
          for p in [a.strip() for a in args.split(',')]:
            if p.startswith(ini_constants.OPTION_STATIC_REF_PREFIX):
              params.append(self._ResolveStaticReference(p))
            else:
              params.append(p)
        if install_order not in packages:
          packages[install_order] = []
        packages[install_order].extend((package_name, params))
    return [packages[a] for a in sorted(packages)]

  def GetPds(self, section):
    """Returns a list of PD definition tuples.

    Args:
      section: string, the name of the section
    Raises:
      InvalidPd: When no name was defined for the PD
    Returns:
      List of PD definition tuples (name, definition string).
    """
    pds = []
    for option in self._config.items(section):
      key, value = option
      if key.startswith(ini_constants.OPTION_PD_PREFIX):
        pd_name = key[len(ini_constants.OPTION_PD_PREFIX):]
        if not pd_name:
          raise config_exceptions.InvalidPdError(
              'The PD definition {} lacks a name.'.format(key))
        pds.append((pd_name, value))
    return pds

  def GetMetadata(self, section):
    """Returns a list (name, value) metadata tuples.

    Before the values are returned all static references are resolved. A static
    reference is a right hand value preceeded by the symbol @ (at). What
    follows must abide to the following format:
      [section_name].option_name.

    Args:
      section: string, the name of the section
    Returns:
      Returns a dictionary of name, value metadata pairs.
    """
    metadata = {}
    for option in self._config.items(section):
      key, value = option
      if (key not in ini_constants.NODE_OPTIONS and
          True not in [key.startswith(a) for a in
                       ini_constants.ALL_OPTION_PREFIXES]):
        if value.startswith(ini_constants.OPTION_STATIC_REF_PREFIX):
          metadata[key] = self._ResolveStaticReference(value)
        else:
          metadata[key] = value
    return metadata

  def NodeNameFromSectionName(self, section):
    """Returns the name of a node given a section name.

    Args:
      section: string. The name of the node section on the .ini file
    Returns:
      The name of the node if the section is actually a node, otherwise ''.
    """
    name = ''
    if section and section.startswith(ini_constants.SECTION_NODE_PREFIX):
      name = section[len(ini_constants.SECTION_NODE_PREFIX):]
    return name

  def _ResolveStaticReference(self, might_have_static_ref):
    """Resolves a static reference right hand value.

    Static references must be a one to one value, and resolved by a string
    replacement. A static value cannot be resolved to a list of values.

    Args:
      might_have_static_ref: string. Option value containing zero or more static
        references using the @[section].option_name notation.
    Returns:
      The value the static reference refers to.
    Raises:
      BadStaticReference: At least one reference was not resolved or resolved to
        an empty value.
    """
    try:
      return_string = might_have_static_ref
      if (might_have_static_ref and
          re.search(r'(\@\[[^\]]+\]\.\w+)', might_have_static_ref)):
        for m in re.finditer(r'(\@\[[^\]]+\]\.\w+)', might_have_static_ref):
          section, option = m.group(0).split('.')
          section = section.strip('@[] ')
          option = option.strip('. ')
          new_value = self._config.get(section, option)
          if not new_value:
            raise config_exceptions.BadStaticReferenceError(
                'Static reference {} results in empty value'.format(m.group(0)))
          return_string = return_string.replace(m.group(0), new_value)
    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
      raise config_exceptions.BadStaticReferenceError(
          'Could not resolve static references on {}'.format(
              might_have_static_ref))
    return return_string

  def _Validate(self):
    """Verifies that all mandatory fields are present."""
    self._ValidateSectionsExist()
    self._ValidateClusterSection()
    self._ValidateNodeSections()

  def _ValidateSectionsExist(self):
    """Verifies that all mandatory sections are present.

    Raises:
      NoClusterSectionInConfig: No cluster section in the config file.
      NoNetworkSectionInConfig: No network section in the config file.
    """
    if ini_constants.SECTION_CLUSTER not in self._sections:
      raise config_exceptions.NoClusterSectionInConfigError(
          'Please add a cluster section to the config file.')

  def _ValidateClusterSection(self):
    """Verifies that all mandatory cluster options are present and valid.

    Raises:
      NoClusterTypeInConfig: invalid cluster type (or none)
      NoProjectInConfig: invalid project name (or none)
      NoZoneInConfig: invalid zone (or none)
    """
    try:
      self.cluster_type
    except ConfigParser.NoOptionError:
      raise config_exceptions.NoClusterTypeInConfigError(
          'Please specify the cluster type in the config file.')
    try:
      self.project
    except ConfigParser.NoOptionError:
      raise config_exceptions.NoProjectInConfigError(
          'Please specify a project name or ID in the config file.')
    try:
      self.zone
    except ConfigParser.NoOptionError:
      raise config_exceptions.NoZoneInConfigError(
          'Please specify a zone to deploy in the config file. At this point '
          'deployments must target a single zone for all nodes.')

  def _ValidateNodeSections(self):
    """Verifies that all mandatory options are present and valid for each node.

    Raises:
      NoNodeTypesInConfig: No a single valid node type was defined (nodes with
        count=0 are fine)
      InvalidVmName: If at least one VM section specifies an invalid VM name (
        defined as something GCE would not allow)
      InvalidPd: If the at least on PD instance has a bad device name,
        mountpoint, or size
    """
    if not self.node_sections:
      raise config_exceptions.NoNodeTypesInConfigError(
          'No VM types found. Please specify at least one type of VM node.'
          'For testing purposes it is possible to set the vm count option to '
          'zero, but you must define at least one node type for validation.')
    else:
      vm_name_regex = re.compile(r'^[A-Za-z][A-Za-z0-9\-]*$')
      for section_name in self.node_sections:
        if not vm_name_regex.match(self.NodeNameFromSectionName(section_name)):
          raise config_exceptions.InvalidVmNameError(
              'Node type {} name is not valid as VM name.'.format(section_name))
    pd_name_regex = re.compile(r'^\w[\w\-]*$')
    path_regex = re.compile(r'^/?\w[\w\-]*(/?\w[\w\-]+)*/?$')
    for pd_spec in self.pd_specs:
      name, value = pd_spec
      if not pd_name_regex.match(name):
        raise config_exceptions.InvalidPdError(
            'PD specification {} name is not valid.'.format(name))
      try:
        size, disk_type, mountpoint = value.split(':')
        if int(size) < 10 or not path_regex.match(mountpoint):
          raise config_exceptions.InvalidPdError()
      except:
        raise config_exceptions.InvalidPdError(
            'The righthand of the PD specification {}={}:{}:{} is not valid. '
            'Make sure device names and mountpoint are valid names, and PD size'
            ' is an integer greater than 10'.format(name,
                                                    size,
                                                    disk_type,
                                                    mountpoint))

  @property
  def cluster_type(self):
    return self._config.get(
        ini_constants.SECTION_CLUSTER, ini_constants.OPTION_CLUSTER_TYPE)

  @property
  def project(self):
    return self._ResolveStaticReference(
        self._config.get(
            ini_constants.SECTION_CLUSTER, ini_constants.OPTION_PROJECT))

  @property
  def zone(self):
    return self._ResolveStaticReference(
        self._config.get(
            ini_constants.SECTION_CLUSTER, ini_constants.OPTION_ZONE))

  @property
  def node_sections(self):
    sections = []
    for section in self._config.sections():
      if section.startswith(ini_constants.SECTION_NODE_PREFIX):
        sections.append(section)
    return sections

  @property
  def pd_specs(self):
    pd_specs = []
    for section in self._config.sections():
      for option in self._config.items(section):
        option_name, option_value = option
        if option_name.startswith(ini_constants.OPTION_PD_PREFIX):
          pd_specs.append((option_name[len(ini_constants.OPTION_PD_PREFIX):],
                           option_value))
    return pd_specs


class IniPrinter(object):
  """Helper class. Exposes printing primitives to other classes."""

  def Header(self, name):
    """Prints a section header.

    Args:
      name: string. The header name
    """
    print '\n[{}]'.format(name)

  def Option(self, name, value):
    """Prints a name, value pair in .ini format.

    Args:
      name: string. The name of the option.
      value: string. The value of the option.
    """
    print '{} = {}'.format(name, value)

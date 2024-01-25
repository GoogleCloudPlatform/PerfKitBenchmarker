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
"""Module to allow for auto registratation of classes by attributes."""
import itertools
import typing
from typing import Any, Optional
from perfkitbenchmarker import errors


T = typing.TypeVar('T')


def GetRegisteredClass(
    registry: dict[tuple[Any], type[T]],
    base_class: type[T],
    default_class: Optional[type[T]] = None,
    **kwargs
) -> type[T]:
  """Returns the subclass with the corresponding attributes.

  Args:

  Args:
    registry: The dictionary of types, pointing from spec attributes -> classes.
    base_class: The parent class whose subclass will be returned.
    default_class: If provided, the class to return if no subclass is found. If
      not provided, throws an exception if no subclass found.
    **kwargs: Every attribute/value of the subclass's REQUIRED_ATTRS that were
      used to register the subclass.

  Raises:
    Exception: If no class could be found with matching attributes &
    default_class not provided, or found class was not a subclass of the base.
  """
  key = [base_class.__name__]
  key += sorted(kwargs.items())
  resource = registry.get(tuple(key), default_class)
  if not resource:
    raise errors.Resource.SubclassNotFoundError(
        'No %s subclass defined with the attributes: %s'
        % (base_class.__name__, kwargs)
    )
  if not issubclass(resource, base_class):
    raise errors.Resource.SubclassNotFoundError(
        'Class %s was registered for type %s but they did not match each other.'
        % (resource.__name__, base_class.__name__)
    )

  # Set the required attributes of the resource class
  for key, value in kwargs.items():
    setattr(resource, key, value)

  return resource


def RegisterClass(
    registry: dict[tuple[Any], type[T]],
    cls: type[T],
    required_attrs: Optional[list[str]],
    cls_type: Optional[str],
):
  """Adds the class with its attributes to the registry.

  Args:
    registry: The dictionary of types, pointing from spec attributes -> classes.
    cls: The given class which is being added to the dictionary.
    required_attrs: The required attributes which a class must have defined to
      be registered.
    cls_type: The class type to be registered (typically base class name).
  """
  if all(hasattr(cls, attr) for attr in required_attrs) and cls_type:
    # Flatten list type attributes with cartesian product.
    # If a class have two list attributes i.e.
    # class Example(AutoRegisterResourceMeta):
    #   CLOUD = ['GCP', 'AWS']
    #   ENGINE = ['mysql', 'postgres']
    #   ....
    # GetResourceClass(Example, CLOUD='GCP', ENGINE='mysql')
    # would return Example.
    attributes = [[cls_type]]
    for attr in sorted(required_attrs):
      value = getattr(cls, attr)
      if not isinstance(value, list):
        attributes.append([(attr, value)])
      else:
        attributes.append([(attr, i) for i in value])

    # Cross product
    for key in itertools.product(*attributes):
      registry[tuple(key)] = cls

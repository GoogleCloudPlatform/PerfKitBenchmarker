"""Contains all benchmark imports and a list of benchmarks."""

import pkgutil


def _LoadModules():
  result = []
  for importer, modname, ispkg in pkgutil.iter_modules(__path__):
    result.append(importer.find_module(modname).load_module(modname))
  return result


BENCHMARKS = _LoadModules()

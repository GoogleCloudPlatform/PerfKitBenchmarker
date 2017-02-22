def dict_inherit(base, child):
  final_dict = base.copy()
  for key, value in child.items():
    if key[-1] == '+':
      if type(value) is dict:
        final_dict[key[:-1]] = dict_inherit(base[key[:-1]], value)
      elif type(value) is list:
        final_dict[key[:-1]] = base[key[:-1]] + value
    else:
      final_dict[key] = value
  return final_dict

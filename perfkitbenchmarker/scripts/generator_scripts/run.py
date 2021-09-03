# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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

import jinja2
import json5 as json
import re
import ast
import argparse
import os
import posixpath
import glob
import shutil
import sys
import traceback
from jsonschema import Draft7Validator

CUSTOM_WORKLOAD_DIR = "custom_pkb_workloads"


def check_valid_file_path(fileName):
  filePath = os.path.realpath(fileName)
  if os.path.islink(filePath):
    raise RuntimeError('Symlinks are not allowed')
  basedir = os.getcwd()
  if not filePath.startswith(basedir):
    raise Exception('Incorrect File Path Provided !! Please make sure the file is within your PKB'
                    'host directory. Aborting')
  return True


def get_Perfkit_Root():
  check_valid_file_path(__file__)
  filePath = os.path.realpath(__file__)
  perfkit_root = os.path.join(os.path.dirname(filePath),
                              os.path.pardir, os.path.pardir)
  return perfkit_root


def parse_flags(cfg, benchmark_variables, flag_option):
  flag_has_cmd = False
  for i, old_dict in enumerate(cfg[flag_option]):
    new_dict = update_cfg_dict(old_dict, cfg["metadata"]["generated_name"])
    if new_dict["default_cmd"]:
      flag_has_cmd = True
    cfg[flag_option][i] = new_dict
    benchmark_variables[old_dict["name"]] = "FLAGS.{0}".format(new_dict["full_name"])
  return flag_has_cmd


def parse_template(config, workload_force_overwrite):
  cfg = config["workload1"]
  excluded_keys = ["config_flags", "tunable_flags", "metadata"]
  # generate unique name
  cfg["metadata"] = config["workload1"]["metadata"]
  generated_name = cfg["metadata"]["name"]
  replacements = (',', '-', '!', '?')
  for r in replacements:
    generated_name = generated_name.replace(r, ' ')
  generated_name = "_".join(generated_name.lower().split())
  check_if_unique_benchmark_name(generated_name, workload_force_overwrite)
  cfg["metadata"]["generated_name"] = generated_name
  benchmark_variables = {}
  config_flags_has_cmd = False
  if "config_flags" in cfg:
    config_flags_has_cmd = parse_flags(cfg, benchmark_variables, "config_flags")
  tunable_flags_has_cmd = False
  if "tunable_flags" in cfg:
    tunable_flags_has_cmd = parse_flags(cfg, benchmark_variables, "tunable_flags")
  if "output_dir" in cfg:
    for key in cfg["output_dir"]:
      benchmark_variables[key] = '"{}"'.format(cfg["output_dir"][key])
    benchmark_variables["output_dir"] = cfg["output_dir"]
  replace_variables(config, benchmark_variables, excluded_keys)
  cfg["config_flags_has_cmd"] = config_flags_has_cmd
  cfg["tunable_flags_has_cmd"] = tunable_flags_has_cmd
  return cfg


def replace_variables(obj, benchmark_variables, excluded_keys=[]):
  if isinstance(obj, dict):
    for k, v in obj.items():
      if k not in excluded_keys:
        obj[k] = replace_variables(v, benchmark_variables, excluded_keys)
    return obj
  elif isinstance(obj, list):
    for elem in range(len(obj)):
      obj[elem] = replace_variables(obj[elem], benchmark_variables, excluded_keys)
    return obj
  else:
    return parse_field(obj, benchmark_variables)


def parse_field(field, benchmark_variables):
  new_cmd = '"{0}"'.format(field)
  cmd_variables = []
  vars_used = set()
  for match in re.findall(r"\$([A-Za-z0-9_]+)", new_cmd):
    if match in benchmark_variables:
      # escape existing '{' and '}' in order to not create issues with our variables when formatting
      if not len(vars_used):
        new_cmd = new_cmd.replace("{", "{{").replace("}", "}}")
      new_cmd = new_cmd.replace("${0}".format(match), '{' + str(len(vars_used)) + '}')
      replace_value = benchmark_variables[match]
      if replace_value not in vars_used:
        cmd_variables.append(replace_value)
        vars_used.add(replace_value)
  if "./" in new_cmd:
    new_cmd = '"cd {0} && ".format(BENCHMARK_DIR) + ' + new_cmd
  if cmd_variables:
    format_str = ".format({0})".format(", ".join(cmd_variables))
    new_cmd += format_str
  if new_cmd == '""':
    new_cmd = ""
  return new_cmd


def update_cfg_dict(cfg, benchmark_name):
  new_cfg = cfg.copy()
  if "range" in cfg:
    range_val = new_cfg["range"]
    range_type = "string"
  new_cfg["full_name"] = "{0}_{1}".format(benchmark_name, new_cfg["name"])
  if "default" in cfg:
    default_field = new_cfg["default"]
    default_val, default_type, default_cmd = parse_default_field(default_field, new_cfg["type"])
  if "range" in cfg and "default" in cfg:
    if range_val.count("-") == 1:
      splitted = range_val.split('-')
      for i in splitted:
        if not is_int(i):
          break
      else:
        new_cfg["min"] = int(splitted[0])
        new_cfg["max"] = int(splitted[1])
        if default_val != "" and not default_cmd:
          if not new_cfg["min"] <= default_val <= new_cfg["max"]:
            raise Exception("Default value of '{0}' is not in range".format(new_cfg["name"]))
        range_type = "range"
    elif re.search(r"\((.+)(,\s*.+)*\)|\(\)", range_val):
      if default_val != "":
        range_list = ast.literal_eval(range_val)
        check_val = default_val.strip("'") if default_type == "string" else default_val
        if check_val not in range_list:
          raise Exception("Default value of '{0}' is not in range".format(new_cfg["name"]))
      range_type = "enum"
    elif re.search(r"\[(.+)(,\s*.+)*\]|\[\]", range_val):
      if default_val != "":
        range_list = ast.literal_eval(range_val)
        check_val = default_val.strip("'") if default_type == "string" else default_val
        if check_val not in range_list:
          raise Exception("Default value of '{0}' is not in range".format(new_cfg["name"]))
        default_val = "[{0}]".format(default_val)
      range_type = "list"
    elif not range_val:
      range_type = default_type
  if "range" in cfg:
    new_cfg["range"] = range_val
    new_cfg["range_type"] = range_type
  if "default" in cfg:
    if default_val == "" or default_cmd:
      new_cfg["default"] = None
    else:
      new_cfg["default"] = default_val
    new_cfg["default_cmd"] = default_cmd
  new_cfg["type"] = default_type
  # Parse this manual because we skipped the 'config' key from the replace_variables function
  for key in ("default_cmd", "setcmd"):
    if key in new_cfg:
      cmd = new_cfg[key]
      if cmd:
        if "./" in cmd:
          new_cfg[key] = '"cd {0} && ".format(BENCHMARK_DIR) + ' + '"{0}"'.format(cmd.strip("'"))
  return new_cfg


def parse_default_field(field, type):
  field_default_cmd = None
  field_raw_val = field
  if type.startswith("OS") or type.startswith("custom"):
    field_raw_type_parsed = type.split("-")
    field_val, field_type = parse_default_field_simple(field_raw_val, field_raw_type_parsed[1])
    field_default_cmd = field_raw_val
  else:
    field_val, field_type = parse_default_field_simple(field_raw_val, type)
  return field_val, field_type, field_default_cmd


def parse_default_field_simple(val, type):
  new_type = "string"
  new_val = val
  if type == "num":
    if is_int(val):
      new_val = int(val)
      new_type = "integer"
    elif is_float(val):
      new_val = float(val)
      new_type = "float"
    else:
      new_type = "integer"
  elif type == "hex":
    if is_hex(val):
      new_val = int(val, 16)
    new_type = "integer"
  return new_val, new_type


def is_int(x):
  try:
    int(x)
  except Exception:
    return False
  return True


def is_float(x):
  try:
    float(x)
  except Exception:
    return False
  return True


def is_hex(x):
  status = True
  if x.lower().startswith('0x'):
    try:
      int(x, 16)
    except Exception:
      status = False
  else:
    status = False
  return status


def render_template(template, cfg):
  if check_valid_file_path(template):
    with open(template) as f:
      template = jinja2.Template(f.read())
    return template.render(cfg)
  raise Exception('Invalid file path !! Aborting !!')


def write_generated_template(template, file):
  with open(file, "w") as f:
    f.write(template)


def valid_file(path):
  if os.path.isfile(path):
    return path
  raise argparse.ArgumentTypeError("The file path: '{}' does not exist".format(path))


def valid_dir(path):
  if os.path.isdir(path):
    return path
  raise argparse.ArgumentTypeError("The directory: '{}' does not exist".format(path))


def valid_json_file(path):
  if valid_file(path) and (path.endswith('.json') or path.endswith('.json5')):
    return path
  raise argparse.ArgumentTypeError("The file path: '{}' is not a valid json file".format(path))


def load_json(filename):
  if check_valid_file_path(filename):
    with open(filename) as f:
      data = json.load(f)
    return data
  raise Exception('Invalid file path !! Aborting !!')


def generate_benchmark_files(benchmark_name, rendered, data_dir):
  perfkit_root = get_Perfkit_Root()
  benchmark_full_name = "{0}_benchmark".format(benchmark_name)
  benchmark_script_name = "{0}.py".format(benchmark_full_name)
  benchmarks_dir = posixpath.join(perfkit_root, "linux_benchmarks", CUSTOM_WORKLOAD_DIR)
  write_generated_template(rendered, posixpath.join(benchmarks_dir, benchmark_script_name))
  benchmark_data_dir = posixpath.join(perfkit_root, "data", CUSTOM_WORKLOAD_DIR, benchmark_full_name)
  shutil.rmtree(benchmark_data_dir, ignore_errors=True)
  shutil.copytree(data_dir, benchmark_data_dir)


def generate_package_file(benchmark_name, rendered, group_name):
  perfkit_root = get_Perfkit_Root()
  pkg_script_name = "{0}_{1}_deps.py".format(benchmark_name, group_name)
  pkg_dir = posixpath.join(perfkit_root, "linux_packages", CUSTOM_WORKLOAD_DIR)
  write_generated_template(rendered, posixpath.join(pkg_dir, pkg_script_name))


def workload_existence_check(benchmark_name):
  def find_files_in_path(path, pattern):
    found_original = False
    data_existing_names = [os.path.basename(p)
                           for p in glob.glob(os.path.join(path, pattern))]
    if data_existing_names:
      found_original = True
    return found_original

  perfkit_root = get_Perfkit_Root()
  search_results = []
  search_results.append(find_files_in_path(posixpath.join(perfkit_root, "data"),
                                           "{0}*_benchmark".format(benchmark_name)))
  search_results.append(find_files_in_path(posixpath.join(perfkit_root, "linux_packages"),
                                           "{0}*_deps.py".format(benchmark_name)))
  search_results.append(find_files_in_path(posixpath.join(perfkit_root, "linux_benchmarks"),
                                           "{0}*_benchmark.py".format(benchmark_name)))
  search_results.append(find_files_in_path(posixpath.join(perfkit_root, "data", CUSTOM_WORKLOAD_DIR),
                                           "{0}*_benchmark".format(benchmark_name)))
  search_results.append(find_files_in_path(posixpath.join(perfkit_root, "linux_packages", CUSTOM_WORKLOAD_DIR),
                                           "{0}*_deps.py".format(benchmark_name)))
  search_results.append(find_files_in_path(posixpath.join(perfkit_root, "linux_benchmarks", CUSTOM_WORKLOAD_DIR),
                                           "{0}*_benchmark.py".format(benchmark_name)))

  for element in search_results:
    if element:
      return True
  return False


def generate_readme_file(benchmark_name, rendered):
  perfkit_root = get_Perfkit_Root()
  benchmark_full_name = "{0}_benchmark".format(benchmark_name)
  readme_file = posixpath.join(perfkit_root, "data", CUSTOM_WORKLOAD_DIR, benchmark_full_name, "README.md")
  write_generated_template(rendered, readme_file)


def check_if_unique_benchmark_name(benchmark_name, workload_force_overwrite):
  elementExists = workload_existence_check(benchmark_name)
  if elementExists and not workload_force_overwrite:
      raise Exception("Benchmark {0} already exists. Please use '-o True' "
                      "if you wish to overwrite".format(benchmark_name))
  else:
    print("Benchmark '{0}' will be generated".format(benchmark_name))


def generate_pkg_files(cfg, package_template_path, benchmark_name):
  for vm_group in cfg["vm_groups"]:
    local_cfg = vm_group.copy()
    local_cfg["metadata"] = cfg["metadata"]
    local_cfg["name"] = local_cfg["name"].strip('"')
    pkg_cfg = generate_pkg_config(local_cfg, benchmark_name)
    generated_pkg_template = render_template(package_template_path, pkg_cfg)
    generate_package_file(benchmark_name, generated_pkg_template, local_cfg["name"])


def generate_pkg_config(config, benchmark_name):
  cfg = config.copy()
  flag_default_name = "{0}_deps".format(benchmark_name)
  pkg_type = "OS"
  for dep in cfg["deps"]:
    lower_name = dep["os_type"].lower()
    if "ubuntu" in lower_name:
      dep["generated_os_name"] = "ubuntu"
      pkg_type = "ubuntu"
    elif "centos" in lower_name:
      dep["generated_os_name"] = "centos"
      pkg_type = "centos"
    if "packages_info" in dep:
      for pkg in dep["packages_info"]:
        pkg_name = pkg["name"].lower().strip('"')
        pkg["flag_name"] = "{0}_{1}_{2}_{3}_ver".format(flag_default_name, pkg_type,
                                                        pkg_name.replace("-", "_"),
                                                        cfg["name"])
        pkg["flag_description"] = "Version of {0} package".format(pkg_name)
        if "ver" in pkg and pkg["ver"]:
          pkg["flag_default"] = pkg["ver"]
        else:
          pkg["flag_default"] = None
  return cfg


def generate_read_config(readme_template, config):
  packageflags = {}
  benchmarkflags = {}
  benchmark_cfg = config.copy()
  if "config" in benchmark_cfg:
    for conf in benchmark_cfg["config"]:
      if conf["type"] == "OS-num":
        if "description" in conf:
          benchmarkflags["{0} = user defined".format(conf["name"])] = conf["description"]
      else:
        if "default" in conf and "description" in conf:
          benchmarkflags["{0} = <{1}>".format(conf["name"], conf["default"])] = conf["description"]
  local_conf = config.copy()
  for vm_group in local_conf["vm_groups"]:
    for dep in vm_group["deps"]:
      if "packages_info" in dep:
        pkgs = dep["packages_info"]
        for pkg in pkgs:
          pkg_name = pkg["name"].lower().strip('"')
          if "ver" in pkg and pkg["ver"]:
            pkg_default = pkg["ver"]
          else:
            pkg_default = None
          pkg_description = "The version of {0} package on corresponding OS".format(pkg_name)
          packageflags["{0} = <{1}>".format(pkg_name, pkg_default)] = pkg_description
  local_conf.update(benchmark_cfg)
  template_readme = render_template(readme_template, local_conf)
  return template_readme


def validate_json(json):
  def GenerateErrorMessage(error):
    err = error.path[0]
    for i in range(1, len(error.path)):
      if isinstance(error.path[i], str):
        err += ".{0}".format(error.path[i])
      elif isinstance(error.path[i], int):
        err += "[{0}]".format(error.path[i])
    return err
  schema = load_json("schema.json")
  v = Draft7Validator(schema)
  errors = sorted(v.iter_errors(json), key=lambda e: e.path)
  if errors:
    for error in errors:
      if error.instance == '':
        print("[ERROR] Missing value: field {0} cannot be empty.".format(GenerateErrorMessage(error)))
      elif "required" in error.message:
        print("[ERROR] Missing key: {0}. JSON path: {1}".format(error.message, GenerateErrorMessage(error)))
      else:
        print("[ERROR] Problem around the line containing the value '{0}'. JSON path: {1}".format(error.instance,
                                                                                                  GenerateErrorMessage(error)))
    return False
  return True


def str2bool(v):
  if v.lower() in ('yes', 'true', 't', 'y', '1'):
    return True
  elif v.lower() in ('no', 'false', 'f', 'n', '0'):
    return False
  else:
    raise Exception('Boolean value expected. Got ' + str(v))


if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  templates_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "templates")
  benchmark_template_default_path = os.path.join(templates_dir, "template_benchmark.j2")
  package_template_path = os.path.join(templates_dir, "template_package.j2")
  readme_template_path = os.path.join(templates_dir, "template_readme.j2")
  parser.add_argument("-b", "--benchmark_template", type=valid_file,
                      help="jinja template file",
                      default=benchmark_template_default_path)
  parser.add_argument("-w", "--workload_template", type=valid_json_file,
                      help="workload template file in json format",
                      default=os.path.join("workloads", "mpich_compile_multi_node",
                                           "mpich_template.json"))
  parser.add_argument("-v", "--validate",
                      help="Validate the json with the schema",
                      type=str2bool,
                      default=True)
  parser.add_argument("-o", "--workload_force_overwrite",
                      help="Overwrite existing workload",
                      type=str2bool,
                      default=False)
  args = parser.parse_args()
  try:
    data_dir = os.path.dirname(args.workload_template)
    config = load_json(args.workload_template)
    if not args.validate:
      print("[WARNING] Validation was disabled. Benchmark files may contain errors")
    if not args.validate or (args.validate and validate_json(config)):
      cfg = parse_template(config, args.workload_force_overwrite)
      cfg["metadata"]["data_dir"] = "{0}_benchmark".format(cfg["metadata"]["generated_name"])
      benchmark_name = cfg["metadata"]["generated_name"]
      generate_pkg_files(cfg, package_template_path, benchmark_name)
      generated_bench_template = render_template(args.benchmark_template, cfg)
      generate_benchmark_files(benchmark_name, generated_bench_template, data_dir)
      generated_readme_template = generate_read_config(readme_template_path, cfg)
      generate_readme_file(benchmark_name, generated_readme_template)
    else:
      print("\nFailed to generate the benchmark files for {0} - Errors were encountered.".format(args.workload_template))
      exit(1)
  except Exception as e:
    print("[ERROR] Other exception occurred. Please use the proper JSON template format")
    print("[ERROR] Exception details for debugging:")
    ex_type, ex_value, ex_traceback = sys.exc_info()
    trace_back = traceback.extract_tb(ex_traceback)
    stack_trace = []
    for trace in trace_back:
      stack_trace.append("File: {0}, Line: {1}, Message: {2}".format(trace[0], trace[1], trace[3]))
    print("            Exception type : {0} ".format(ex_type.__name__))
    print("            Exception message : {0}".format(ex_value))
    print("            Stack trace:")
    for line in stack_trace:
      print("                {0}".format(line))
    print("\nFailed to generate the benchmark files for {0} - Errors were encountered.".format(args.workload_template))
    exit(1)

# Template Generator

The template generator is used to create pkb benchmarks starting from a json file.

## Prerequisites
```
cd perfkitbenchmarker/scripts/generator_scripts
sudo pip install -r requirements.txt
```

## How to add a new workload
1. Create a new directory for your workload
2. Copy the sample template.json file to your directory and update it with information of the workload
3. Add the necessary scripts to this directory
```
cd perfkitbenchmarker/scripts/generator_scripts
mkdir workloads/<new workload>
cp templates/template.json workloads/<new workload>
```

## How to run
```
cd perfkitbenchmarker/scripts/generator_scripts
./run.py -w workloads/<new workload>/workload_template
```

## How to modify an existing workload with overwrite flag
```
cd perfkitbenchmarker/scripts/generator_scripts
./run.py -w workloads/<existing workload>/workload_template -o True
```

The `workload_template` variable is the template json file used in the benchmark.

## Examples

### Dummy Benchmark

#### Single mode
```
Generate with: `cd perfkitbenchmarker/scripts/generator_scripts && ./run.py -w workloads/dummy/dummy_template.json`

Run on Centos with: `./pkb.py --cloud=AWS  --benchmarks=dummy --machine_type=m5.xlarge --os_type=centos7`

Run on Ubuntu with: `./pkb.py --cloud=GCP  --benchmarks=dummy --machine_type=n1-standard-4 --os_type=ubuntu2004`

Run on baremetal with: `./pkb.py --benchmarks=dummy --os_type=ubuntu2004 --benchmark_config_file=./dummy.yaml --http_proxy=<proxy> --https_proxy=<proxy>
```
### dummy.yaml
```
static_vms:
  - &vm0
    ip_address: x.x.x.x
    user_name: pkb
    ssh_private_key: ~/.ssh/id_rsa
    internal_ip: x.x.x.x

dummy:
  vm_groups:
    vm_group1:
      static_vms:
        - *vm0
```


## Template Description

```
# This template and adjacent resources (such as custom scripts) need to be placed in the same
# directory. Adjacent resources can be placed in sub-directories, as long as they are properly
# referenced in the template (e.g. "./scripts-dir/custom-script.sh" instead of "./custom-script.sh").
#
# All these resources will be copied by the code generator in the benchmark's data directory - perfkitbenchmarker/data/workload1_name/
# so that we have a centralized point for workload metadata (OS version, kernel version, package versions)
#
# When reporting the workload metadata in the PKB output, the versions will be checked against the template
# and all mismatches will be reported using a format such as
# ("package name": {"WARNING": "", "version on SUT": xxx, "version expected": xxy})
{
  "workload1": {
    # workload-specific metadata. Use the "metadata_name" field to define
    # the name of the benchmark in PKB, but be careful for name conflicts.
    # Report type and load in the PKB output as-is
    "metadata": {
      "name": "Workload1 Name",
      "type": "workload category - Web, DB, HPC, Media, etc. ",
      "load": "Average or Peak or Unknown"
    },
    # specify the requirements per vm group
    "vm_groups": [
      # [REQUIRED] At least 1 vm_group needs to be defined
      {
        "name": "vm_group1",
        # [REQUIRED] per-os package dependencies for the benchmark. At least 1 os entry required.
        # All these packages will be reported in the PKB output (package name + version), alongside a warning
        # if the package version on the SUT does not match the version provided in the template. Please find a
        # warning format suggestion at the top of the template
        "deps": [
          { # start of OS description and packages
            # [REQUIRED] Choose the os_type from the option available
            # "amazonlinux1", "amazonlinux2", "centos7", "centos8", "clear", "core_os", "cos", "debian9",
            # "debian10", "rhel7", "rhel8", "ubuntu1604", "ubuntu1710", "ubuntu1804", "ubuntu1910", "ubuntu2004"
            "os_type": "centos7"
            # [Optional] Information about packages that need to be installed
            # for the OS specified above
            "packages_info": [
              {
                # [REQUIRED] name of the package to be installed. This will be displayed in the metadata.
                "name": "name1",
                # [OPTIONAL] package version. This will be displayed in the metadata.
                "ver": "v0.xx"
              },
              {
                "name": "name2",
                "ver": "v0.yyy
              }
            ],
            # [Optional] Scripts to install packages for OS specified above
            "packages_install": [
              {
                "install_script": "Name of the script which has the instructions to install the packages"
              },
              {
                "install_script": "Name of the script which has the instructions to install the packages"
              }
            ],
            # [Optional] Scripts to remove packages for OS specified above
            "packages_remove": [
              {
                "uninstall_script: "Name of the script which has instructions to remove packages"
              }
            ]
          } # end of OS description and packages
        ], # end of "deps" list
      } # end of "vm_group1"
    ] # end of "vm_groups" list
    # [Optional] logic for installing the workload. The "install" field is a list
    # because, for multi node workloads, installing might need to be split into phases.
    # Each step is run on the vm_group or individual vm within vm_group specified as the target.
    # Using this workflow, for example we can run step 1 on the master node, then run step 2
    # on only one worker node, then run step 3 across all worker nodes.
    #
    # Because multi-node setups will always require knowing the IPs of other machines involved,
    # these can be accessed as "$vm_group1.IP_list", which will provide a list of IPs belonging
    # to every instance in that VM group. The list can be indexed to access
    # a single IP - "$vm_group1.IP_list[0]"
    "install": [
      { #  step 1
        # [REQUIRED] target can be either an entire vm_group, or a single vm within a vm_group
        # "target": "vm_group1" -> run this install step in parallel on all vms in vm_group1
        # "target": "vm_group2[0]" -> run this install step only on vm with index 0 in vm_group2
        "target": "vm_group or individual vm where to run this install step",
        # [REQUIRED] cmd is run on the SUT. It can be either an OS command or custom script (needs to be
        # provided alongside this template if custom). All custom resources mentioned from here on, which
        # are provided alongside this template, will be placed by the code generator into
        # perfkitbenchmarker/data/workload1_name/.
        # If custom script, it will be specified with "./", while OS commands are expected to be
        # in PATH and don't use file path.
        # If null string (""), ignore.
        "cmd": "command to install the workload or app",
        # [Optional] internal_resources define all resources that need to be downloaded from behind a
        # proxy. Many times, workloads use resources that are not yet public and will
        # need to be downloaded from a local URL. All these resources will be downloaded by the
        # workload on the PKB host (NOT the SUTs), then copied onto the SUTs in the same directory
        # as the custom scripts provided with the templates, so that the scripts can use them.
        "internal_resources": [
          {
            "name": "Description of the resource",
            "url": "Internal URL where the resource can be downloaded"
          }
        ]
      }, # end of step 1
      {  # step 2
        "target": "vm_group or individual vm where to run this install step"
        "cmd": "command to run in step 2",
        # No internal resources for step 2
      }
    ], # end of "install" list
    # [REQUIRED] This section describes how the workload is run
    "run": [
      {# [REQUIRED] Target can be either the 'local' which is your PKB host, an entire vm_group, or a single vm within a vm_group.
       # eg:- 'target': 'vm_group1' -> run this step in parallel on all vms in vm_group1.
       #      'target': 'vm_group2[0]' -> run this step only on vm with index 0 in vm_group2 "
       #      'target': "local" -> Run the command on PKB host
       "target": "individual vm where to start the workload run",
       # [REQUIRED] It will be executed on the machine as provided.
       "cmd": "command to execute / run the workload"
      },
      {
        # Run the command on PKB host
        "target":"local"
        "cmd" : "command to execute / run the workload"
      }
    ],
    # [OPTIONAL] This section describes the workload knobs for the workload configuration
    "config_flags": [
      {
        # [REQUIRED] The name will become a PKB flag, using the format workload1_knob1_name.
        # Any blank spaces in the name will be replaced by "_".
        #
        # Any "key":"value" pair defined in the template can be referenced as
        # $key
        # throughout the template.
        #
        "name": "knob1_name",
        # [OPTIONAL, but highly recommended] If not blank, the description will serve as a description for the PKB flag
        "description": "Description of the knob",
        # [OPTIONAL] Ignore if null string. Accepted format:
        # "range": "1-5" will be generated as a PKB integer parameter with a min value 1 and max value 5
        # "range": "[1, 2, 3]" will be generated as a PKB list with one or more accepted values falling in that list
        # "range": "('a', 'b', 'c')" will be generated as a PKB enum with only a single accepted value from the specified enum
        "range": "Minimum to Maximum or list/enum",
        # [REQUIRED] The type is required, as all PKB flags need to have specific types. Accepted values are:
        # "type": "num" -> numeric, can be either integer or float
        # "type": "str" -> string
        # "type": "hex" -> string containing hexadecimal representation of a number
        # "type": "OS-num" -> numeric result of running an OS command
        # "type": "OS-str" -> string result of running an OS command
        # "type": "custom-num" -> numeric result of running a custom, user provided script
        # "type": "custom-str" -> string result of running a custom, user provided script
        "type": "Type of argument provided",
        # [REQUIRED IF TYPE IS A COMMAND] Skipped otherwise.
        # In case the type specified above is any type of command ("OS-num", "OS-str", "custom-num", "custom-str"), it is
        # obvious that we need to know where to run that command.
        # The "target" complies with the previous explanations and is supposed to be a single vm in this case, as
        # it is needed to be run only once, to determine the default value of the knob.
        "target_default": "individual vm where to run the command to retrieve the knob default",
        # [Optional] If null string or missing, set the default PKB flag value to "None". Examples:
        # "default": "5" -> numerical default value equal to 5 (type needs to be "num" above)
        # "default": "3.14" -> numerical default value equal to 3.14 (type needs to be "num" above)
        # "default": "xyz" -> string default value equal to xyz (type needs to be "str" above)
        # "default": "0xFF" -> numerical default value equal to 255 (type needs to be "hex" above)
        # "default": "nproc" -> numerical default value equal to the output of the `nproc` command run on
        #                      -> the SUT (type needs to be "OS-num" above)
        # "default": "uname -r" -> string default value equal to the output of the `uname -r` command run on
        #                         -> the SUT (type needs to be "OS-str" above)
        # "default": "./get_num_memory_DIMMs" -> numerical default value equal to the output obtained after
        #                                     -> running the custom script "get_num_memory_DIMMs" (which needs to be
        #                                     -> provided alongside the template) on the SUT. Type needs to be "custom-num" above.
        "default": "Can be numbers, string, hex, OS or custom cmd",
        # [Optional] If not provided or null string, ignore.
        #
        # This command does not produce output. Its only role is to propagate the value of
        # the knob to the right place, so that the benchmark will leverage the new knob
        # value (e.g. change a Mysql config entry in /etc/mysql/my.cnf, then restart the
        # service). Setcmd will take as parameter either the default, or a user-provided value.
        #
        # As with the custom scripts mentioned before, if it begins with "./", it means a
        # custom script is executed (and needs to be provided alongside the template).
        "setcmd": "command or script or API call to set the configuration value",
        # REQUIRED IF SETCMD IS PROVIDED. Skipped otherwise.
        # In case setcmd is provided, we need to know where to run that command to set the knob value.
        # The "target" complies with the previous explanations and can be a single vm or entire vm_group
        "target_setcmd": "vm or vm_group where to run setcmd to set the knob value"
      },
      {
        "name": "knob2 name",
        "description": "Description of the knob"
        "range": "Minimum to Maximum or list",
        "type": "Any of the types described above",
        "target_default": "Target vm to run the default command, if type is cmd",
        "default": "Can be numbers, string, hex, OS or custom cmd",
        "setcmd": "command or script or API call to set the configuration value",
        "target_setcmd": "vm or vm_group where to run setcmd to set the knob value"
      }
    ], # end of config list
    #[OPTIONAL] These are similar to config flags but they are specifically used to tune the workload to analyse the performance.
    "tunable_flags": [
      {
        # [REQUIRED] The name will become a PKB flag, using the format workload1_knob1_name.
        # Any blank spaces in the name will be replaced by "_".
        #
        # Any "key":"value" pair defined in the template can be referenced as
        # $key
        # throughout the template.
        #
        "name": "knob1_name",
        # [OPTIONAL, but highly recommended] If not blank, the description will serve as a description for the PKB flag
        "description": "Description of the knob",
        # [OPTIONAL] Ignore if null string. Accepted format:
        # "range": "1-5" will be generated as a PKB integer parameter with a min value 1 and max value 5
        # "range": "[1, 2, 3]" will be generated as a PKB list with one or more accepted values falling in that list
        # "range": "('a', 'b', 'c')" will be generated as a PKB enum with only a single accepted value from the specified enum
        "range": "Minimum to Maximum or list/enum",
        # [REQUIRED] The type is required, as all PKB flags need to have specific types. Accepted values are:
        # "type": "num" -> numeric, can be either integer or float
        # "type": "str" -> string
        # "type": "hex" -> string containing hexadecimal representation of a number
        # "type": "OS-num" -> numeric result of running an OS command
        # "type": "OS-str" -> string result of running an OS command
        # "type": "custom-num" -> numeric result of running a custom, user provided script
        # "type": "custom-str" -> string result of running a custom, user provided script
        "type": "Type of argument provided",
        # [REQUIRED IF TYPE IS A COMMAND] Skipped otherwise.
        # In case the type specified above is any type of command ("OS-num", "OS-str", "custom-num", "custom-str"), it is
        # obvious that we need to know where to run that command.
        # The "target" complies with the previous explanations and is supposed to be a single vm in this case, as
        # it is needed to be run only once, to determine the default value of the knob.
        "target_default": "individual vm where to run the command to retrieve the knob default",
        # [Optional] If null string or missing, set the default PKB flag value to "None". Examples:
        # "default": "5" -> numerical default value equal to 5 (type needs to be "num" above)
        # "default": "3.14" -> numerical default value equal to 3.14 (type needs to be "num" above)
        # "default": "xyz" -> string default value equal to xyz (type needs to be "str" above)
        # "default": "0xFF" -> numerical default value equal to 255 (type needs to be "hex" above)
        # "default": "nproc" -> numerical default value equal to the output of the `nproc` command run on
        #                      -> the SUT (type needs to be "OS-num" above)
        # "default": "uname -r" -> string default value equal to the output of the `uname -r` command run on
        #                         -> the SUT (type needs to be "OS-str" above)
        # "default": "./get_num_memory_DIMMs" -> numerical default value equal to the output obtained after
        #                                     -> running the custom script "get_num_memory_DIMMs" (which needs to be
        #                                     -> provided alongside the template) on the SUT. Type needs to be "custom-num" above.
        "default": "Can be numbers, string, hex, OS or custom cmd",
        # [Optional] If not provided or null string, ignore.
        #
        # This command does not produce output. Its only role is to propagate the value of
        # the knob to the right place, so that the benchmark will leverage the new knob
        # value (e.g. change a Mysql config entry in /etc/mysql/my.cnf, then restart the
        # service). Setcmd will take as parameter either the default, or a user-provided value.
        #
        # As with the custom scripts mentioned before, if it begins with "./", it means a
        # custom script is executed (and needs to be provided alongside the template).
        "setcmd": "command or script or API call to set the configuration value",
        # REQUIRED IF SETCMD IS PROVIDED. Skipped otherwise.
        # In case setcmd is provided, we need to know where to run that command to set the knob value.
        # The "target" complies with the previous explanations and can be a single vm or entire vm_group
        "target_setcmd": "vm or vm_group where to run setcmd to set the knob value"
      },
      {
        "name": "knob2 name",
        "description": "Description of the knob"
        "range": "Minimum to Maximum or list",
        "type": "Any of the types described above",
        "target_default": "Target vm to run the default command, if type is cmd",
        "default": "Can be numbers, string, hex, OS or custom cmd",
        "setcmd": "command or script or API call to set the configuration value",
        "target_setcmd": "vm or vm_group where to run setcmd to set the knob value"
      }
    ], # end of config list
    # [OPTIONAL] If specified (and not null string), the $output_dir directory on the SUT
    # will be copied in the PKB results dir on the PKB host, under /tmp/perfkitbenchmarker/runs/<run_uri>/
    "output_dir": {
        # [REQUIRED] Specifies the path on the SUT that will be copied to the results folder on the PKB host
        "path": "path/to/output/dir/on/SUT",
        # [REQUIRED] The "target" complies with the previous explanations and should be a single vm
        # from which to retrieve WL results
        "target": "single vm from where to retrieve WL results folder"
    },
    # [REQUIRED] At least one metric needs to be defined
    "metric": [
      {
        # [REQUIRED] String containing the metric name
        "name": "metric1 name",
        # [REQUIRED] Will be reported as-is by PKB
        "goal": "Increase or Decrease or Sustain",
        # [REQUIRED] The command below can be either an OS command or a
        # custom command, in which case it will start with "./". All
        # previous comments regarding custom commands are applicable here.
        "read": "command to read/access the metric or score",
        # [REQUIRED] The "target" complies with the previous explanations and should be a single vm which determines
        # where the read command specified above should be run
        "target": "single vm where to run read command in order to retrieve the WL metric",
        # [REQUIRED] Reported as-is by PKB
        "unit": "Measure unit"
      },
      {
        "name": "metric2 name",
        "goal": "Increase or Decrease or Sustain",
        "read": "command to read/access the metric or score",
        "target": "single vm where to run read command in order to retrieve the WL metric",
        "unit": "Measure unit"
      }
    ], # end of metric list
    # [OPTIONAL, but recommended] Same as the install logic, the cleanup is performed in steps,
    # to make sure that, for multinode workloads, the cleanup sequence is respected (e.g. maybe
    # the steps on the worker nodes depend on something being run first on the master node).
    # There can be as many steps as needed, the example below shows 2 steps.
    "cleanup": [
      {
        # [REQUIRED] target can be either an entire vm_group, or a single vm within a vm_group
        # "target": "vm_group1" -> run this cleanup step in parallel on all vms in vm_group1
        # "target": "vm_group2[0]" -> run this cleanup step only on vm with index 0 in vm_group2
        "target": "vm_group or individual vm where to run this cleanup step",
        # [REQUIRED] cleanup command to run
        "cmd": "command to perform first step of cleanup"
      },
      {
        "target": "vm_group or individual vm where to run this cleanup step",
        "cmd": "command to perform second step of cleanup"
      }
    ] # end of "cleanup" list
  } # end of "workload1"
} # end of json
```

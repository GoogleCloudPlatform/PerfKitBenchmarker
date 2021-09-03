# dummy benchmark

## How to run on AWS

- Runing on Ubuntu

```./pkb.py --cloud=AWS --benchmarks=dummy --machine_type=m5.2xlarge --os_type=ubuntu2004```

- Runing on Centos

```./pkb.py --cloud=AWS --benchmarks=dummy --machine_type=m5.2xlarge --os_type=centos7```

## dummy specific flag values

### Benchmark flags values

### Configuration flags


- `dummy_version=[v1.0]`: Set Version of the Workload

### Tunable Flags


- `dummy_set_hugepages=[100]`: Set Hugepages to desired number

### Package flags values


- `dummy_deps_ubuntu_make_vm_group1_ver=["4.0"]`: Version of make package on "ubuntu2004" OS

- `dummy_deps_centos_make_vm_group1_ver=["4.0"]`: Version of make package on "centos7" OS

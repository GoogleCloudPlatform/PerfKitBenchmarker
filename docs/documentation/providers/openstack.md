# OpenStack

## Install OpenStack CLI client and setup authentication
---
Install OpenStack CLI utilities via the following command:

```bash
  sudo pip install -r perfkitbenchmarker/providers/openstack/requirements.txt
```

To setup credentials and endpoint information simply set the environment
variables using an OpenStack RC file. For help, see [`OpenStack` docs](http://docs.openstack.org/cli-reference/common/cli_set_environment_variables_using_openstack_rc.html)

## Running a Single Benchmark
---

### Example run on OpenStack

```bash
  python pkb.py --cloud=OpenStack --machine_type=m1.medium \
                --openstack_network=private --benchmarks=iperf
```

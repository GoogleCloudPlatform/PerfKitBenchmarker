# Juju

## Running Benchmarks with Juju

[Juju](https://jujucharms.com/) is a service orchestration tool that enables you
to quickly model, configure, deploy and manage entire cloud environments.
Supported benchmarks will deploy a Juju-modeled service automatically, with no
extra user configuration required, by specifying the `--os_type=juju` flag.

### Example
```bash
  python pkb.py --cloud=AWS --os_type=juju --benchmarks=cassandra_stress
```

## Benchmark support
---
Benchmark/Package authors need to implement the `JujuInstall()` method inside
their package. This method deploys, configures, and relates the services to be
benchmarked. Please note that other software installation and configuration
should be bypassed when `FLAGS.os_type == JUJU`. See `perfkitbenchmarker/linux_packages/cassandra.py` for an example implementation.

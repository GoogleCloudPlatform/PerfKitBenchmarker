# Contributing

## How to Extend PerfKit Benchmarker
---
First start with the [CONTRIBUTING.md](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/blob/master/CONTRIBUTING.md)
file.  It has the basics on how to work with PerfKitBenchmarker, and how to submit your pull requests.

In addition to the [CONTRIBUTING.md](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/blob/master/CONTRIBUTING.md)
file we have added a lot of comments into the code to make it easy to:

* Add new benchmarks (e.g.: `--benchmarks=<new benchmark>`)
* Add new package/os type support (e.g.: `--os_type=<new os type>`)
* Add new providers (e.g.: `--cloud=<new provider>`)
* etc.

Even with lots of comments we make to support more detailed documentation.  You will find the documentation we have on the [wiki](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/wiki).  Missing documentation you want?  Start a page and/or open an [issue](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/issues) to get it added.

## Integration Testing
---
In addition to regular unit tests, which are run via
[`hooks/check-everything`](hooks/check-everything), PerfKit Benchmarker has
integration tests, which create actual cloud resources and take time and money
to run. For this reason, they will only run when the variable
`PERFKIT_INTEGRATION` is defined in the environment. The command

```bash
  tox -e integration
```

will run the integration tests. The integration tests depend on having installed
and configured all of the relevant cloud provider SDKs, and will fail if you
have not done so.

# Authoring PKB Benchmarks

## What is a PKB Benchmark?

A PKB benchmark produces reproducible performance samples based on a predefined
set of configurations. By managing the full lifecycle of cloud resources and
standardizing product features via the `BENCHMARK_CONFIG`, it ensures consistent
benchmarking methodology across multiple runs and different cloud platforms.

## Common PKB Setups

### One VM Setup

Many VM benchmarks are conceptually simple. They have but 1 VM provisioned,
install some packages and/or copy some files to that VM, run a classic benchmark
like SPECCPU, parse the output & return the parsed output as samples.

See
[example_vm_benchmark](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/blob/master/perfkitbenchmarker/linux_benchmarks/example_vm_benchmark.py)
for how to set up a benchmark that provision & use a VM.
See
[example_preprovisioned_benchmark](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/blob/master/perfkitbenchmarker/linux_benchmarks/example_preprovision_benchmark.py)
for how to load a static file from a preprovisioned GCS location onto a VM.

### Two VM Setup

Another common pattern has 2 VMs - one client & one server. The server is the
system under test while the client sends loads to that system. Netperf, and IAAS
self-managed database benchmarks follow this pattern.

### Resource + VM Setup

Instead of hosting server-under-test in a VM, the server-under-test may be a
cloud resource. This is the most common pattern for PAAS benchmarks, such as
SparkSQL, where a client VM sends load to a Platform resource.

See
[example_resource_benchmark](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/blob/master/perfkitbenchmarker/linux_benchmarks/example_resource_benchmark.py)
for how to provision & use the
[example_resource](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/blob/master/perfkitbenchmarker/resources/example_resource.py).

## Phases of a Benchmark

A PKB benchmark consists of 5 stages: provision, prepare, run, cleanup and
teardown, executed in this exact order.

As a benchmark author, one defines what the benchmark ought to do in each of the
stages. You do so by defining the following variables and functions.

### Provision

This stage provisions all the cloud resources that will be used for
benchmarking. To do so, the user specifies the `BENCHMARK_NAME`, and the
resources to set up in the `BENCHMARK_CONFIG`.

*   Variable: `BENCHMARK_NAME`
    *   Generally matches the filename without the `_benchmark.py` suffix.
*   Variable: `BENCHMARK_CONFIG`
    *   The set of resources to create for this benchmark by default. Uses yaml
        & can be overridden. See
        [here](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker?tab=readme-ov-file#configurations-and-configuration-overrides)
        for details.
*   Function: `GetConfig`
    *   Creates the final config, and applies some flag overrides into the
        config.
*   Function: `CheckPrerequisites`
    *   Optional. Validate flags or spec. Useful for early termination of an
        invalid config.

### Prepare

This stage prepares the resources for benchmarking. This includes actions such
as installing packages on VMs, upgrading software, setting up database
permissions, and pre-loading databases with data, to name a few.
Since the run stage can be timed and traced, any long-running process that is
not a part of the active result measurement should happen here.

*   Function: `Prepare`
    *   Any setup code to ready the resource(s) for benchmarking.

Instead of a single ‘Prepare’ method, you can split ‘Prepare’ into 3 functions,
‘PrepareSystem’, ‘InstallPackages’, and ‘StartServices’ for finer granularity.

### Run

This stage runs the benchmark. This stage is timed and can be traced.

*   Function: `Run`
    *   The main code of the benchmark. Returns a list of
        [Sample](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/blob/master/perfkitbenchmarker/sample.py)s
        as result.

### Cleanup

Release any licenses and clean up any non-resource objects. Most benchmarks can
pass on this step since packages are deleted when VMs are deleted. To trigger
this step, the following must be added in the earlier Prepare step.

```py
benchmark_spec.always_call_cleanup = True
```

*   Function: `Cleanup`
    *   Optional. Inverse of Prepare. Runs any code wanted in the `--cleanup`
        step.

### Teardown

Inverse of Provision. Deletes resources that were created during provisioning.

## Benchmarking Data

Some benchmarks come with their own data set, others come with source scripts to
run. These resources are placed in the /data directory.

Additions to /data directory are organized by benchmarks.

Large data sources and proprietary datasets cannot be added to the PKB source
code. In order to use large source data, the benchmark can point to a
proprietary GCS source using a preprovisioned data bucket. See above example for
using preprovisioned data in a benchmark.

## Providers vs Packages vs Benchmark

### Provider

The /provider directory is for cloud provider specific logic. These are logic
unique to a cloud provider. Some examples of provider code are GCP products, and
gcloud commands to interact with these products. This also means that any
logic/code that is provider-specific should reside in the /provider directory.
If you find yourself writing ‘if cloud == GCP’, then there is a good chance you
are not following best practices.

### Packages

Packages include both /linux_packages and /windows_packages directories.
Packages are universal ways to install the package in question on all providers
across different benchmarks. This ensures that benchmarking is done fairly.
Installation of packages MUST happen in the package file. Issuing of package
commands and parsing of package generated output can optionally be implemented
in the package file or benchmark file.

### Benchmark

Benchmarks are located in /linux_benchmarks and /windows_benchmark directories.
Benchmark files define the phases of a benchmark as described above.
Benchmark files should not contain provider specific code.

## Difference between PKB & Other Frameworks

PKB is a benchmarking framework which provisions resources & runs benchmarks
such as `netperf` or `speccpu`. The difference here is precisely the resource
provisioning - each PKB run generally spins up & deletes its own Cloud
resources, while traditional benchmarks run on a set of already provisioned
resources. This has a couple of consequences:

*   Some resources take a long time to create & some benchmarks take a long time
    to run. This is expected.
*   You can emulate a “run on static resources” setup by spinning up resources
    with a first run specifying `--run_stage=provision,prepare,run` (omitting
    teardown) & a second specifying `--run_stage=run --run_uri=[urifromrun1]`.
    This mostly works but is not 100% supported - most benchmarks will fail to
    clean up their resources/files.

### Configuration Suggestions

*   One of the strengths of the above “set up & tear down resources every time”
    approach is very reproducible & side-effect free results. Don’t throw this
    strength away by trying to do too much in one benchmark. For example, don’t
    have a benchmark run some workload, modify a variable, & then rerun that
    workload & compare results. Instead add said variable as a flag/config
    option to PKB & run PKB twice with it set to value A & then set to value B.
*   Beware of the effect running benchmarks can have on your resource.
    Especially for eg resources with some amount of autoscaling or caching, the
    first test you run on a resource may perform quite a bit worse than the
    second. For this reason we sometimes run a “warm up test” & then record the
    results of subsequent tests.
*   Means, medians, & percentiles are great - both within a benchmark & over
    multiple runs! Within a benchmark, see
    [sample.py](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/blob/master/perfkitbenchmarker/sample.py)
    for a helper calculating percentiles, or use numpy or pandas which are
    already imported in
    [requirements.txt](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/blob/master/requirements.txt).
    Over multiple benchmarks, we like to get data from a few runs, especially to
    observe variance. It can be difficult to draw any conclusion if the
    performance of the benchmark/resource varies significantly between runs.
*   When trying out different configurations & scales, prefer smaller scale
    tests. Smaller scale tests are faster & cheaper to execute and easier to
    develop, but often have the same performance characteristics as larger
    tests. ie if configuration A scales to 10, 100, & 500 QPS better than
    configuration B, it will likely scale to 1000 & 10000 QPS better as well.
    This is obviously not always the case, but when possible prefer the smaller
    scale.

## Coding Preferences

*   There are a million ways to do things (CLI, libraries, command 1 vs 2,
    etc..). When possible, choose the way closest to how PKB is already doing
    it. There may be some small differences, but they tend not to be that large
    compared to e.g. the different providers or resources. And it greatly
    simplifies PKB’s codebase, avoiding repetition & redundant code.
*   Prefer NOT to add additional python libraries or tool installations to the
    runner VM (the one running PKB) when possible. Instead, utilize the above
    “client VM” approach & install tools or run data scripts from the client VM.
    This is what most of the code in the
    [linux_packages](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/tree/master/perfkitbenchmarker/linux_packages)
    directory does - installs various linux packages on the client VM. The use
    of the client VMs helps us keep PKB’s
    [requirements.txt](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/blob/master/requirements.txt)
    dependency graph from exploding. There can be exceptions but you will be
    asked to justify the addition.
*   When handling errors, prefer to be brittle & fail fast rather than silently
    swallowing errors. A failure is loud & can be easily detected, alerted on, &
    fixed, while silent failures look like successes but might hide real
    problems. Some error handling is useful, especially to enable retries.
*   Prefer small and incremental changes over one big change. In general, a pull
    request should contain one self-contained change. This means that the pull
    request should make a minimal change that addresses **just one thing**. This
    is usually just one part of a feature, rather than a whole feature at once.
    For example, to add new benchmarks, the first pull request is the skeletal
    benchmark with `BENCHMARK_NAME`, and `BENCHMARK_CONFIG` defined, but `pass`
    and `return []` for everything else. The second pull request adds the
    `Prepare` function, the third adds partials of `Run` function, the fourth
    adds the parsing of results etc.
*   Add related unit tests.
*   Avoid code duplication.

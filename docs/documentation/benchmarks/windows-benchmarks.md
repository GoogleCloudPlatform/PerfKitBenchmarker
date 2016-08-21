# Running Windows Benchmarks

You must be running on a Windows machine in order to run Windows benchmarks.
Install all dependencies as above and set TrustedHosts to accept all hosts so
that you can open PowerShell sessions with the VMs (both machines having each
other in their TrustedHosts list is neccessary, but not sufficient to issue
remote commands; valid credentials are still required):

```
set-item wsman:\localhost\Client\TrustedHosts -value *
```

Now you can run Windows benchmarks by running with `--os_type=windows`. Windows has a
different set of benchmarks than Linux does. They can be found under
perfkitbenchmarker/windows_benchmarks/. The target VM OS is Windows Server 2012
R2.

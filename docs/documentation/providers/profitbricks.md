# ProfitBricks

## ProfitBricks configuration and credentials
---
Get started by running:
```bash
  sudo pip install -r perfkitbenchmarker/providers/profitbricks/requirements.txt
```

PerfKit Benchmarker uses the
<a href='http://docs.python-requests.org/en/master/'>Requests</a> module
to interact with ProfitBricks' REST API. HTTP Basic authentication is used
to authorize access to the API. Please set this up as follows:

Create a configuration file containing the email address and password
associated with your ProfitBricks account, separated by a colon.
Example:

```bash
less ~/.config/profitbricks-auth.cfg
email:password
```

The PerfKit Benchmarker will automatically base64 encode your credentials
before making any calls to the REST API.

PerfKit Benchmarker uses the file location `~/.config/profitbricks-auth.cfg`
by default. You can use the `--profitbricks_config` flag to
override the path.

## Running a Single Benchmark
---
### Example run on ProfitBricks

```bash
python pkb.py --cloud=ProfitBricks --machine_type=Small --benchmarks=iperf
```

# Digital Ocean

## DigitalOcean configuration and credentials
---
PerfKitBenchmarker uses the *curl* tool to interact with
DigitalOcean's REST API. This API uses oauth for authentication.
Please set this up as follows:

Log in to your DigitalOcean account and create a Personal Access Token
for use by PerfKitBenchmarker with read/write access in Settings /
API: https://cloud.digitalocean.com/settings/applications

Save a copy of the authentication token it shows, this is a
64-character hex string.

Create a curl configuration file containing the needed authorization
header. The double quotes are required. Example:

```sh
  cat > ~/.config/digitalocean-oauth.curl
  header = "Authorization: Bearer 9876543210fedc...ba98765432"
  ^D
```

Confirm that the authentication works:

```bash
  curl --config ~/.config/digitalocean-oauth.curl https://api.digitalocean.com/v2/sizes
  {"sizes":[{"slug":"512mb","memory":512,"vcpus":1, ... }]}
```

PerfKitBenchmarker uses the file location `~/.config/digitalocean-oauth.curl`
by default, you can use the `--digitalocean_curl_config` flag to
override the path.

## Running a Single Benchmark
---
### Example run on DigitalOcean

```bash
  python pkb.py --cloud=DigitalOcean --machine_type=16gb --benchmarks=iperf
```

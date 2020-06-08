# CloudStack

## Install `csapi` and set the API keys
---
```bash
  sudo pip install csapi
```

Get the API key and SECRET from Cloudstack. Set the following environement variables.

```bash
  export CS_API_URL=<insert API endpoint>
  export CS_API_KEY=<insert API key>
  export CS_API_SECRET=<insert API secret>
```

## Running a Single Benchmark
---
### Example run on CloudStack
Specify the network offering when running the benchmark. If using VPC
(`--cs_use_vpc`), also specify the VPC offering (`--cs_vpc_offering`).

```bash
  python pkb.py --cloud=CloudStack --benchmarks=ping \
                --cs_network_offering=DefaultNetworkOffering
```

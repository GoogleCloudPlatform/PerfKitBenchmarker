# Microsoft Azure

## Windows Azure CLI and credentials
You first need to install node.js and NPM.
This version of Perfkit Benchmarker is compatible with azure version 0.9.9.

Go [here](https://nodejs.org/download/), and follow the setup instructions.

Next, run the following (omit the `sudo` on Windows):

```bash
  sudo npm install azure-cli@0.9.9 -g
  azure account download
```

Read the output of the previous command. It will contain a webpage URL. Open that in a browser. It will download
a file (`.publishsettings`) file. Copy to the folder you're running PerfKit Benchmarker. In my case the file was called
`Free Trial-7-18-2014-credentials.publishsettings`

```bash
  azure account import [path to .publishsettings file]
```

Test that azure is installed correctly
```bash
  azure vm list
```

## Running a Single Benchmark
---

### Example run on Azure
```bash
  python pkb.py --cloud=Azure --machine_type=ExtraSmall --benchmarks=iperf
```

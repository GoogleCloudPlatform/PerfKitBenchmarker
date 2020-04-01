### Coremark Script

-   Tested to run from Google Compute Engine VMs running Ubuntu 1604 or
    Ubuntu 1804.

-   GCP Project authentication is required.

    -   Recommended: `gcloud auth activate-service-account --key-file
        <YOUR_SERVICE_ACCOUNT_KEY>`

    -   See https://cloud.google.com/sdk/ for how to use gcloud.

-   `source setup.sh` sets up Perfkit Benchmarer (PKB) from github using Python
    3 and virtualenv.

-   `bash coremark.sh` runs PKB. It provisions an GCE VM, installs Coremark,
    runs the benchmark 5 times, tears down the machine, and prints the Coremark
    results.

    -   You may run the the PKB command under different zones and machine types.

    -   You may adjust the number of Coremark iterations per run (currently set
        to 5) and the `os_type` to use `windows2019_core` instead of
        `ubuntu1804`.

    -   The --gcp_min_cpu_platform=skylake flag is only supported for the N1
        machine family. To run on other machine families (e.g. C2, N2, E2),
        remove the flag.

    -   Running Coremark this way should take around 30 minutes.

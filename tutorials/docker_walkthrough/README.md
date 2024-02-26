# Running PerfKit Benchmarker using Docker

This tutorial outlines a few methods for running PerfKit Benchmarker (PKB) on
Google Cloud using Docker, in increasing amounts of complexity.

## Options

Here are the options, described in more detail in their sections:

-   [Cloud Shell](#run-via-google-cloud-shell)
-   [Local Docker](#run-via-local-docker)
-   [Cloud Run Jobs](#run-via-cloud-run-jobs)
-   [Local Install](#run-via-local-install)

## Run via Google Cloud Shell

1.  In a Google Cloud Project, launch the cloud shell terminal

![alt text](../beginner_walkthrough/img/cloud_shell_icon.png "Cloud Shell Icon")

[Cloud Shell](https://cloud.google.com/shell) is an interactive terminal running
on some VM, with (optionally, see "ephemeral mode") persistent disk for your
project.

1.  In terminal, run: `docker pull \
    us-east1-docker.pkg.dev/p3rf-gke/public/pkb-for-gcp:latest`

    This pulls an image based off this [Dockerfile](pkb_for_gcp/Dockerfile)
    which installs pkb & some dependencies onto a clean Ubuntu image.

1.  Run: `docker run -it us-east1-docker.pkg.dev/p3rf-gke/public/pkb-for-gcp \
    --project=your-project --benchmarks=iperf --machine_type=f1-micro`

    This runs PKB with flags for the iperf benchmark in the docker image &
    outputs results to the console.

### Viability of Cloud Shell

While great for this tutorial, Cloud Shell limits runs to an hour and is
primarily meant for active development. Many benchmarks won't finish running in
that time, so consider using the below two options instead.

## Run via local Docker

The above commands will also work on your local machine or any Linux environment
with docker installed. Docker provides isolation for PKB, so it will work
regardless of what version of python you have installed locally.

There are a couple of additional flags needed as compared to Cloud Shell:

-   Pass in your local GCP credentials using `-v ~/.config:/root/.config` (see
    [stackoverflow](https://stackoverflow.com/questions/38938216/pass-google-default-application-credentials-in-local-docker-run)).
-   Pass `-v "$(pwd)"/pkb-results:/tmp/perfkitbenchmarker`, which maps pkb's
    output directory (/tmp/perfkitbenchmarker/) to a local folder on the host
    (./pkb-results) so you can access those files later. The output directory
    stores the sdout and the results as a json.

That full command looks like: `docker run -v ~/.config:/root/.config -v
"$(pwd)"/pkb-results:/tmp/perfkitbenchmarker -it
us-east1-docker.pkg.dev/p3rf-gke/public/pkb-for-gcp --project=your-project
--benchmarks=iperf --machine_type=f1-micro`

## Run via Cloud Run Jobs

Rather than run via local docker, you can also run in the cloud via Cloud Run
Jobs. This requires a fair bit more setup though, basically following
[this quickstart](https://cloud.google.com/run/docs/quickstarts/jobs/create-execute):

1.  Run: gcloud iam service-accounts create cloud-run-pkb --display-name="PKB
    Service Account"

    This sets up a service account which Cloud Run will need for permissions.

1.  Run: `gcloud iam service-accounts``

    This prints all the service accounts for your project; copy the new service
    account's email for the next command

1.  Run: `gcloud projects add-iam-policy-binding your-project
    --member="serviceAccount:cloud-run-pkb@your-project.iam.gserviceaccount.com"
    --role roles/editor`

    This gives the service account overly broad permissions so PKB can run. PKB
    creates & deletes temporary resources for most benchmarks, so editor level
    permissions are needed on any services being tested.

1.  Run: `gcloud run jobs deploy pkb-gcp --image
    us-east1-docker.pkg.dev/p3rf-gke/public/pkb-for-gcp --task-timeout 24h
    --service-account=cloud-run-pkb@your-project.iam.gserviceaccount.com
    --region=us-east1`

    This creates a Cloud Run Job with the same docker image we were using
    earlier. See [create docs](https://cloud.google.com/run/docs/create-jobs)
    for more details.

1.  Run: `gcloud run jobs execute pkb-gcp
    --args="--project=your-project,--benchmarks=iperf,--machine_type=f1-micro"
    --region=us-east1`

    This runs PKB with the iperf benchmark, using the same args as earlier but
    now wrapped in
    [quotation marks](https://cloud.google.com/run/docs/configuring/services/containers#equals-commas-in-args).

    The command output gives a link to the running job, which also has a UI for
    viewing the logs ([example screenshot](imgs/run_logs.png)). That's pretty
    much it! If you'd still like access to results as a json, you can pass the
    additional argument to PKB (still wrapped in quotes) of
    `--cloud_storage_bucket=my-pkb-storage-bucket`.

1.  Optionally, clean up your project after you're done with `gcloud run jobs
    delete pkb-gcp --region=us-east1` Even if you don't delete it, you won't be
    charged anything if you're not using it, as the job automatically scales
    down to 0 instances when not in use.

## Run via Local Install

Of course you can also install PKB locally & run it on your local machine.
There's a good tutorial for that flow in
[beginner_walkthrough](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/tree/master/tutorials/beginner_walkthrough)

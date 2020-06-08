# Object Storage Benchmark

## Create and Configure a .boto file for object storage benchmarks
---
In order to run object storage benchmark tests, you need to have a properly configured `~/.boto` file.

Here is how:

* Create the `~/.boto` file (If you already have ~/.boto, you can skip this step. Consider making a backup copy of your existing .boto file.)

To create a new `~/.boto` file, issue the following command and follow the instructions given by this command:
```bash
  gsutil config
```
As a result, a `.boto` file is created under your home directory.

* Open the `.boto` file and edit the following fields:
1. In the [Credentials] section:

`gs_oauth2_refresh_token`: set it to be the same as the `refresh_token` field in your gcloud credential file (~/.config/gcloud/credentials), which was setup as part of the `gcloud auth login` step.

`aws_access_key_id`, `aws_secret_access_key`: set these to be the AWS access keys you intend to use for these tests, or you can use the same keys as those in your existing AWS credentials file (`~/.aws/credentials`).

2. In the `[GSUtil]` section:

`default_project_id`: if it is not already set, set it to be the google cloud storage project ID you intend to use for this test. (If you used `gsutil config` to generate the `.boto` file, you should have been prompted to supply this information at this step).

3. In the [OAuth2] section:
`client_id`, `client_secret`: set these to be the same as those in your gcloud credentials file (`~/.config/gcloud/credentials`), which was setup as part of the 'gcloud auth login' step.

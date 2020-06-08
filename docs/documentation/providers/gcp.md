# Google Cloud Platform

## Install gcloud and setup authentication
---
Instructions: [https://developers.google.com/cloud/sdk/](https://developers.google.com/cloud/sdk/)

If you're using OS X or Linux you can run the command below.

When prompted pick the local folder, then Python project, then the defaults for all the rest
```bash
  curl https://sdk.cloud.google.com | bash
```
Restart your shell window (or logout/ssh again if running on a VM)

On Windows, visit the same page and follow the Windows installation instructions on the page.

Set your credentials up: [https://developers.google.com/cloud/sdk/gcloud/#gcloud.auth](https://developers.google.com/cloud/sdk/gcloud/#gcloud.auth)

 Run the command below.
It will print a web page URL. Navigate there, authorize the gcloud instance you just installed to use the services
it lists, copy the access token and give it to the shell prompt.
```
  gcloud auth login
```

You will need a project ID before you can run. Please navigate to
[https://console.developers.google.com](https://console.developers.google.com) and create one.

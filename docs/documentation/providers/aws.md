# Amazon Web Services

## Install AWS CLI and setup authentication
Make sure you have installed pip (see the section above).

Follow instructions at http://aws.amazon.com/cli/ or run the following command (omit the 'sudo' on Windows)

```bash
  sudo pip install awscli
```
Navigate to the AWS console to create access credentials: https://console.aws.amazon.com/ec2/
* On the console click on your name (top left)
* Click on "Security Credentials"
* Click on "Access Keys", the create New Access Key. Download the file, it contains the Access key and Secret keys
  to access services. Note the values and delete the file.

Configure the CLI using the keys from the previous step

```bash
  aws configure
```

## Running a Single Benchmark
---

### Example run on AWS
```bash
  python pkb.py --cloud=AWS --benchmarks=iperf --machine_type=t1.micro
```

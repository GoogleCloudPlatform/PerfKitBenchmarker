# AliCloud

## Install AliCloud CLI and setup authentication
---
Make sure you have installed pip (see the section above).

Run the following command to install aliyuncli(omit the ‘sudo’ on Windows)

1\. Install python development tools:
In Debian or Ubuntu:
```bash
  sudo apt-get install -y python-dev
```
In CentOS:
```bash
  sudo yum install python-devel
```
2\. Install aliyuncli tool and python SDK for ECS:
```bash
  sudo pip install aliyuncli
```
To check if AliCloud is installed:
```bash
  aliyuncli --help
```
Install python SDK for ECS:
```bash
  sudo pip install aliyun-python-sdk-ecs
```
Check if aliyuncli ecs command is ready:
```bash
  aliyuncli ecs help
```
If you see the "usage" message, you should follow step 3.
Otherwise, jump to step 4.

3\. Dealing with an exception when it runs on some specific version of Ubuntu
Get the python lib path: `/usr/lib/python2.7/dist-packages`
```python
$ python
> from distutils.sysconfig import get_python_lib
> get_python_lib()
'/usr/lib/python2.7/dist-packages'
```
Copy to the right directory(for the python version 2.7.X):
```bash
  sudo cp -r /usr/local/lib/python2.7/dist-packages/aliyun* /usr/lib/python2.7/dist-packages/
```
Check again:
```bash
  aliyuncli ecs help
```

4\. Navigate to the AliCloud console to create access credentials:
 [https://home.console.aliyun.com/#/](https://home.console.aliyun.com/#/)
 * Login first
 * Click on "AccessKeys" (top right)
 * Click on "Create Access Key", copy and store the "Access Key ID" and "Access Key Secret" to a safe place.

 Configure the CLI using the Access Key ID and Access Key Secret from the previous step
```bash
  aliyuncli configure
```

## Running a Single Benchmark
---

### Example run on AliCloud
```bash
  python pkb.py --cloud=AliCloud --machine_type=ecs.s2.large --benchmarks=iperf
```

# Apache Mesos

## Mesos configuration
---
Mesos provider communicates with Marathon framework in order to manage Docker instances. Thus it is required to setup Marathon framework along with the Mesos cluster. In order to connect to Mesos you need to provide IP address and port to Marathon framework using `--marathon_address` flag.

Provider has been tested with Mesos v0.24.1 and Marathon v0.11.1.

**Overlay network**  
Mesos on its own doesn't provide any solution for overlay networking. You need to configure your cluster so that the instances will live in the same network. For this purpose you may use Flannel, Calico, Weave, etc.

**Mesos cluster configuration**  
Make sure your Mesos-slave nodes are reachable (by hostname) from the machine which is used to run the benchmarks. In case they are not, edit the `/etc/hosts` file appropriately.

**Image prerequisites**  
Please refer to the [Image prerequisites for Docker based clouds](../image-prereqs-docker-based-clouds/).

## Running a Single Benchmark
---
### Example run on Mesos

```bash
  python pkb.py --cloud=Mesos --benchmarks=iperf \
                --marathon_address=localhost:8080 --image=image-with-ssh-server
```

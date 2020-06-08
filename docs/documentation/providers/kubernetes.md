# Kubernetes

## Kubernetes configuration and credentials
---
Perfkit uses `kubectl` binary in order to communicate with Kubernetes cluster - you need to pass the path to `kubectl` binary using `--kubectl` flag. It's recommended to use version 1.0.1 (available to download here: [https://storage.googleapis.com/kubernetes-release/release/v1.0.1/bin/linux/amd64/kubectl](https://storage.googleapis.com/kubernetes-release/release/v1.0.1/bin/linux/amd64/kubectl)).
Authentication to Kubernetes cluster is done via `kubeconfig` file ([https://github.com/kubernetes/kubernetes/blob/release-1.0/docs/user-guide/kubeconfig-file.md](https://github.com/kubernetes/kubernetes/blob/release-1.0/docs/user-guide/kubeconfig-file.md)). Its path is passed using `--kubeconfig` flag.

**Image prerequisites**  

Docker instances by default don't allow to SSH into them. Thus it is important to configure your Docker image so that it has SSH server installed. You can use your own image or build a new one based on a Dockerfile placed in tools/docker_images directory - in this case please refer to [https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/tree/master/tools/docker_images](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/tree/master/tools/docker_images) document.

**Kubernetes cluster configuration**  
If your Kubernetes cluster is running on CoreOS:

1\. Fix `$PATH` environment variable so that the appropriate binaries can be found:
```bash
  sudo mkdir /etc/systemd/system/kubelet.service.d
  sudo vim /etc/systemd/system/kubelet.service.d/10-env.conf
```
2\. Add the following line to `[Service]` section:
```bash
  Environment=PATH=/opt/bin:/usr/bin:/usr/sbin:$PATH
```
3\. Reboot the node:
```bash
  sudo reboot
```

Note that some benchmark require to run within a privileged container. By default Kubernetes doesn't allow to schedule Dockers in privileged mode - you have to add `--allow-privileged=true` flag to `kube-apiserver` and each `kubelet` startup commands.

**Ceph integration**  
When you run benchmarks with standard scratch disk type (`--scratch_disk_type=standard` - which is a default option), Ceph storage will be used. There are some configuration steps you need to follow before you will be able to spawn Kubernetes PODs with Ceph volume. On each of Kubernetes-Nodes and on the machine which is running Perfkit benchmarks do the following:

1\. Copy `/etc/ceph` directory from Ceph-host  
2\. Install `ceph-common` package so that `rbd` command is available

* If your Kubernetes cluster is running on CoreOS, then you need to create a bash script called `rbd` which will run `rbd` command inside a Docker container:

```bash
    #!/usr/bin/bash
    /usr/bin/docker run -v /etc/ceph:/etc/ceph -v /dev:/dev -v /sys:/sys \
                  --net=host --privileged=true --rm=true ceph/rbd $@
```

Save the file as 'rbd'. Then:
```bash
  chmod +x rbd
  sudo mkdir /opt/bin
  sudo cp rbd /opt/bin
```

Install `rbdmap` (https://github.com/ceph/ceph-docker/tree/master/examples/coreos/rbdmap):
```bash
  git clone https://github.com/ceph/ceph-docker.git
  cd ceph-docker/examples/coreos/rbdmap/
  sudo mkdir /opt/sbin
  sudo cp rbdmap /opt/sbin
  sudo cp ceph-rbdnamer /opt/bin
  sudo cp 50-rbd.rules /etc/udev/rules.d
  sudo reboot
```



You have two Ceph authentication options available (http://kubernetes.io/v1.0/examples/rbd/README.html):

1. Keyring - pass the path to the keyring file using `--ceph_keyring` flag
2. Secret. In this case you have to create a secret first:

   Retrieve base64-encoded Ceph admin key:
```bash
   ceph auth get-key client.admin | base64
   QVFEYnpPWlZWWnJLQVJBQXdtNDZrUDlJUFo3OXdSenBVTUdYNHc9PQ==  
```

Create a file called `create_ceph_admin.yml` and replace the `key` value with the output from the previous command:

```{yaml}
  apiVersion: v1
  kind: Secret
  metadata:
    name: my-ceph-secret
  data:
    key: QVFEYnpPWlZWWnJLQVJBQXdtNDZrUDlJUFo3OXdSenBVTUdYNHc9PQ==
```

Add secret to Kubernetes:  
```bash
   kubectl create -f create_ceph_admin.yml
```
   You will have to pass the Secret name (using `--ceph_secret` flag) when running the benchmakrs. In this case it should be: `--ceph_secret=my-ceph-secret`.

## Running a Single Benchmark
---
### Example run on Kubernetes
```bash
python pkb.py --cloud=Kubernetes --benchmarks=iperf --kubectl=/path/to/kubectl \
              --kubeconfig=/path/to/kubeconfig --image=image-with-ssh-server \
              --ceph_monitors=10.20.30.40:6789,10.20.30.41:6789 \
              --kubernetes_nodes=10.20.30.42,10.20.30.43
```

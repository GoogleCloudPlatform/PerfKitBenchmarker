# Image prerequisites for Docker based clouds

## Docker Image requirements
Docker instances by default don't allow to SSH into them. Thus it is important
to configure your Docker image so that it has SSH server installed. You can use
your own image or build a new one based on a Dockerfile placed in
`tools/docker_images` directory - in this case please refer to
[Docker images document](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/tree/master/tools/docker_images).

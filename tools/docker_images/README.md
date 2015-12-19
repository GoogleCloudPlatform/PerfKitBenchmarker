# README

You may use a sample Dockerfile in order to build a Docker image with SSH server installed.

1. Build a docker image

   ```
   cd tools/docker_images
   docker build -t=ubuntu_ssh --force-rm=true .
   ```

2. Save the image and copy it to each of the slave nodes
   ```
   docker save -o=ubuntu_ssh.tar.gz ubuntu_ssh
   # scp ubuntu_ssh.tar.gz file to each of slave nodes
   ```

3. Load the image on each of the slave nodes
   ```
   docker load -i=ubuntu_ssh.tar.gz
   ```

4. The image is ready to be used by Perfkit:
   ```
   ./pkb.py --image=ubuntu_ssh ...
   ```

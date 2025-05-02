FROM ubuntu:latest

# Start from ubuntu for openssl & ssh-keygen
ARG DEBIAN_FRONTEND=noninteractive
RUN apt update
RUN apt-get -y install software-properties-common
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get -y install python3.11

RUN apt-get -y install git
RUN apt-get -y install python3-pip
RUN apt-get -y install python-is-python3

# Install gcloud
RUN apt-get -y install curl
RUN curl -sSL https://sdk.cloud.google.com > install.sh
RUN bash install.sh --disable-prompts --install-dir=/bin
ENV PATH=$PATH:/bin/google-cloud-sdk/bin

# Install pkb following instructions: https://github.com/GoogleCloudPlatform/PerfKitBenchmarker
ENV HOME /root
WORKDIR $HOME
RUN git clone https://github.com/GoogleCloudPlatform/PerfKitBenchmarker.git
RUN python3.11 -m pip install -r $HOME/PerfKitBenchmarker/requirements.txt

# Run PKB directly, example:
# docker run -v ~/.config:/root/.config -it us-east1-docker.pkg.dev/p3rf-gke/public/pkb-for-gcp --project=my-project --benchmarks=iperf --machine_type=f1-micro
ENTRYPOINT ["python3.11", "./PerfKitBenchmarker/pkb.py"]

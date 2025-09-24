FROM ubuntu:latest

ARG PYTHON_VERSION=3.12

# Start from ubuntu for openssl & ssh-keygen
ARG DEBIAN_FRONTEND=noninteractive

RUN apt update; apt install -y make build-essential libssl-dev \
    zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev curl git \
    libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev \
    liblzma-dev

# install python 3.12 and make default
ENV HOME="/root"
WORKDIR ${HOME}
RUN apt-get install -y git
RUN git clone --depth=1 https://github.com/pyenv/pyenv.git .pyenv
ENV PYENV_ROOT="${HOME}/.pyenv"
ENV PATH="${PYENV_ROOT}/shims:${PYENV_ROOT}/bin:${PATH}"
RUN pyenv install ${PYTHON_VERSION}
RUN pyenv global ${PYTHON_VERSION}

# Install gcloud
RUN curl -sSL https://sdk.cloud.google.com > install.sh
RUN bash install.sh --disable-prompts --install-dir=/bin
ENV PATH=$PATH:/bin/google-cloud-sdk/bin

# Install pkb following instructions: https://github.com/GoogleCloudPlatform/PerfKitBenchmarker
ENV HOME /root
WORKDIR $HOME
RUN git clone https://github.com/GoogleCloudPlatform/PerfKitBenchmarker.git
RUN python3 -m pip install -r $HOME/PerfKitBenchmarker/requirements.txt

# Run PKB directly, example:
# docker run -v ~/.config:/root/.config -it us-docker.pkg.dev/p3rf-gke/public/pkb-for-gcp --project=my-project --benchmarks=iperf --machine_type=f1-micro
ENTRYPOINT ["python3", "./PerfKitBenchmarker/pkb.py"]

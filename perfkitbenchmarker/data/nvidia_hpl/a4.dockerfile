FROM nvcr.io/nvidia/hpc-benchmarks:25.04

ENV NCCL_TUNER_CONFIG_PATH="/usr/local/gib/configs/tuner_config_a4.txtpb"
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /etc/apt/trusted.gpg.d/cloud.google.gpg
RUN echo 'deb https://packages.cloud.google.com/apt gpudirect-gib-apt main' | tee /etc/apt/sources.list.d/nccl-gib.list
RUN apt update
RUN apt install nccl-gib
ENV LD_LIBRARY_PATH=/usr/local/gib/lib64:\$LD_LIBRARY_PATH

ENV NCCL_DEBUG=INFO \
    NCCL_LIB_DIR=/usr/local/gib/scripts \
    OMPI_MCA_btl=tcp,self \
    OMPI_MCA_mtl=^ofi \
    OMPI_MCA_btl_tcp_if_include=enp0s19 \
    PMIX_MCA_gds=^ds12 \
    NCCL_SOCKET_IFNAME=enp0s19,enp192s20
ENV NCCL_NET=gIB
ENV NCCL_CROSS_NIC=0
ENV NCCL_NET_GDR_LEVEL=PIX
ENV NCCL_P2P_NET_CHUNKSIZE=131072
ENV NCCL_NVLS_CHUNKSIZE=524288
ENV NCCL_IB_ADAPTIVE_ROUTING=1
ENV NCCL_IB_QPS_PER_CONNECTION=4
ENV NCCL_IB_TC=52
ENV NCCL_IB_FIFO_TC=84

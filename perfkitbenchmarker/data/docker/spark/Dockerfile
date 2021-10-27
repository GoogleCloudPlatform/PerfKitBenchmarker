# tini is not available in 18
ARG base_image=ubuntu:20.04

FROM $base_image

# TODO(pclay): support java 11
ARG java_version=8
ENV JAVA_HOME=/usr/lib/jvm/java-$java_version-openjdk-amd64

ARG hadoop_version=3.3.1
# Spark picks this up for executor classpath:
# https://github.com/apache/spark/blob/master/resource-managers/kubernetes/docker/src/main/dockerfiles/spark/entrypoint.sh#L56
ENV HADOOP_HOME=/opt/pkb/hadoop
ENV HADOOP_OPTIONAL_TOOLS=hadoop-aws

ARG spark_version=3.1.2
ENV SPARK_HOME=/opt/pkb/spark

RUN apt-get update && \
    apt-get install -y curl openjdk-$java_version-jre-headless python3 tini

# Install hadoop to make s3 connector work
RUN mkdir -p $HADOOP_HOME
RUN curl -L https://downloads.apache.org/hadoop/common/hadoop-$hadoop_version/hadoop-$hadoop_version.tar.gz \
    | tar -C $HADOOP_HOME --strip-components=1 -xzf -

RUN mkdir -p $SPARK_HOME
RUN curl -L https://downloads.apache.org/spark/spark-$spark_version/spark-$spark_version-bin-without-hadoop.tgz \
    | tar -C $SPARK_HOME --strip-components=1 -xzf -

# Install GCS connector
RUN cd $HADOOP_HOME/share/hadoop/common/lib && \
    curl -O https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar

# Configure Spark
RUN echo "spark.master=k8s://https://kubernetes" \
    > $SPARK_HOME/conf/spark-defaults.conf

# Used by spark-submit for driver classpath
RUN echo SPARK_DIST_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath` \
    > $SPARK_HOME/conf/spark-env.sh

# Set up entrypoint
RUN cp $SPARK_HOME/kubernetes/dockerfiles/spark/entrypoint.sh /entrypoint.sh
RUN chmod 755 /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

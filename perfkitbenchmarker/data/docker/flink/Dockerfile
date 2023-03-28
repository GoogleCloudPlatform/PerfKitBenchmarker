ARG base_image=flink:1.15
FROM $base_image
# Add flink job jar.
RUN mkdir /opt/flink/usrlib
ADD job.jar /opt/flink/usrlib/
# Support flink-gs-fs-hadoop file system to use Google Cloud Storage.
RUN if ["$base_image" >= "flink:1.15"]; then \
      RUN mkdir -p $FLINK_HOME/plugins/gs-fs-hadoop && \
      mv $FLINK_HOME/opt/flink-gs-fs-hadoop-* $FLINK_HOME/plugins/gs-fs-hadoop \
   else \
      RUN mkdir -p $FLINK_HOME/lib \
       && cd $FLINK_HOME/lib \
       && curl -LO curl -LO https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop2-uber/2.8.3-1.8.3/flink-shaded-hadoop2-uber-2.8.3-1.8.3.jar \
       && curl -LO https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop2-2.2.0/gcs-connector-hadoop2-2.2.0.jar; \
   fi

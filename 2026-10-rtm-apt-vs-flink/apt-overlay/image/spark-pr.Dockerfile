# PR (SPARK-58131) Spark image for EKS = the 4.3.0-SNAPSHOT engine + RTM jars + Kafka connector
# + S3A jars, baked in. Built FROM the stock multi-arch release image so the OS/entrypoint match;
# the 4.3.0 engine jars and PySpark are swapped in (jars are arch-independent, so this is safe to
# build --platform linux/amd64 for m5 nodes). Build context = repo root.
#
# Prereqs (produced locally, see spark/pr-image/build.sh + the run scripts):
#   spark/pr-image/jars/     4.3.0-SNAPSHOT assembly jars
#   spark/pr-image/python/   4.3.0-SNAPSHOT PySpark (pyspark/ + lib/py4j)
#   spark/pr-image/s3a/      hadoop-aws-3.5.0.jar + bundle-2.35.4.jar  (match Hadoop 3.5.0)
#   spark/libs-pr/           4.3.0 kafka connector + kafka-clients 3.9.2 + commons-pool2 2.13.1
#   spark/scala-rtm/target/scala-2.13/scala-rtm_2.13-0.1.0.jar   (sbt package)
#   spark/java-rtm/target/java-rtm.jar                           (mvn package)
FROM apache/spark:4.1.2-scala2.13-java17-ubuntu

USER root
ARG SCALA_BIN=2.13

# Python for the measured PySpark RTM job (executors run no Python — out_ts via reflect()).
RUN apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq python3 \
 && ln -sf /usr/bin/python3 /usr/local/bin/python3 && rm -rf /var/lib/apt/lists/*

# --- Swap the engine to 4.3.0-SNAPSHOT (contains the PR) ---
RUN rm -f /opt/spark/jars/*.jar
COPY spark/pr-image/jars/   /opt/spark/jars/
RUN rm -rf /opt/spark/python
COPY spark/pr-image/python/ /opt/spark/python/

# --- The compiled RTM pipeline jars (Scala + Java) and the measured PySpark job ---
COPY spark/scala-rtm/target/scala-${SCALA_BIN}/scala-rtm_${SCALA_BIN}-0.1.0.jar /opt/spark/jars/scala-rtm.jar
COPY spark/java-rtm/target/java-rtm.jar /opt/spark/jars/java-rtm.jar
COPY spark/pyspark-bridge/rtm_pyspark_measured.py /opt/spark/work-dir/rtm_pyspark_measured.py

# --- Version-matched Kafka connector (4.3.0) + S3A jars (Hadoop 3.5.0 / AWS SDK v2 2.35.4) ---
COPY spark/libs-pr/*.jar        /opt/spark/jars/
COPY spark/pr-image/s3a/*.jar   /opt/spark/jars/

RUN chmod -R a+rX /opt/spark/jars /opt/spark/python
USER spark

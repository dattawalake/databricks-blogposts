# PR image for SPARK-58131 (async progress tracking for Real-Time Mode).
# Built FROM the stock release image so the OS / Java 17 / entrypoint / launcher scripts are
# identical and proven; only the Spark engine jars and PySpark are swapped to 4.3.0-SNAPSHOT
# (branch-4.x, which contains the PR). Version reported by the cluster becomes 4.3.0-SNAPSHOT.
ARG BASE_IMAGE=apache/spark:4.1.2-scala2.13-java17-ubuntu
FROM ${BASE_IMAGE}

USER root

# Replace the entire jar set (4.1.2 -> 4.3.0-SNAPSHOT). Different filenames, so remove first.
RUN rm -f /opt/spark/jars/*.jar
COPY jars/ /opt/spark/jars/

# Replace PySpark with the branch-4.x sources (py4j zip included under python/lib).
RUN rm -rf /opt/spark/python
COPY python/ /opt/spark/python/

# World-readable/traversable so both the default `spark` user and root (jupyter) can use them.
RUN chmod -R a+rX /opt/spark/jars /opt/spark/python

USER spark

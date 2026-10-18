#!/usr/bin/env bash
# Assemble the SPARK-58131 PR image and the version-matched Kafka connector jars.
# Run this AFTER the branch-4.x maven build completes:
#   mvn -DskipTests -Dmaven.test.skip=true clean package -pl assembly,connector/kafka-0-10-sql -am
#
# Produces:
#   - docker image  local/spark-pr:4.3.0   (4.3.0-SNAPSHOT engine + PySpark on the stock base)
#   - spark/libs-pr/*.jar                   (4.3.0 kafka connector + matched kafka-clients/pool2)
set -euo pipefail

SRC="${SPARK_SRC:-$HOME/spark}"
SCALA_BIN="2.13"
TAG="${PR_IMAGE_TAG:-local/spark-pr:4.3.0}"
BASE_IMAGE="${BASE_IMAGE:-apache/spark:4.1.2-scala2.13-java17-ubuntu}"

HERE="$(cd "$(dirname "$0")" && pwd)"          # .../spark/pr-image
REPO="$(cd "$HERE/../.." && pwd)"              # repo root
ASSEMBLY_JARS="$SRC/assembly/target/scala-${SCALA_BIN}/jars"

echo "==> [1/5] Verify build outputs exist"
[ -d "$ASSEMBLY_JARS" ] || { echo "ERROR: $ASSEMBLY_JARS not found (build not finished?)"; exit 1; }
SPARK_VER="$(ls "$ASSEMBLY_JARS"/spark-core_${SCALA_BIN}-*.jar | sed -E 's/.*spark-core_[0-9.]+-(.*)\.jar/\1/')"
echo "    detected Spark version: ${SPARK_VER}"

echo "==> [2/5] Stage jars + python into build context"
rm -rf "$HERE/jars" "$HERE/python"
mkdir -p "$HERE/jars"
cp "$ASSEMBLY_JARS"/*.jar "$HERE/jars/"
# PySpark sources + py4j (from the source tree, matches branch-4.x)
mkdir -p "$HERE/python"
cp -R "$SRC/python/pyspark" "$HERE/python/pyspark"
cp -R "$SRC/python/lib"     "$HERE/python/lib"
echo "    staged $(ls "$HERE/jars" | wc -l | tr -d ' ') jars"

echo "==> [3/5] docker build ${TAG}"
docker build --build-arg BASE_IMAGE="${BASE_IMAGE}" -t "${TAG}" "$HERE"

echo "==> [4/5] Assemble spark/libs-pr (version-matched connector jars)"
LIBS_PR="$REPO/spark/libs-pr"
mkdir -p "$LIBS_PR"
rm -f "$LIBS_PR"/*.jar
# Connector jars built in the reactor:
cp "$SRC/connector/kafka-0-10-sql/target/spark-sql-kafka-0-10_${SCALA_BIN}-${SPARK_VER}.jar" "$LIBS_PR/"
cp "$SRC/connector/kafka-0-10-token-provider/target/spark-token-provider-kafka-0-10_${SCALA_BIN}-${SPARK_VER}.jar" "$LIBS_PR/"
# Their runtime deps resolved into the local maven cache during the build:
KAFKA_VER="$(sed -nE 's/.*<kafka.version>([^<]+)<.*/\1/p' "$SRC/pom.xml" | head -1)"
POOL_VER="$(sed -nE 's/.*<commons-pool2.version>([^<]+)<.*/\1/p' "$SRC/pom.xml" | head -1)"
cp "$HOME/.m2/repository/org/apache/kafka/kafka-clients/${KAFKA_VER}/kafka-clients-${KAFKA_VER}.jar" "$LIBS_PR/"
cp "$HOME/.m2/repository/org/apache/commons/commons-pool2/${POOL_VER}/commons-pool2-${POOL_VER}.jar" "$LIBS_PR/"
echo "    libs-pr:"; ls -1 "$LIBS_PR"

echo "==> [5/5] Done."
echo "    Run PR benchmark, e.g.:"
echo "      SPARK_IMAGE=${TAG} SPARK_LIBS=libs-pr SPARK_VERSION=${SPARK_VER} bench/run_spark.sh rtm"
echo "      SPARK_IMAGE=${TAG} SPARK_LIBS=libs-pr SPARK_VERSION=${SPARK_VER} bench/run_pyspark.sh"

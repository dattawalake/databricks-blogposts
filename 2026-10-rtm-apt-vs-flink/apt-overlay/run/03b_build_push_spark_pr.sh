#!/usr/bin/env bash
# Build + push the APT-enabled Spark app image to ECR (repo: rtm-bench/spark-pr), for Spark 4.3.0 GA.
#
# Async Progress Tracking (SPARK-58131) ships inside Spark 4.3.0, so there is NO source build and
# NO jar-swap. This just: (1) compiles the RTM pipeline jars (Scala + Java), (2) bakes them + the
# Kafka/S3A connectors onto the official apache/spark:4.3.0 base (see eks/images/spark-pr.Dockerfile),
# (3) pushes. APT is turned on by the config flag in the SparkApplication manifest, not the image.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -n "${AWS_PROFILE:-}" ]; then export AWS_PROFILE; fi   # else use exported AWS_* env creds
export AWS_REGION="${AWS_REGION:-us-east-1}"
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
REGISTRY="${ACCOUNT}.dkr.ecr.${AWS_REGION}.amazonaws.com"
TAG="${TAG:-apt1}"
PLATFORM=linux/amd64

echo "==> ECR login ($REGISTRY)"
aws ecr get-login-password --region "$AWS_REGION" | docker login --username AWS --password-stdin "$REGISTRY"
aws ecr describe-repositories --repository-names rtm-bench/spark-pr >/dev/null 2>&1 || \
  aws ecr create-repository --repository-name rtm-bench/spark-pr >/dev/null

echo "==> Build RTM pipeline jars (Scala + Java)"
( cd spark/scala-rtm && sbt -batch package >/tmp/sbt_build.log 2>&1 )
( cd spark/java-rtm && mvn -q -B package >/tmp/mvn_java_build.log 2>&1 )
test -f spark/scala-rtm/target/scala-2.13/scala-rtm_2.13-0.1.0.jar || { echo "scala jar missing"; exit 1; }
test -f spark/java-rtm/target/java-rtm.jar || { echo "java jar missing"; exit 1; }

echo "==> buildx push rtm-bench/spark-pr:$TAG ($PLATFORM) — FROM apache/spark:4.3.0"
docker buildx build --platform $PLATFORM -t "$REGISTRY/rtm-bench/spark-pr:$TAG" -f eks/images/spark-pr.Dockerfile . --push

echo "REGISTRY=$REGISTRY"  > eks/.registry.pr.env
echo "PR_TAG=$TAG"        >> eks/.registry.pr.env
echo "==> pushed $REGISTRY/rtm-bench/spark-pr:$TAG"

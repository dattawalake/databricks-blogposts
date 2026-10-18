#!/usr/bin/env bash
# Build + push ONLY the SPARK-58131 PR Spark image to ECR (repo: rtm-bench/spark-pr).
# Coexists with the stock rtm-bench/spark image from 03_build_push_images.sh — pick which
# one to run via the engine name in 04_run_benchmark.sh (spark-rtm-* vs spark-rtm-pr-*).
#
# Prereq: run  SPARK_SRC=/path/to/spark-src bash spark/pr-image/build.sh  first, so the
# 4.3.0 jars/python are staged under spark/pr-image/ and spark/libs-pr/ exists.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -n "${AWS_PROFILE:-}" ]; then export AWS_PROFILE; fi   # else use exported AWS_* env creds
export AWS_REGION="${AWS_REGION:-us-east-1}"
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
REGISTRY="${ACCOUNT}.dkr.ecr.${AWS_REGION}.amazonaws.com"
TAG="${TAG:-pr1}"
PLATFORM=linux/amd64

echo "==> Preflight: staged PR artifacts"
for p in spark/pr-image/jars spark/pr-image/python spark/pr-image/s3a spark/libs-pr; do
  [ -d "$p" ] && [ -n "$(ls -A "$p" 2>/dev/null)" ] || { echo "MISSING/empty: $p — run spark/pr-image/build.sh + download s3a jars first"; exit 1; }
done

echo "==> ECR login ($REGISTRY)"
aws ecr get-login-password --region "$AWS_REGION" | docker login --username AWS --password-stdin "$REGISTRY"
aws ecr describe-repositories --repository-names rtm-bench/spark-pr >/dev/null 2>&1 || \
  aws ecr create-repository --repository-name rtm-bench/spark-pr >/dev/null

echo "==> Build RTM pipeline jars (Scala + Java)"
( cd spark/scala-rtm && sbt -batch package >/tmp/sbt_build.log 2>&1 )
( cd spark/java-rtm && mvn -q -B package >/tmp/mvn_java_build.log 2>&1 )
test -f spark/scala-rtm/target/scala-2.13/scala-rtm_2.13-0.1.0.jar || { echo "scala jar missing"; exit 1; }
test -f spark/java-rtm/target/java-rtm.jar || { echo "java jar missing"; exit 1; }

echo "==> buildx push rtm-bench/spark-pr:$TAG ($PLATFORM)"
docker buildx build --platform $PLATFORM -t "$REGISTRY/rtm-bench/spark-pr:$TAG" -f eks/images/spark-pr.Dockerfile . --push

echo "REGISTRY=$REGISTRY"  > eks/.registry.pr.env
echo "PR_TAG=$TAG"        >> eks/.registry.pr.env
echo "==> pushed $REGISTRY/rtm-bench/spark-pr:$TAG"

#!/usr/bin/env bash
# Apply the APT (SPARK-58131) overlay onto a cloned copy of the upstream harness
#   https://github.com/fmorillo7694/spark-rtm-vs-flink
#
# Usage:  bash apply_overlay.sh /path/to/spark-rtm-vs-flink
#
# Copies our APT-specific additions (net-new) and our modified pipeline sources
# (drop-in replacements) into the right locations in the upstream tree.
set -euo pipefail

DEST="${1:-}"
[ -n "$DEST" ] && [ -d "$DEST" ] || { echo "usage: apply_overlay.sh <upstream-repo-dir>"; exit 1; }
HERE="$(cd "$(dirname "$0")" && pwd)"

echo "==> applying APT overlay from $HERE  ->  $DEST"

mkdir -p "$DEST/eks/jobs/p6" "$DEST/spark/pr-image" "$DEST/eks/images" \
         "$DEST/spark/scala-rtm/src/main/scala" "$DEST/spark/pyspark-bridge" "$DEST/common"

# --- net-new files ---
cp "$HERE/run/04d_run_p6_pr.sh"           "$DEST/eks/04d_run_p6_pr.sh"
cp "$HERE/run/03b_build_push_spark_pr.sh" "$DEST/eks/03b_build_push_spark_pr.sh"
cp "$HERE/manifests/spark-rtm-pr.yaml"    "$DEST/eks/jobs/p6/spark-rtm-pr.yaml"
cp "$HERE/manifests/pyspark-rtm-pr.yaml"  "$DEST/eks/jobs/p6/pyspark-rtm-pr.yaml"
cp "$HERE/image/build.sh"                 "$DEST/spark/pr-image/build.sh"
[ -f "$HERE/image/spark-pr.Dockerfile" ] && cp "$HERE/image/spark-pr.Dockerfile" "$DEST/eks/images/spark-pr.Dockerfile"

# --- modified-from-upstream (drop-in replacements) ---
cp "$HERE/pipeline/RtmPipeline.scala"        "$DEST/spark/scala-rtm/src/main/scala/RtmPipeline.scala"
cp "$HERE/pipeline/rtm_pyspark_measured.py"  "$DEST/spark/pyspark-bridge/rtm_pyspark_measured.py"
cp "$HERE/pipeline/latency_consumer.py"      "$DEST/common/latency_consumer.py"

# --- harness compat fix: pin Kafka to 4.2.0 (upstream ships 4.1.0) ---
cp "$HERE/harness/p6_kafka_strimzi.yaml"     "$DEST/eks/p6_kafka_strimzi.yaml"

chmod +x "$DEST/eks/04d_run_p6_pr.sh" "$DEST/eks/03b_build_push_spark_pr.sh" "$DEST/spark/pr-image/build.sh" 2>/dev/null || true

echo "==> overlay applied. Next: follow docs/REPRODUCE.md (§3 run the APT off/on A/B)."

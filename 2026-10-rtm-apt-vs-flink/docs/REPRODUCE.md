# Reproduce: Spark RTM + APT vs Flink

This guide reproduces the APT-off → APT-on tail collapse and the Flink comparison. It uses
the upstream [`fmorillo7694/spark-rtm-vs-flink`](https://github.com/fmorillo7694/spark-rtm-vs-flink)
harness plus the **APT overlay** in this folder.

There are two Spark paths — **pick one**:

| Path | When | Spark |
|---|---|---|
| **A. Spark 4.3.0 GA** (recommended) | Spark 4.3.0 is released | official `apache/spark:4.3.0` / `pip install pyspark==4.3.0` |
| **B. Build from source** | Pre-GA (APT is merged to `branch-4.x`, fix version 4.3.0) | build `apache/spark` `branch-4.x` and bake the image |

> **Why 4.3?** RTM base shipped in Spark 4.1, but **Async Progress Tracking (SPARK-58131)**
> lands in **4.3.0**. On 4.1/4.2 the RTM commit path is synchronous (the slow tail). You need
> 4.3+ to turn APT on.

---

## 0. One-time: fork the harness and apply the overlay

```bash
git clone https://github.com/fmorillo7694/spark-rtm-vs-flink.git
cd spark-rtm-vs-flink

# drop our APT additions on top (net-new files + modified pipeline sources)
bash /path/to/2026-10-rtm-apt-vs-flink/apt-overlay/apply_overlay.sh .
```

`apply_overlay.sh` copies:
- `eks/04d_run_p6_pr.sh`, `eks/03b_build_push_spark_pr.sh`
- `eks/jobs/p6/spark-rtm-pr.yaml`, `eks/jobs/p6/pyspark-rtm-pr.yaml`
- `spark/pr-image/build.sh`, `eks/images/spark-pr.Dockerfile`
- the modified `RtmPipeline.scala`, `rtm_pyspark_measured.py`, `latency_consumer.py`
- `eks/p6_kafka_strimzi.yaml` (Kafka pinned to 4.2.0)

> **AWS profile note.** Upstream's `eks/*.sh` bringup scripts hardcode `AWS_PROFILE=default`
> and `AWS_REGION=us-east-1`. If you use a named profile, either make it your default or edit
> those lines. Our APT run driver `eks/04d_run_p6_pr.sh` already honors `AWS_PROFILE` /
> `AWS_REGION` from the environment.

---

## Path A — Spark 4.3.0 GA (no source build)

### A1. Get Spark 4.3.0

```bash
# local / driver
pip install "pyspark==4.3.0"
# or, for the container image used by the K8s jobs, set the base image to:
#   apache/spark:4.3.0-scala2.13-java17-ubuntu
```

In the Spark job manifests the APT overlay templates the image tag — point it at the
official 4.3.0 image instead of a locally-baked one, and skip step B2 entirely.

### A2. Bring up Kafka + run

Follow the upstream README to stand up Kafka (6 partitions) and the Spark/Flink operators,
then jump to [§3 Run the A/B](#3-run-the-apt-off--on-ab).

---

## Path B — build Spark from source (pre-GA)

### B1. Build `apache/spark` branch-4.x

```bash
git clone https://github.com/apache/spark.git
cd spark && git checkout branch-4.x        # fix version 4.3.0-SNAPSHOT
# confirm SPARK-58131 is in your tree:
git log --oneline | grep -i "SPARK-58131" || echo "check branch"
./build/mvn -Pkubernetes -DskipTests -T1C clean package
```

### B2. Bake the APT image

```bash
# from your forked harness root, with SPARK_SRC pointing at the build above
SPARK_SRC=/path/to/spark bash spark/pr-image/build.sh
TAG=pr1 bash eks/03b_build_push_spark_pr.sh      # builds + pushes rtm-bench/spark-pr:pr1
```

### B3. Bring up Kafka + run

Same as A2.

---

## 3. Run the APT off / on A/B

Same image, same resources — only the `APT` flag flips.

```bash
# APT ON:
APT=true  RUN_SECONDS=600 WARMUP=60 bash eks/04d_run_p6_pr.sh spark-rtm-scala
APT=true  RUN_SECONDS=600 WARMUP=60 bash eks/04d_run_p6_pr.sh spark-rtm-java
APT=true  RUN_SECONDS=600 WARMUP=60 bash eks/04d_run_p6_pr.sh pyspark-rtm

# APT OFF (baseline — the synchronous-commit stall):
APT=false RUN_SECONDS=300 WARMUP=60 bash eks/04d_run_p6_pr.sh spark-rtm-scala
APT=false RUN_SECONDS=300 WARMUP=60 bash eks/04d_run_p6_pr.sh spark-rtm-java
APT=false RUN_SECONDS=300 WARMUP=60 bash eks/04d_run_p6_pr.sh pyspark-rtm
```

Flink reference (same cluster):

```bash
RUN_SECONDS=600 WARMUP=60 bash eks/04b_run_p6.sh flink
```

Each run writes `results/eks/<engine>-...-apt{true,false}_summary.json`.

## 4. What you should see

Our runs — APT-on from a 10-min steady window
([`../sample-results/eks-apt-on-10min/`](../sample-results/eks-apt-on-10min/)); APT-off from a
5-min A/B on the same image with the flag off
([`../sample-results/eks-apt-off-5min/`](../sample-results/eks-apt-off-5min/)):

| | APT off | APT on |
|---|--:|--:|
| Spark RTM e2e p99 (Scala / Java / PySpark) | ~849 / 921 / 782 ms | **~61 / 64 / 67 ms** |
| e2e p99 tail collapse | — | **~12–14×** |
| median (pipeline / e2e) | ~6 ms / ~16 ms | ~6 ms / ~16 ms (unchanged) |
| Flink DataStream e2e p99 (reference) | — | **63 ms** |

APT-on Spark lands within **0.97–1.06× of Flink's e2e p99 (63 ms)** — effectively at parity.
The APT-off tail is a structural per-checkpoint stall and reproduces to within ~±10%
regardless of window length or whether you use shipped 4.1 (833 / 906 / 763 ms) or the 4.3
image with the flag off.

## 5. Key knobs

- `APT` → `spark.sql.streaming.realTimeMode.asyncProgressTrackingByDefault.enabled`.
  Confirm it's active in the driver log:
  `AsyncProgressTrackingMicroBatchExecution ... Disabling AQE ... for Real-time Mode`.
- Trigger duration (`"60 seconds"`) is the **checkpoint cadence**, not a latency target.
- `WARMUP` seconds are dropped so batch 0 / cold start is excluded.
- Resources per engine matched across engines (7 cores / ~14 GB).

## 6. Tear down

Use the upstream harness teardown (`eks/99_teardown.sh`) to remove the cluster, registry
repos, object-store checkpoints, and keys. The EKS path creates **billable** cloud resources.

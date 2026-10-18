# Spark Real-Time Mode + Async Progress Tracking vs Apache Flink

A reproducible, end-to-end latency benchmark showing how **Async Progress Tracking (APT,
[SPARK-58131](https://issues.apache.org/jira/browse/SPARK-58131))** closes the tail-latency
gap between **Apache Spark Structured Streaming Real-Time Mode (RTM)** and **Apache Flink**
on the *identical* stateless Kafka → transform → Kafka pipeline at ~100k events/s.

> These are un-official, un-supported code snippets, shared for educational purposes.

## Overview

An earlier community study ([`fmorillo7694/spark-rtm-vs-flink`](https://github.com/fmorillo7694/spark-rtm-vs-flink),
reproducing the Neuw84 Spark RTM Medium post) found that Spark RTM **tied Flink at the
median** but **trailed badly on the tail**: with durable S3 checkpoints at scale, RTM's e2e
p99 sat at **~0.8–0.9 s vs Flink's ~63 ms (≈14×)**. The root cause was that RTM's
offset/commit-log write was **synchronous** — it stalled the data path at every checkpoint.

**APT makes that commit asynchronous.** This benchmark measures the before/after on the same
pipeline, same cluster, same resources — flipping a single config flag.

## Headline result

100k events/s in (~33.3k/s out after a ~33% filter), 6 Kafka partitions, 7 cores / 14 GB
per engine, 60-s trigger, durable S3 checkpoints.

**APT on — at parity with Flink (e2e p99):**

| Engine | pipeline p99 (ms) | e2e p99 (ms) | vs Flink |
|---|--:|--:|--:|
| **Flink** (Java DataStream, reference) | 33 | **63** | 1.00× |
| **Spark RTM — Scala** | 29.3 | **61.3** | **0.97×** |
| **Spark RTM — Java** | 33.5 | **64.3** | **1.02×** |
| **PySpark RTM** | 32.4 | **66.8** | **1.06×** |

**The APT effect — same image, flip the flag off → on:**

| Engine | e2e p99 **APT-off** | e2e p99 **APT-on** | tail collapse |
|---|--:|--:|--:|
| PySpark RTM | 781.7 ms | **66.8 ms** | **11.7×** |
| Spark RTM — Java | 921.5 ms | **64.3 ms** | **14.3×** |
| Spark RTM — Scala | 849.2 ms | **61.3 ms** | **13.8×** |

- **With APT on, Spark RTM is effectively at parity with Flink** on tail latency (0.97–1.06×);
  medians are tied (~6 ms pipeline / ~16 ms e2e).
- APT fixes the **tail only** — the median is ~6 ms in both cases. It removes the periodic
  checkpoint stall; it does not change steady-state compute.
- Scala / Java / PySpark are equivalent — the measured PySpark job runs **zero Python on
  executors** (`out_ts` via `reflect()`), so all three share one JVM plan.

The one knob that changes everything:

```
spark.sql.streaming.realTimeMode.asyncProgressTrackingByDefault.enabled = true   # APT on
                                                                        = false  # APT off (pre-fix path)
```

Raw summaries: [`sample-results/`](sample-results/) (`eks-apt-on-10min/` + `eks-apt-off-5min/`).

## Requirements

- **Spark 4.3.0+** (APT / SPARK-58131 fix version is **4.3.0**). Two options, see
  [`docs/REPRODUCE.md`](docs/REPRODUCE.md):
  - **Spark 4.3.0 GA** — `pip install pyspark==4.3.0` / official `apache/spark:4.3.0` image (no build).
  - **Pre-GA** — build `apache/spark` `branch-4.x` from source and bake the image.
- **Output mode:** `update` (RTM requirement); **no Photon**; fixed cluster size (no autoscale).
- A Kafka cluster (self-managed/Strimzi, Confluent, or Redpanda) and durable object-store
  checkpoints (S3/ADLS/GCS or a UC Volume).
- The upstream harness: the original
  [`fmorillo7694/spark-rtm-vs-flink`](https://github.com/fmorillo7694/spark-rtm-vs-flink)
  repo (pipeline + load generator + EKS scripts), onto which you drop this folder's
  **APT overlay**.

## Files

| Path | Description |
|---|---|
| `docs/REPRODUCE.md` | Step-by-step reproduce guide (4.3-GA path first, build-from-source fallback) |
| `apt-overlay/run/04d_run_p6_pr.sh` | **net-new** — APT A/B run driver (toggles the flag, 60-s trigger, 100k/s load) |
| `apt-overlay/run/03b_build_push_spark_pr.sh` | **net-new** — build + push the APT (4.3-line) Spark image to a registry |
| `apt-overlay/manifests/spark-rtm-pr.yaml` | **net-new** — Scala/Java RTM SparkApplication (APT flag templated) |
| `apt-overlay/manifests/pyspark-rtm-pr.yaml` | **net-new** — PySpark RTM SparkApplication (APT flag templated) |
| `apt-overlay/image/build.sh` | **net-new** — stage 4.3 jars/python/connector libs from a Spark source build |
| `apt-overlay/image/pr-image.Dockerfile` | **net-new** — local image Dockerfile used by `build.sh` |
| `apt-overlay/image/spark-pr.Dockerfile` | **net-new** — buildx Dockerfile used by `03b` to push the EKS image |
| `apt-overlay/pipeline/RtmPipeline.scala` | *modified-from-upstream* — adds the APT conf toggle |
| `apt-overlay/pipeline/rtm_pyspark_measured.py` | *modified-from-upstream* — measured PySpark RTM job + APT toggle |
| `apt-overlay/pipeline/latency_consumer.py` | *modified-from-upstream* — latency sampler used for these numbers |
| `apt-overlay/harness/p6_kafka_strimzi.yaml` | *compat* — pins Kafka to **4.2.0** (upstream ships 4.1.0) |
| `apt-overlay/apply_overlay.sh` | copies the overlay onto a cloned upstream repo |
| `sample-results/` | our APT-on (10-min) and APT-off (5-min) summary JSONs |

> **What's here vs what you fork.** This folder contains **only our APT additions**: files
> marked **net-new** are ours; files marked *modified-from-upstream* are our edits to upstream
> files (drop-in replacements applied by `apply_overlay.sh`). The base pipeline, load
> generator, and cluster scripts come from the upstream repo you clone — we do not re-host
> them here.

## Quick start

```bash
# 1. Clone the upstream harness
git clone https://github.com/fmorillo7694/spark-rtm-vs-flink.git
# 2. Apply this APT overlay on top
bash 2026-10-rtm-apt-vs-flink/apt-overlay/apply_overlay.sh spark-rtm-vs-flink
# 3. Follow docs/REPRODUCE.md (bring up Kafka + Spark, run the APT off/on A/B)
```

Full steps, including the Flink reference run and the Spark 4.3-GA (no-build) path, are in
[`docs/REPRODUCE.md`](docs/REPRODUCE.md).

## Credits

- **Neuw84** — the original Spark Real-Time Mode Medium post (the claim under test).
- **[`fmorillo7694/spark-rtm-vs-flink`](https://github.com/fmorillo7694/spark-rtm-vs-flink)**
  — the RTM-vs-Flink head-to-head harness (base pipeline, engine implementations, load
  generator, EKS scripts) this benchmark extends.
- This folder adds the **Async Progress Tracking (SPARK-58131)** before/after on top.

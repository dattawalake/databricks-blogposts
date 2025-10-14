# Real-Time Mode in Ad Attribution - Spark Streaming Demo

This example demonstrates the performance improvements of **Real-Time Mode (RTM)** compared to traditional **Trigger Mode** in Apache Spark Structured Streaming for AdTech workloads, specifically ad attribution processing.

## Overview

In real-world ad attribution scenarios, streaming platforms receive both "request" messages (ad exposure) and "acknowledgment" messages (user action like clicks). These messages need to be matched by transaction ID to enable proper ad attribution. This example showcases how RTM significantly reduces end-to-end latency in such stateful streaming workloads.

## Project Structure

```
main/
└── 2025-10-RTM-TransformWithState-AdAttribution/
    ├── adtechdata_producer_scala.py         # Data producer (Databricks notebook with Scala)
    ├── adtechdata_stream_processor_scala.scala  # Stream processor with stateful logic
    └── run_adtech_rtm_demo.scala            # Main demo runner and comparison
```

## Components

### 1. AdTech Data Producer (`adtechdata_producer_scala.py`)

**Class: `AdTechDataHelper`**

Generates synthetic AdTech data and publishes it to Kafka/Azure Event Hub:
- Creates request/acknowledgment pairs with matching transaction IDs
- Writes data to Kafka in batches with configurable delays
- Provides methods to read and parse Kafka topics
- Calculates latency metrics (avg, P50, P90, P99)

**Key Features:**
- Generates 100 records per batch (30-60% matching pairs)
- Adds Kafka headers for record type identification
- Supports batch ID tracking for latency analysis

### 2. Stream Processor (`adtechdata_stream_processor_scala.scala`)

**Class: `LogRecordProcessor`**

Stateful processor that matches request and acknowledgment records:
- Maintains state for requests and acknowledgments separately
- Uses `ListState` for multiple acknowledgments per request
- Implements TTL-based state expiration
- Combines matched records into output transactions

**Class: `AdTechStreamProcessor`**

End-to-end streaming ETL pipeline:
- Reads from Kafka with structured streaming
- Applies stateful processing using `transformWithState`
- Writes matched records to output Kafka topic
- Supports both RTM and Trigger modes

**State Management:**
- RocksDB state store backend
- Configurable state timeout (default: 2 minutes)
- Handles late arrivals and out-of-order events

### 3. Demo Runner (`run_adtech_rtm_demo.scala`)

Comprehensive demo notebook that:
- Configures Kafka/Event Hub connections
- Defines payload schemas for requests and acknowledgments
- Runs both RTM and Trigger Mode experiments
- Compares latency metrics with visualizations

## Architecture

```
┌─────────────┐        ┌──────────────────┐        ┌─────────────┐
│   Producer  │───────>│  Input Kafka     │───────>│   Stream    │
│  (Batched)  │        │  Topic (eh6)     │        │  Processor  │
└─────────────┘        └──────────────────┘        │  (Stateful) │
                                                    └──────┬──────┘
                                                           │
                                                           v
┌─────────────┐        ┌──────────────────┐        ┌─────────────┐
│  Latency    │<───────│ Output Kafka     │<───────│   Matched   │
│  Analysis   │        │ Topic (eh5/eh7)  │        │   Records   │
└─────────────┘        └──────────────────┘        └─────────────┘
```

## Prerequisites

### Databricks Cluster Configuration

**Required Libraries:**
- `org.apache.kafka:kafka-clients:3.7.0`

**Cluster Spark Configurations:**
```
spark.databricks.streaming.realTimeMode.enabled = true
spark.shuffle.manager = org.apache.spark.shuffle.streaming.MultiShuffleManager
spark.databricks.dagScheduler.type = ConcurrentStageDAGScheduler
```

### Kafka/Event Hub Setup

You'll need:
- Azure Event Hub namespace (or Kafka cluster)
- Three topics: one for input, two for output (RTM and Trigger modes)
- Connection credentials stored in Databricks secrets

## Setup Instructions

### 1. Clone the Repository

```bash
git clone <repository-url>
cd rtm_blog
```

### 2. Configure Databricks Secrets

Store your Event Hub credentials in Databricks secrets:

```bash
databricks secrets put --scope datta_eh --key eventhub_policy_name
databricks secrets put --scope datta_eh --key eventhub_key
```

### 3. Update Configuration

In `run_adtech_rtm_demo.scala`, update these variables:

```scala
val input_topic_name = "your-input-topic"
val rtm_output_topic = "your-rtm-output-topic"
val trigger_output_topic = "your-trigger-output-topic"
val EH_NAMESPACE = "your-eventhub-namespace"
```

### 4. Import Notebooks to Databricks

1. Upload all `.scala` and `.py` files to your Databricks workspace
2. Ensure they're in the same directory for `%run` commands to work

## Usage

### Running the Demo

Open `run_adtech_rtm_demo.scala` in Databricks and execute the cells sequentially:

#### Real-Time Mode Example:

```scala
val query = streamProcessor.processStream(
  mode="rtm", 
  stateTimeoutInmin=2, 
  inputTopicName=input_topic_name, 
  outputTopicName=rtm_output_topic
)

// Wait for stream to initialize
Thread.sleep(20000)

// Produce 5 batches with 10-second intervals
dataHelper.startProducer(
  inputTopicName=input_topic_name, 
  numberOfbacthes=5, 
  delayInSec=10
)

// Wait and calculate latency
query.awaitTermination(5 * 60 * 1000)
query.stop()
val rmtDf = dataHelper.calculateLatency(outputTopicName=rtm_output_topic)
```

#### Trigger Mode Example:

```scala
val query = streamProcessor.processStream(
  mode="trigger", 
  stateTimeoutInmin=2, 
  inputTopicName=input_topic_name, 
  outputTopicName=trigger_output_topic
)

// Similar workflow as RTM...
```

### Analyzing Results

The demo generates a comparison chart showing latency metrics:
- **Average Latency**
- **P50 (Median)**
- **P90**
- **P99**

Expected results: RTM shows significantly lower latency across all percentiles.

## Key Concepts

### Real-Time Mode (RTM)

RTM is a Spark Structured Streaming optimization that:
- Processes micro-batches with minimal latency
- Enables concurrent stage execution
- Uses optimized shuffle managers
- Reduces scheduling overhead

### Stateful Processing

The demo uses `transformWithState` API for:
- Maintaining state across micro-batches
- Matching requests with acknowledgments
- Handling out-of-order events
- Automatic state cleanup with TTL

### Data Model

**Input Schema:**
- `id`: Record identifier
- `type`: Record type (1=ack, 2=request)
- `transaction_id`: Match key
- `timestamp`: Event timestamp
- `value`: JSON payload

**Output Schema:**
- `transaction_id`: Match key
- `key_request` / `key_ack`: Record keys
- `record_timestamp_request` / `record_timestamp_ack`: Timestamps
- `value_request` / `value_ack`: Parsed JSON payloads
- `etl_timestamp`: Processing timestamp

## Performance Tuning

### State Store Configuration

```scala
spark.sql.streaming.stateStore.providerClass = 
  org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled = true
```

### Parallelism

```scala
spark.sql.shuffle.partitions = 10  // Adjust based on data volume
```

### State Timeout

```scala
// Configure based on expected arrival delay
val stateTimeoutInmin = 2  // minutes
```

## Troubleshooting

### Common Issues

1. **Stream not processing records:**
   - Verify Kafka credentials and topic names
   - Check cluster configuration for RTM settings
   - Review checkpoint location permissions

2. **High latency in RTM:**
   - Ensure RTM cluster configs are properly set
   - Check shuffle partition count
   - Monitor resource utilization

3. **State-related errors:**
   - Verify checkpoint location is accessible
   - Check state timeout configuration
   - Review RocksDB settings

## Metrics and Monitoring

The project provides several helper methods for analysis:

```scala
// Read parsed output data
dataHelper.readAndParseKafkaOutputTopic(outputTopicName)

// Read input topic
dataHelper.readAndParseKafkaInputTopic(inputTopicName)

// Calculate latency statistics
dataHelper.calculateLatency(outputTopicName)
```

## Acknowledgments

This demo showcases Apache Spark's Real-Time Mode capabilities for low-latency streaming workloads in production AdTech environments.

---

**Note:** This is a demonstration project. Adjust configurations, error handling, and production-readiness features as needed for your specific use case.


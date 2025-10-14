// Databricks notebook source
// MAGIC %md
// MAGIC #Real Time Mode in Ad Attribution 

// COMMAND ----------

// MAGIC %md
// MAGIC The below notebook shows the improved latency with real time mode (RTM) as opposed to the legacy trigger mode in ad streaming workload as well as how to implement. 
// MAGIC
// MAGIC In this real world example, the incoming data from a streaming platform contains both "request" messages as well as "ack" messages which need to find each other's match in order to get proper ad attribution between exposure to the ad and an action (such as a click) with that ad.

// COMMAND ----------

// MAGIC %md
// MAGIC ##Cluster Configurations

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Cluster Jar: 
// MAGIC - org.apache.kafka:kafka-clients:3.7.0
// MAGIC ##### Cluster configs : 
// MAGIC - spark.databricks.streaming.realTimeMode.enabled true
// MAGIC - spark.shuffle.manager org.apache.spark.shuffle.streaming.MultiShuffleManager
// MAGIC - spark.databricks.dagScheduler.type ConcurrentStageDAGScheduler

// COMMAND ----------

// MAGIC %md
// MAGIC ##Run the data generation simulaters below

// COMMAND ----------

// DBTITLE 1,Run Adtech Data Stream Processor Scala
// MAGIC
// MAGIC %run ./adtechdata_stream_processor_scala

// COMMAND ----------

// DBTITLE 1,Run Adtech Data Producer Scala Script
// MAGIC %run ./adtechdata_producer_scala

// COMMAND ----------

// DBTITLE 1,Defining Event Hub Configurations and Payload Schemas
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType, TimestampType}

// Topic names for source and target Event Hubs, replace with your values
val input_topic_name = "eh6"

var rtm_output_topic = "eh7"
val trigger_output_topic = "eh5"

// Event Hub configuration, replace as per your setup Kafka/Eventhub
val EH_NAMESPACE = "gc-streaming"
val EVENTHUB_POLICY_NAME = dbutils.secrets.get(scope = "datta_eh", key = "eventhub_policy_name")
val EVENTHUB_KEY = dbutils.secrets.get(scope = "datta_eh", key = "eventhub_key")

// Build Event Hub connection string
val EH_CONN_STR = s"Endpoint=sb://$EH_NAMESPACE.servicebus.windows.net/;" +
  s"SharedAccessKeyName=$EVENTHUB_POLICY_NAME;" +
  s"SharedAccessKey=$EVENTHUB_KEY"

// Kafka options for connecting to Event Hub
val kafka_options = Map(
  "kafka.bootstrap.servers" -> s"$EH_NAMESPACE.servicebus.windows.net:9093",
  "kafka.sasl.mechanism" -> "PLAIN",
  "kafka.security.protocol" -> "SASL_SSL",
  "kafka.sasl.jaas.config" ->
    s"""kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$$ConnectionString" password="$EH_CONN_STR";"""
)

//Below code keep same as is to test this use case.
// Sample payload data for adtech request
val adtech_request_value = Map(
  "ad_id" -> "ad123",
  "campaign_id" -> "camp456",
  "bid" -> 1.23,
  "user" -> Map(
    "user_id" -> "user789",
    "geo" -> Map("country" -> "US", "city" -> "San Francisco")
  ),
  "device" -> Map(
    "os" -> "iOS",
    "model" -> "iPhone 13"
  )
)

// Sample payload data for adtech ack
val adtech_ack_value = Map(
  "ad_id" -> "ad123",
  "status" -> "delivered",
  "delivery_time" -> "2025-10-01T12:34:56Z"
)

// Combined payload map
val payload = Map(
  "request" -> adtech_request_value,
  "ack" -> adtech_ack_value
)

// Schema for adtech ack messages
val adtech_ack_schema = StructType(Seq(
  StructField("ad_id", StringType),
  StructField("batchId", StringType),
  StructField("status", StringType),
  StructField("delivery_time", StringType)
))

// Schema for adtech request messages
val adtech_request_schema = StructType(Seq(
  StructField("ad_id", StringType),
  StructField("batchId", StringType),
  StructField("campaign_id", StringType),
  StructField("bid", DoubleType),
  StructField("user", StructType(Seq(
    StructField("user_id", StringType),
    StructField("geo", StructType(Seq(
      StructField("country", StringType),
      StructField("city", StringType)
    )))
  ))),
  StructField("device", StructType(Seq(
    StructField("os", StringType),
    StructField("model", StringType)
  )))
))

// Combined schema for the full payload
val schema = StructType(Seq(
  StructField("value_ack", adtech_ack_schema),
  StructField("value_request", adtech_request_schema),
  StructField("etl_timestamp", TimestampType),
  StructField("record_timestamp_request", TimestampType),
  StructField("record_timestamp_ack", TimestampType)
))

// COMMAND ----------

// DBTITLE 1,Create Producer and stream processor object
import com.demo.data.{AdTechDataHelper}
import com.demo.data.{AdTechStreamProcessor}

// Get the active Spark session
implicit val spark = org.apache.spark.sql.SparkSession.active

// Initialize helper and processor classes for adtech data
val dataHelper = new AdTechDataHelper(kafka_options, payload, schema)
val streamProcessor = new AdTechStreamProcessor(kafka_options, schema)

// COMMAND ----------

// MAGIC %md
// MAGIC ##Real Time Mode Example:

// COMMAND ----------

// DBTITLE 1,AdTech Stream Processing in RTM Mode example

// Step 1: Run first thread (stream processing) in a single-threaded executor context
val query = streamProcessor.processStream(mode="rtm", stateTimeoutInmin=2, inputTopicName=input_topic_name, outputTopicName = rtm_output_topic)
println("stream process started..")

// Step 2: Wait for 20 seconds, then start producer
Thread.sleep(20000)
println("Data producer starting..")

// Produce 5 batches, interval of 10 seconds
dataHelper.startProducer(inputTopicName=input_topic_name , numberOfbacthes=5, delayInSec=10)
println("Data producer completed..")

// Step 3: Terminate streaming query after 1 min
query.awaitTermination(5 * 60 * 1000)
query.stop()
println("stream process completed..")

// Step 4: Run latency calculation after stream processing is stopped
val rmtDf = dataHelper.calculateLatency(outputTopicName = rtm_output_topic)
//display(rmtDf)

// COMMAND ----------

// MAGIC %md
// MAGIC ##Trigger Mode Example:

// COMMAND ----------

// DBTITLE 1,AdTech Stream Processing in Trigger Mode example

// Step 1: Run first thread (stream processing) in a single-threaded executor context
val query = streamProcessor.processStream(mode="trigger", stateTimeoutInmin=2, inputTopicName=input_topic_name, outputTopicName = trigger_output_topic)
println("stream process started..")

// Step 2: Wait for 20 seconds, then start producer
Thread.sleep(20000)
println("Data producer starting..")

// Produce 5 batches, interval of 10 seconds
dataHelper.startProducer(inputTopicName=input_topic_name , numberOfbacthes=5, delayInSec=10)
println("Data producer completed..")

// Step 3: Terminate streaming query after 2 min
query.awaitTermination(2 * 60 * 1000)
query.stop()
println("stream process completed..")

// Step 4: Run latency calculation after stream processing is stopped
val tiggerDf = dataHelper.calculateLatency(outputTopicName = trigger_output_topic)
//display(tiggerDf)

// COMMAND ----------

// MAGIC %md
// MAGIC ##Latency Comparison:

// COMMAND ----------

// MAGIC %md
// MAGIC ####The below chart displays the latency differences between RTM and Trigger Mode (lower is better)
// MAGIC ####As you can see, for every percentile ameasured RTM out performs Trigger Mode from anywhere from x ms to y ms and by an average of z ms. 
// MAGIC

// COMMAND ----------

// DBTITLE 1,Summarize Latency Percentiles Analysis
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._

// Helper to extract latency values for specific columns
val latencyMetrics = Seq("avg", "p50", "p90", "p99")

def extractLatencies(df: DataFrame): Map[String, Double] = {
  val cols = df.columns.map(_.toLowerCase.replaceAll("\\s", ""))
  val selected = latencyMetrics.filter(m => cols.exists(_.contains(m)))
  val colMap = df.columns.map(c => c.toLowerCase.replaceAll("\\s", "") -> c).toMap
  val aggExprs = selected.map(m => avg(col(colMap(m))).as(m))
  val aggRow = df.agg(aggExprs.head, aggExprs.tail: _*).collect()(0)
  selected.map { m =>
    val vAny = aggRow.getAs[Any](m)
    val v = vAny match {
      case n: java.lang.Number => n.doubleValue()
      case s: String => s.toDouble
      case other => other.toString.toDouble
    }
    m -> v
  }.toMap
}

val rmtLatencies = extractLatencies(rmtDf.selectExpr("P50_Latency as p50", "P90_Latency as p90", "P99_Latency as p99", "avg"))
val triggerLatencies = extractLatencies(tiggerDf.selectExpr("P50_Latency as p50", "P90_Latency as p90", "P99_Latency as p99", "avg"))

val xLabels = latencyMetrics.map(_.toUpperCase)
val rtmValues = latencyMetrics.map(m => rmtLatencies.getOrElse(m, 0.0))
val triggerValues = latencyMetrics.map(m => triggerLatencies.getOrElse(m, 0.0))

val html = s"""
<div id="bar_chart" style="width: 900px; height: 480px;"></div>
<script src="https://cdn.plot.ly/plotly-2.31.1.min.js"></script>
<script>
const xLabels = ${xLabels.map(l => s"'$l'").mkString("[", ",", "]")};
const rtmValues = [${rtmValues.map(_.formatted("%.1f")).mkString(",")}];
const triggerValues = [${triggerValues.map(_.formatted("%.1f")).mkString(",")}];

const data = [
  {
    x: xLabels,
    y: rtmValues,
    name: 'RTM',
    type: 'bar',
    marker: {color: '#1f77b4'},
  },
  {
    x: xLabels,
    y: triggerValues,
    name: 'Trigger',
    type: 'bar',
    marker: {color: '#ff7f0e'},
  }
];

const layout = {
  barmode: 'group',
  title: 'Latency Comparison: RTM vs Trigger Mode',
  xaxis: {title: 'Latencies'},
  yaxis: {title: 'ms'},
  legend: {orientation: 'h', x: 0.3, y: 1.15},
  height: 480,
  width: 900
};

Plotly.newPlot('bar_chart', data, layout);
</script>
"""

displayHTML(html)

// COMMAND ----------

// MAGIC %md
// MAGIC ##Additional Metrics from the Real Time Mode Stream:

// COMMAND ----------

// DBTITLE 1,Other helper methods to analyze topic data
// Display the parsed data from the Kafka output topic for inspection
display(dataHelper.readAndParseKafkaOutputTopic(rtm_output_topic))

// COMMAND ----------

// DBTITLE 1,Read and Parse Kafka Input Topic Data
display(dataHelper.readAndParseKafkaInputTopic(input_topic_name))
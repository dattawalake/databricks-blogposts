# Databricks notebook source
# DBTITLE 1,AdTech Kafka Producer - Generate and Write JSON Data and calculates metrics
# MAGIC %scala
# MAGIC package com.demo.data
# MAGIC
# MAGIC import scala.util.Random
# MAGIC import java.util.UUID
# MAGIC import scala.collection.mutable.ArrayBuffer
# MAGIC import scala.collection.immutable.Map
# MAGIC import scala.concurrent.duration._
# MAGIC import scala.language.postfixOps
# MAGIC
# MAGIC import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
# MAGIC import org.apache.kafka.common.header.internals.RecordHeader
# MAGIC import java.util.Properties
# MAGIC import java.nio.charset.StandardCharsets
# MAGIC import org.json4s._
# MAGIC import org.json4s.jackson.Serialization
# MAGIC import org.json4s.jackson.Serialization.write
# MAGIC
# MAGIC import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType, TimestampType}
# MAGIC import org.apache.spark.sql.{DataFrame, SparkSession}
# MAGIC import org.apache.spark.sql.functions._
# MAGIC
# MAGIC // Helper class for AdTech data generation, Kafka I/O, and latency analysis
# MAGIC class AdTechDataHelper(
# MAGIC   kafkaConfig: Map[String, String],
# MAGIC   samplePayloadStructure: scala.collection.immutable.Map[String, scala.collection.immutable.Map[String,Any]],
# MAGIC   schema: org.apache.spark.sql.types.StructType
# MAGIC )(implicit val spark: SparkSession){
# MAGIC
# MAGIC   // Extract ack and request payload templates from samplePayloadStructure
# MAGIC   val adtech_ack_value = samplePayloadStructure.find { case (k, _) => k == "ack" }.map(_._2).getOrElse(Map.empty)
# MAGIC   val adtech_request_value = samplePayloadStructure.find { case (k, _) => k == "request" }.map(_._2).getOrElse(Map.empty)
# MAGIC
# MAGIC   // Generate sample JSON data for Kafka publishing
# MAGIC   private def generate_sample_json_data(): Seq[Map[String, Any]] = {
# MAGIC     val num_records = 100
# MAGIC     val num_pairs = Random.nextInt(31) + 30 // Number of request/ack pairs with same transaction_id
# MAGIC     val sample_json_data = ArrayBuffer[Map[String, Any]]()
# MAGIC
# MAGIC     // Generate pairs with same transaction_id for request and ack
# MAGIC     for (_ <- 0 until num_pairs) {
# MAGIC       val base_transaction_id = UUID.randomUUID().toString
# MAGIC     
# MAGIC       sample_json_data += Map(
# MAGIC         "key" -> s"$base_transaction_id", 
# MAGIC         "type" -> s"2", //request
# MAGIC         "value" -> (adtech_request_value + ("ad_id" -> base_transaction_id))
# MAGIC       )
# MAGIC       sample_json_data += Map(
# MAGIC         "key" -> s"$base_transaction_id", 
# MAGIC         "type" -> s"1", //ack
# MAGIC         "value" -> (adtech_ack_value + ("ad_id" -> base_transaction_id))
# MAGIC       )
# MAGIC     }
# MAGIC
# MAGIC     // Generate remaining records with different transaction_id for request and ack
# MAGIC     val remaining = num_records - sample_json_data.length
# MAGIC     for (_ <- 0 until remaining) {
# MAGIC       val transaction_id = UUID.randomUUID().toString
# MAGIC       val record_type = if (Random.nextBoolean()) 1 else 2
# MAGIC       val value = if (record_type == 1) adtech_ack_value else adtech_request_value
# MAGIC       sample_json_data += Map(
# MAGIC         "key" -> s"$transaction_id", 
# MAGIC         "type" -> s"$record_type",// ack/req
# MAGIC         "value" -> value
# MAGIC       )
# MAGIC     }
# MAGIC     sample_json_data.toSeq
# MAGIC   }
# MAGIC
# MAGIC   // Write generated sample data to Kafka input topic in batches
# MAGIC   private def write_to_kafka(batchId: String, inputTopicName: String): Unit = {
# MAGIC     implicit val formats = Serialization.formats(NoTypeHints)
# MAGIC
# MAGIC     val props = new Properties()
# MAGIC     props.put("bootstrap.servers", kafkaConfig("kafka.bootstrap.servers"))
# MAGIC     props.put("security.protocol", "SASL_SSL")
# MAGIC     props.put("sasl.mechanism", "PLAIN")
# MAGIC     props.put("sasl.jaas.config", kafkaConfig("kafka.sasl.jaas.config"))
# MAGIC     props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
# MAGIC     props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
# MAGIC
# MAGIC     val producer = new KafkaProducer[String, String](props)
# MAGIC
# MAGIC     for (_ <- 1 to 10) {
# MAGIC       val batch = generate_sample_json_data() // Should return Seq[Map[String, Any]]
# MAGIC       batch.foreach { record =>
# MAGIC         val valueMap = record("value").asInstanceOf[Map[String, Any]] + ("batchId" -> batchId)
# MAGIC         val key = record("key").toString
# MAGIC         val recordType = record("type").toString
# MAGIC         val value = write(valueMap)
# MAGIC         
# MAGIC         val producerRecord = new ProducerRecord[String, String](inputTopicName, key, value)
# MAGIC
# MAGIC         // add header "record_type"
# MAGIC         producerRecord.headers().add(new RecordHeader("record_type", recordType.getBytes(StandardCharsets.UTF_8)))
# MAGIC
# MAGIC         producer.send(producerRecord)
# MAGIC       }
# MAGIC       producer.flush()
# MAGIC       Thread.sleep(1000)
# MAGIC     }
# MAGIC     println(s"Done Batch $batchId")
# MAGIC   }
# MAGIC
# MAGIC   // Start producing batches of sample data to Kafka with delay between batches
# MAGIC   def startProducer(inputTopicName: String, numberOfbacthes:Int, delayInSec:Int): Unit = {
# MAGIC
# MAGIC       for (i <- 0 until numberOfbacthes) {
# MAGIC         val batchId = s"${UUID.randomUUID().toString}_$i"
# MAGIC         write_to_kafka(batchId, inputTopicName)
# MAGIC         Thread.sleep(delayInSec * 1000) //milisec
# MAGIC       }
# MAGIC   }
# MAGIC
# MAGIC   // Extract value_ack and value_request fields from schema for parsing
# MAGIC   val value_ack_field: StructType = schema("value_ack").dataType.asInstanceOf[StructType]
# MAGIC   val value_request_field: StructType = schema("value_request").dataType.asInstanceOf[StructType]
# MAGIC
# MAGIC   // Read and parse records from Kafka input topic, extracting key fields and batchId
# MAGIC   def readAndParseKafkaInputTopic(inputTopicName:String): DataFrame = {
# MAGIC     spark.read
# MAGIC       .format("kafka")
# MAGIC       .options(kafkaConfig)
# MAGIC       .option("subscribe", inputTopicName)
# MAGIC       .option("includeHeaders", "true")
# MAGIC       .load()
# MAGIC       .withColumn("key", col("key").cast("string"))
# MAGIC       .withColumn("id", col("key"))
# MAGIC       .withColumn("transaction_id", col("key"))
# MAGIC       .withColumn("type", expr("CAST(filter(headers, x -> x.key = 'record_type')[0].value AS STRING)").cast("int"))
# MAGIC       .withColumn("batchId",
# MAGIC         when(col("type") === 1, from_json(col("value").cast("string"), value_ack_field)("batchId"))
# MAGIC           .otherwise(from_json(col("value").cast("string"), value_request_field)("batchId"))
# MAGIC       )
# MAGIC   }
# MAGIC
# MAGIC   // Read and parse records from Kafka output topic, extracting timestamps and calculating latency
# MAGIC   def readAndParseKafkaOutputTopic(outputTopicName: String): DataFrame = {
# MAGIC     val df = spark.read
# MAGIC       .format("kafka")
# MAGIC       .options(kafkaConfig)
# MAGIC       .option("subscribe", outputTopicName)
# MAGIC       .load()
# MAGIC
# MAGIC     df
# MAGIC       .withColumn("key", col("key").cast("string"))
# MAGIC       .withColumn("data", col("value").cast("string"))
# MAGIC       .withColumn("parsed_value", from_json(col("data"), schema))
# MAGIC       .withColumn("sink-timestamp", unix_millis(col("timestamp")))
# MAGIC       .withColumn("source-etl-timestamp", unix_millis(col("parsed_value.etl_timestamp")))
# MAGIC       .withColumn("source-req-timestamp", unix_millis(col("parsed_value.record_timestamp_request")))
# MAGIC       .withColumn("source-ack-timestamp", unix_millis(col("parsed_value.record_timestamp_ack")))
# MAGIC       .withColumn("req-ack-timestamp_diff", abs(col("source-req-timestamp") - col("source-ack-timestamp")))
# MAGIC       .withColumn(
# MAGIC         "latency",
# MAGIC         col("sink-timestamp") - greatest(col("source-req-timestamp"), col("source-ack-timestamp"))
# MAGIC       )
# MAGIC       .select(
# MAGIC         "key",
# MAGIC         "source-req-timestamp",
# MAGIC         "source-ack-timestamp",
# MAGIC         "source-etl-timestamp",
# MAGIC         "sink-timestamp",
# MAGIC         "latency",
# MAGIC         "req-ack-timestamp_diff",
# MAGIC         "parsed_value"
# MAGIC       )
# MAGIC       .filter(col("parsed_value").isNotNull)
# MAGIC   }
# MAGIC
# MAGIC   // Calculate latency statistics (count, avg, percentiles) grouped by batchId
# MAGIC   def calculateLatency(outputTopicName: String): DataFrame = {
# MAGIC     val df = readAndParseKafkaOutputTopic(outputTopicName)
# MAGIC     df.groupBy(col("parsed_value.value_request.batchId"))
# MAGIC       .agg(
# MAGIC         count("*").alias("record_count"),
# MAGIC         round(avg(col("latency")), 2).alias("avg"),
# MAGIC         percentile_approx(col("latency"), lit(0.5), lit(200)).alias("P50_Latency"),
# MAGIC         percentile_approx(col("latency"), lit(0.9), lit(200)).alias("P90_Latency"),
# MAGIC         percentile_approx(col("latency"), lit(0.99), lit(200)).alias("P99_Latency")
# MAGIC       )
# MAGIC
# MAGIC   }
# MAGIC }
// Databricks notebook source
// DBTITLE 1,Define Input and Output Schema
package data_object

import java.sql.Timestamp

object LogRecordObjects {

  /**
   * Represents an input row from the log stream.
   *
   * @param id Unique identifier for the log record.
   * @param type Type of the log record (e.g., 2 for request, others for ack).
   * @param transaction_id Transaction identifier associated with the log.
   * @param timestamp Event timestamp (epoch seconds).
   * @param value Serialized value as byte array.
   */
  case class InputRow(
    id: String,
    `type`: Int,
    transaction_id: String,
    timestamp: Long,
    value: Array[Byte]
  )

  /**
   * Represents a processed log record combining request and acknowledgment.
   *
   * @param transaction_id Transaction identifier.
   * @param key_request Key of the request record.
   * @param record_timestamp_request Timestamp of the request record.
   * @param value_request Value of the request record as byte array.
   * @param key_ack Key of the acknowledgment record.
   * @param record_timestamp_ack Timestamp of the acknowledgment record.
   * @param value_ack Value of the acknowledgment record as byte array.
   */
  case class LogRecord(
    transaction_id: String,
    key_request: String,
    record_timestamp_request: Timestamp,
    value_request: Array[Byte], 
    key_ack: String,
    record_timestamp_ack: Timestamp,
    value_ack: Array[Byte]
  )
}

// COMMAND ----------

// DBTITLE 1,Define stateful application logic using an object-oriented model
package stateprocess

import data_object.LogRecordObjects._
import org.apache.spark.sql.streaming._
import java.time._
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.Encoders

/**
 * A stateful processor for handling log records.
 *
 * This processor maintains state for request and acknowledgment log records,
 * and generates output records by combining these logs based on transaction IDs.
 *
 * @param timeout_duration The duration (in min) for holding records in state before they expire.
 */
class LogRecordProcessor(timeout_duration: Long) extends StatefulProcessor[String, InputRow, LogRecord] {

  // Define multiple state variables to store keyed state within the stateful processor
  @transient protected var _ackLogRecordListState: ListState[InputRow] = _ // State to store acknowledgment log records
  @transient protected var _requestLogRecordState: ValueState[InputRow] = _ // State to store request log record

  /**
   * Initialize the stateful processor.
   * This method is called once when the processor is initialized.
   *
   * @param outputMode The output mode for the streaming query.
   * @param timeMode The time mode for the streaming query.
   */
  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    _ackLogRecordListState = getHandle.getListState(
      "ack",
      Encoders.product[InputRow],
      TTLConfig(Duration.ofMinutes(timeout_duration))
    )
    _requestLogRecordState = getHandle.getValueState[InputRow](
      "req",
      Encoders.product[InputRow],
      TTLConfig(Duration.ofMinutes(timeout_duration))
    )
  }

  /**
   * Handle input rows for a given key.
   * This method processes the input rows and generates output records.
   *
   * @param key The key associated with the input rows.
   * @param inputRows An iterator of input rows to be processed.
   * @param timerValues The current timer values.
   * @return An iterator of LogRecord, which contains the processed output records.
   */
  override def handleInputRows(
      key: String,
      inputRows: Iterator[InputRow],
      timerValues: TimerValues
  ): Iterator[LogRecord] = {

    val (request, acks) = getRequestAcksRecords(inputRows)
    val existingAcks = _ackLogRecordListState.get()

    val outputRecords = (request, acks.isEmpty && existingAcks.isEmpty) match {
      case (null, _) =>
        // Append all acknowledgments to the state if no request exists
        _ackLogRecordListState.appendList(acks)
        println(s"###### Request is empty/null for transaction id: $key ")
        Iterator.empty
      case (_, true) =>
        // Update the request state if there are no acknowledgments
        _requestLogRecordState.update(request)
        println(s"###### Ack is empty/null for transaction id: $key ")
        Iterator.empty
      case (_, false) =>
        // Handle the case where both request and acknowledgments exist
        _requestLogRecordState.update(request)
        println(s"###### request & acks available for transcation id: $key ")
        val allAcks =
          if (_ackLogRecordListState.exists()) {
            _ackLogRecordListState.clear()
            acks ++ existingAcks
          } else {
            acks
          }
        // Generate transactions from acknowledgments and the request
        val requestAckRecords = allAcks.map { ack =>
          LogRecord(
            ack.transaction_id,
            request.id,
            new java.sql.Timestamp(request.timestamp * 1000),
            request.value,
            ack.id,
            new java.sql.Timestamp(ack.timestamp * 1000),
            ack.value
          )
        }
        requestAckRecords.toIterator
    }

    outputRecords
  }

  
  /**
   * Extracts the request and acknowledgment records from the input rows.
   * This method processes the input rows to identify the request record (type == 2)
   * and collects all acknowledgment records.
   *
   * @param inputRows An iterator of input rows to be processed.
   * @return A tuple containing the request record and an array of acknowledgment records.
   */
  def getRequestAcksRecords(inputRows: Iterator[InputRow]): (InputRow, Array[InputRow]) = {
    var requestOpt: Option[InputRow] = None
    val acks = scala.collection.mutable.ArrayBuffer[InputRow]()

    // Process the iterator lazily
    inputRows.foreach { row =>
      if (row.`type` == 2 && requestOpt.isEmpty) {
        requestOpt = Some(row) // Capture the request row with type == 2
      } else {
        acks += row // Collect other ack rows into the buffer
      }
    }

    val request = requestOpt.headOption.getOrElse(_requestLogRecordState.getOption().orNull)

    // Return the single object (request) and array (acks)
    (request, acks.toArray)
  }
}

// COMMAND ----------

package com.demo.data

import data_object.LogRecordObjects._
import stateprocess._
import org.apache.spark.sql.streaming._
import java.time._
import org.apache.spark.sql.protobuf.functions._
import scala.collection.JavaConverters._
import org.apache.spark.sql._
import org.apache.spark.util.Utils
import org.apache.spark.sql.execution.streaming.RealTimeTrigger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.Encoders
  

/**
 * AdTechStreamProcessor handles the end-to-end streaming ETL pipeline for ad tech log records.
 *
 * @param kafkaConfig Kafka configuration map.
 * @param schema      Input schema for the log records.
 * @param spark       Implicit SparkSession.
 */
class AdTechStreamProcessor(
  kafkaConfig: Map[String, String],
  schema: org.apache.spark.sql.types.StructType
)(implicit val spark: SparkSession){

  import spark.implicits._

  // UDF to generate current ETL timestamp
  val etlTimestampUdf = udf(() => java.sql.Timestamp.from(java.time.Instant.now()))

  // Extract value_ack and value_request field schemas from the provided schema
  val value_ack_field: StructType = schema("value_ack").dataType.asInstanceOf[StructType]
  val value_request_field: StructType = schema("value_request").dataType.asInstanceOf[StructType]

  /**
   * Initialize Spark configuration for stateful streaming and RocksDB state store.
   */
  def initializeSparkConfig(): Unit = {
    spark.conf.set("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
    spark.conf.set("spark.sql.streaming.stateStore.checkpointFormatVersion", 2)
    spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", 10)
  }
  initializeSparkConfig()

  /**
   * Main method to process the input Kafka stream and write processed records to output Kafka topic.
   *
   * @param mode Streaming trigger mode ("trigger" or other).
   * @param stateTimeoutInmin State timeout in minutes for stateful processing.
   * @return StreamingQuery object.
   */
  def processStream(mode:String, stateTimeoutInmin:Int, inputTopicName:String, outputTopicName:String): StreamingQuery = {
    
    // Checkpoint location for streaming state
    val checkpointLocation = s"/tmp/datta_demo/dev/data/$outputTopicName/${java.util.UUID.randomUUID.toString}/"
    
    // Read from Kafka and parse key and value fields
    var sourceStream: DataFrame = spark.readStream
      .format("kafka")
      .options(kafkaConfig)
      .option("subscribe", inputTopicName)
      .option("startingOffsets", "latest")
      .option("includeHeaders", "true")
      .load()
      .withColumn("key", col("key").cast("string"))
      .withColumn("id", col("key"))
      .withColumn("transaction_id", col("key"))
      .withColumn("type", expr("CAST(filter(headers, x -> x.key = 'record_type')[0].value AS STRING)").cast("int"))
      .withColumn("timestamp", col("timestamp").cast("long"))

    // Group by transaction_id and apply stateful processing using LogRecordProcessor
    val transactionDF = sourceStream
      .as[InputRow]
      .groupByKey(_.transaction_id)
      .transformWithState(
        new LogRecordProcessor(timeout_duration = stateTimeoutInmin),
        TimeMode.ProcessingTime,
        OutputMode.Append
      )
      .toDF()

    // Select and format output columns, including parsing JSON fields and adding ETL timestamp
    var outputDf = transactionDF.select(
      $"transaction_id",
      $"key_request",
      date_format($"record_timestamp_request", "yyyy-MM-dd HH:mm:ss").alias("record_timestamp_request"),
      from_json($"value_request".cast("string"), value_request_field).alias("value_request"),
      from_json($"value_ack".cast("string"), value_ack_field).alias("value_ack"),
      $"key_ack",
      date_format($"record_timestamp_ack", "yyyy-MM-dd HH:mm:ss").alias("record_timestamp_ack")
    )
    outputDf = outputDf.withColumn("etl_timestamp", etlTimestampUdf()).withColumn("value", struct(
      col("value_request"),
      col("value_ack"),
      col("etl_timestamp"),
      col("record_timestamp_request"),
      col("record_timestamp_ack")
    ))

    // Write the processed records to the output Kafka topic
    val query = outputDf.select($"transaction_id".cast("string").alias("key"),
      to_json($"value").alias("value"))
      .writeStream
      .format("kafka")
      .options(kafkaConfig)
      .option("topic", outputTopicName)
      .option("checkpointLocation", checkpointLocation)
      .trigger(
        if (mode == "trigger") Trigger.ProcessingTime("0 minutes")
        else RealTimeTrigger.apply("5 minutes")
      )
      .outputMode("update")
      .start()

    query
  }

}
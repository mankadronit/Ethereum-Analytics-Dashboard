package edu.neu.ethanalyzer

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}

object RowWiseProducer {

  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers","localhost:9092")
    props.put("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks","all")

    val topic = "eth_transactions"
    val producer = new KafkaProducer[String, String](props)

    val spark: SparkSession = SparkSession
      .builder()
      .appName("EthereumAnalytics")
      .master("local[*]")
      .getOrCreate()

    val spc: SparkContext = spark.sparkContext
    val sc: SQLContext= spark.sqlContext
    spark.sparkContext.setLogLevel("ERROR")


    val schema = StructType(Array(
      StructField("hash", StringType, nullable = false),
      StructField("nonce", IntegerType, nullable = false),
      StructField("transaction_index", IntegerType, nullable = false),
      StructField("from_address", StringType, nullable = false),
      StructField("to_address", StringType),
      StructField("value", DoubleType),
      StructField("gas", IntegerType),
      StructField("gas_price", IntegerType),
      StructField("input", StringType),
      StructField("receipt_cumulative_gas_used", IntegerType),
      StructField("receipt_gas_used", IntegerType),
      StructField("receipt_contract_address", StringType),
      StructField("receipt_root", StringType),
      StructField("receipt_status", IntegerType),
      StructField("block_timestamp", TimestampType, nullable = false),
      StructField("block_number", IntegerType, nullable = false),
      StructField("block_hash", StringType, nullable = false)
    ))

    val data = spark.read.schema(schema).csv("data/transactions/*.csv")

    val query = data.selectExpr("CAST(hash AS STRING) AS key", "to_json(struct(*)) AS value").collect()

    var count = 0

    for (row <- query) {
      val key = row.get(0).toString

      val value = row.get(1).toString

      val record = new ProducerRecord[String, String](topic, key, value)
      val metadata = producer.send(record)

      println(value)

      count = count + 1

      if (count % 50 == 0) {println("Sleeping for 5 seconds"); Thread.sleep(5000)}
    }

  }
}

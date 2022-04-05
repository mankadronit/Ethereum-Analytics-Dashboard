package edu.neu.ethanalyzer

import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}



object TransactionsConsumer {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("EthereumAnalytics")
      .master("local[*]")
      .getOrCreate()

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

    val data = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "eth_transactions")
      .load()
      .select(col("value").cast(StringType).as("col"))
      .select(from_json(col("col"), schema).alias("transaction"))

    data.printSchema()


    val jdbcHostname = "localhost"
    val jdbcPort = 3306
    val jdbcDatabase = "crypto_db"

    // Create the JDBC URL without passing in the user and password parameters.
    val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"

    // Create a Properties() object to hold the parameters.
    import java.util.Properties
    val connectionProperties = new Properties()

    connectionProperties.put("user", "root")
    connectionProperties.put("password", "csye7200")

    val driverClass = "com.mysql.jdbc.Driver"
    connectionProperties.setProperty("Driver", driverClass)

    val query1 = data
      .select("transaction.*")
      .writeStream
      .trigger(Trigger.ProcessingTime(5000))
      .foreachBatch({ (batchDF: Dataset[Row], _: Long) =>
        batchDF.write.mode("append")
          .jdbc(jdbcUrl, "transactions", connectionProperties)
      })
      .start()

    query1.awaitTermination()
    spark.stop()
  }

}
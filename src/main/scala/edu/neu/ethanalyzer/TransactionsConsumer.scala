package edu.neu.ethanalyzer


import edu.neu.ethanalyzer.Schemas.{blocks_schema, transactions_schema}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.StringType



object TransactionsConsumer {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("EthereumAnalytics")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val trans_data = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "eth_transactions")
      .load()
      .select(col("value").cast(StringType).as("col"))
      .select(from_json(col("col"), transactions_schema).alias("transaction"))

    trans_data.printSchema()

    val blocks_data = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "eth_blocks")
      .load()
      .select(col("value").cast(StringType).as("col"))
      .select(from_json(col("col"), blocks_schema).alias("block"))

    blocks_data.printSchema()



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

    import spark.sqlContext.implicits._

    val query1 = trans_data
      .select("transaction.*")
      .writeStream
      .trigger(Trigger.ProcessingTime(2000))
      .foreachBatch({ (batchDF: Dataset[Row], _: Long) => {
        batchDF.write.mode("append")
          .jdbc(jdbcUrl, "transactions", connectionProperties)
      }})
      .start()

    val query2 = blocks_data
      .select("block.*")
      .writeStream
      .trigger(Trigger.ProcessingTime(2000))
      .foreachBatch({ (batchDF: Dataset[Row], _: Long) => {
        batchDF.write.mode("append")
          .jdbc(jdbcUrl, "blocks", connectionProperties)
      }})
      .start()

//    trans_data.select("transaction.*").createOrReplaceTempView("transactions")
//    blocks_data.select("block.*").createOrReplaceTempView("blocks")
//    val total_trans_data = trans_data
//      .select("transaction.*")
//      .withWatermark("block_timestamp", "30 minutes")
//      .groupBy(
//        window($"block_timestamp", "30 minutes", "15 minutes")
//      )
//      .sum("value")
//
//    total_trans_data.selectExpr("window.start AS block_timestamp", "`sum(value)` as total_trans")
//      .writeStream
//      .trigger(Trigger.ProcessingTime(2000))
//      .foreachBatch({ (batchDF: Dataset[Row], _: Long) => {
//        batchDF.write
//          .jdbc(jdbcUrl, "trans_total", connectionProperties)
//      }})
//      .start()
//      .awaitTermination()

//
//    trans_data
//      .groupBy("EXTRACT(YEAR FROM transaction.block_timestamp) AS year")
//      .avg("transaction.gas_price", "transaction.gas", "transaction.value", "transaction.cumulative_gas_used")
//      .write
//      .mode("append")
//      .jdbc(jdbcUrl, "trans_avg_data", connectionProperties)


//
//    val query2 = spark.sqlContext.sql(q)
//      .writeStream
//      .trigger(Trigger.ProcessingTime(2000))
//      .foreachBatch({ (batchDF: Dataset[Row], _: Long) => {
//        batchDF.write.mode("append")
//          .jdbc(jdbcUrl, "trans_blocks", connectionProperties)
//      }})
//      .start()



    query1.awaitTermination()
    query2.awaitTermination()
    spark.stop()
  }
}
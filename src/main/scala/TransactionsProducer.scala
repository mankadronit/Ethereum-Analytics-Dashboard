import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}



object TransactionsProducer {
  def main(args: Array[String]): Unit = {
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

    val data = spark.read.schema(schema).csv("./data/eth-transactions*.csv")

    val query = data.selectExpr("CAST(hash AS STRING) AS key", "to_json(struct(*)) AS value").write
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "eth_transactions")
      .option("checkpointLocation", "checkpoint")
      .save()

    spark.stop()
  }

}
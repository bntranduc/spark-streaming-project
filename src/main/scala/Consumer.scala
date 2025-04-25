import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import Config._

object Consumer {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Consumer")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Consommer Business
    consumeKafkaTopicAndWriteToDB(spark, BUSINESS_TOPIC, BUSINESS_SCHEMA, BUSINESS_TABLE)

    // Consommer Review
    // consumeKafkaTopicAndWriteToDB(spark, REVIEW_TOPIC, REVIEW_SCHEMA, REVIEW_TABLE)

    // Consommer User
    // consumeKafkaTopicAndWriteToDB(spark, USER_TOPIC, USER_SCHEMA, USER_TABLE)

    // Consommer Tip
    // consumeKafkaTopicAndWriteToDB(spark, TIP_TOPIC, TIP_SCHEMA, TIP_TABLE)


    spark.streams.awaitAnyTermination()
  }

  def consumeKafkaTopicAndWriteToDB(spark: SparkSession, topic: String, schema: StructType, tableName: String): Unit = {
    val kafkaStreamDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()

    val messages = kafkaStreamDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    val parsedMessages = messages.select(
      from_json(col("value"), schema).as("data")
    ).select("data.*")

    val query = parsedMessages.writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row], batchId: Long) =>
          batchDF.write
          .format("jdbc")
          .option("url", DB_URL)
          .option("dbtable", tableName)
          .option("user", DB_USER)
          .option("password", DB_PASSWORD)
          .option("driver", DB_DRIVER)
          .mode("append")
          .save()
        }
      .start()

    query.awaitTermination()

  }
}
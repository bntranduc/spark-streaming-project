import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import Config._

object Consumer {

  def loadJson(spark: SparkSession, filePath: String, schema: StructType): DataFrame = {
    val df = spark.read.schema(schema).json(filePath)
    df
  }

  def saveToJson(df: DataFrame, outputPath: String, overwrite: Boolean = true): Unit = {
    df.write
      .mode(if (overwrite) "overwrite" else "append")
      .json(outputPath)
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Consumer")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val users = loadJson(spark, USER_JSON_PATH, USER_SCHEMA)
    val business = loadJson(spark, BUSINESS_JSON_PATH, BUSINESS_SCHEMA)

    val usersWithNbrCount = users.withColumn("nbr_count", lit(0))
    val businessWithNbrCount = business.withColumn("nbr_count", lit(0))

    consumeKafkaTopic(spark, REVIEW_TOPIC, REVIEW_SCHEMA, REVIEW_TABLE)

    spark.streams.awaitAnyTermination()
  }

  def consumeKafkaTopic(spark: SparkSession, topic: String, schema: StructType, tableName: String): Unit = {
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

    val updatedUsers = parsedMessages
      .groupBy("user_id")
      .agg(
        count("review_id").alias("nbr_count")
      )
      .orderBy(col("nbr_count").desc)
      .limit(5)

    val updatedBusinesses = parsedMessages
      .groupBy("business_id")
      .agg(
        count("review_id").alias("nbr_count")
      )
      .orderBy(col("nbr_count").desc)
      .limit(5)

    updatedUsers.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    updatedBusinesses.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    // Démarrer les streams
    // parsedMessages.writeStream
    //  .outputMode("append")
    //  .format("console")
    //  .start()
    //  .awaitTermination()
  }
}

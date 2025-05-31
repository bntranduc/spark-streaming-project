import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import Config._
import StatsProcessor.processTopFunBusiness
import DataSourceReader.loadOrCreateArtefactSafe
import UpdateDatabse.{updateUserTable, updateBusinessTable, updateReviewTable}

object Consumer {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Consumer")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val usersDF = loadOrCreateArtefactSafe(
      spark,
      USER_JSON_PATH,
      USER_ARTEFACT_PATH,
      USER_SCHEMA,
      Seq("user_id", "name"),
      "orc"
    )

    val businessDF = loadOrCreateArtefactSafe(
      spark,
      BUSINESS_JSON_PATH,
      BUSINESS_ARTEFACT_PATH,
      BUSINESS_SCHEMA,
      Seq("business_id", "name", "city", "state", "categories"),
      "orc"
    )

    // println(usersDF.columns.mkString("Array(", ", ", ")"))
    // println(businessDF.columns.mkString("Array(", ", ", ")"))

    consumeKafkaTopic(spark, businessDF, usersDF)

    spark.streams.awaitAnyTermination()
  }


  def consumeKafkaTopic(spark: SparkSession, businessDF: DataFrame, usersDF: DataFrame): Unit = {
    val kafkaStreamDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
      .option("subscribe", REVIEW_TOPIC)
      .option("startingOffsets", "earliest")
      .load()

    val messages = kafkaStreamDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    val parsedMessages = messages.select(
      from_json(col("value"), REVIEW_SCHEMA).as("data")
    ).select("data.*")

    val dbOptions = Map(
      "url" -> DB_URL,
      "user" -> DB_USER,
      "password" -> DB_PASSWORD,
      "driver" -> DB_DRIVER
    )

    parsedMessages.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>


        val df_review_dba = spark.read
          .format("jdbc")
          .options(dbOptions + ("dbtable" -> REVIEW_TABLE))
          .load()
          .select("business_id", "useful")
        
        println("de=",df_review_dba.count())

        val df_review_db = spark.read
          .format("jdbc")
          .options(dbOptions + ("dbtable" -> REVIEW_TABLE))
          .load()
          .select("review_id")

        val new_reviews = batchDF
          .join(df_review_db, Seq("review_id"), "left_anti").distinct()
        
        println("b=", batchDF.count())

        updateUserTable(spark, new_reviews, usersDF)
        updateBusinessTable(spark, new_reviews, businessDF)
        updateReviewTable(new_reviews)
        processTopFunBusiness(spark)

        println(s"✅ Batch $batchId traité et statistiques insérées.")
      }
      .outputMode("append")
      .start()
      .awaitTermination()
  }
}

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import Config._

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
      Seq("business_id", "name", "city", "state"),
      "orc"
    )

    // println(usersDF.columns.mkString("Array(", ", ", ")"))
    // println(businessDF.columns.mkString("Array(", ", ", ")"))

    consumeKafkaTopic(spark, businessDF, usersDF)

    spark.streams.awaitAnyTermination()
  }

  def loadOrCreateArtefactSafe(
      spark: SparkSession,
      jsonPath: String,
      artefactPath: String,
      schema: StructType,
      columnsToKeep: Seq[String],
      format: String = "orc"
  ): DataFrame = {
    try {
      println(s"Tentative de chargement de l’artefact : $artefactPath...")
      val df = format.toLowerCase match {
        case "orc" => spark.read.orc(artefactPath)
        case "parquet" => spark.read.parquet(artefactPath)
        case _ => throw new IllegalArgumentException(s"Format non supporté : $format")
      }
      println("Artefact trouvé et chargé.")
      df
    } catch {
      case _: Exception =>
        println(s"Aucun artefact trouvé à $artefactPath. Création depuis le JSON...")
        val df = spark.read.schema(schema).json(jsonPath)
          .select(columnsToKeep.head, columnsToKeep.tail: _*)
  
        format.toLowerCase match {
          case "orc" => df.write.mode("overwrite").orc(artefactPath)
          case "parquet" => df.write.mode("overwrite").parquet(artefactPath)
          case _ => throw new IllegalArgumentException(s"Format non supporté : $format")
        }
        df
    }
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

        val df_users_db = spark.read
          .format("jdbc")
          .options(dbOptions + ("dbtable" -> USER_TABLE))
          .load()
          .select("user_id")

        val df_business_db = spark.read
          .format("jdbc")
          .options(dbOptions + ("dbtable" -> BUSINESS_TABLE))
          .load()
          .select("business_id")

        val df_review_db = spark.read
          .format("jdbc")
          .options(dbOptions + ("dbtable" -> REVIEW_TABLE))
          .load()
          .select("review_id")

        val new_reviews = batchDF
          .join(df_review_db, Seq("review_id"), "left_anti").distinct()

        val new_users_ids = new_reviews.select("user_id")
            .join(df_users_db, Seq("user_id"), "left_anti").distinct()

        val new_business_ids = new_reviews.select("business_id")
            .join(df_business_db, Seq("business_id"), "left_anti").distinct()

        val new_users = usersDF
          .join(new_users_ids, Seq("user_id"), "inner")

        val new_business = businessDF
          .join(new_business_ids, Seq("business_id"), "inner")
        
        new_users.write
          .format("jdbc")
          .options(dbOptions + ("dbtable" -> USER_TABLE))
          .mode("append")
          .save()

        new_business.write
          .format("jdbc")
          .options(dbOptions + ("dbtable" -> BUSINESS_TABLE))
          .mode("append")
          .save()
        
        new_reviews.write
          .format("jdbc")
          .options(dbOptions + ("dbtable" -> REVIEW_TABLE))
          .mode("append")
          .save()

        println(s"✅ Batch $batchId traité et statistiques insérées.")
      }
      .outputMode("append")
      .start()
      .awaitTermination()
  }
}

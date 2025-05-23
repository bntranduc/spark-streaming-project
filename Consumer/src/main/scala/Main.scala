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

    println(usersDF.columns.mkString("Array(", ", ", ")"))
    println(businessDF.columns.mkString("Array(", ", ", ")"))

    consumeKafkaTopic(spark, REVIEW_TOPIC, REVIEW_SCHEMA, REVIEW_TABLE)

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

    parsedMessages.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        println(s"================================ Batch $batchId reçu - taille = ${batchDF.count()} ================================")
        batchDF.write
          .format("jdbc")
          .option("url", DB_URL)
          .option("dbtable", tableName)
          .option("user", DB_USER)
          .option("password", DB_PASSWORD)
          .option("driver", DB_DRIVER)
          .mode("append")
          .save()
        batchDF.show(truncate = false)
      }
      .outputMode("append")
      .start()
      .awaitTermination()

  }
}

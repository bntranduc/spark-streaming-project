import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}
import Config._
import DataSourceReader.loadOrCreateArtefactSafe
import UpdateDatabase.updateReviewTable
import StatsProcessor.{processBusinessLocationState, processBusinessState, processRatingByOpenStatus, processReviewDistributionByUseful, processReviewEvolution, processTopCategoriesPerRating, processUsersStates, saveNoteStarsDistribution}

import scala.annotation.tailrec

object Consumer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Consumer")
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .config("spark.sql.shuffle.partitions", "100")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try {
      val usersDF = loadOrCreateArtefactSafe(
        spark,
        USER_JSON_PATH,
        USER_ARTEFACT_PATH,
        USER_SCHEMA,
        Seq("user_id", "name", "fans", "elite", "friends", "yelping_since"),
      )
      val userDF = usersDF.withColumn("yelping_since", to_timestamp(col("yelping_since"), "yyyy-MM-dd HH:mm:ss"))

      val businessDF = loadOrCreateArtefactSafe(
        spark,
        BUSINESS_JSON_PATH,
        BUSINESS_ARTEFACT_PATH,
        BUSINESS_SCHEMA,
        Seq("business_id", "name", "city", "address" ,"latitude", "longitude","state", "categories", "is_Open"),
      )

      consumeKafkaTopic(spark, businessDF, userDF)
    } catch {
      case e: java.io.FileNotFoundException =>
        println(s"Fichier introuvable : ${e.getMessage}")
        e.printStackTrace()

      case e: org.apache.spark.sql.AnalysisException =>
        println(s"Erreur d'analyse Spark SQL : ${e.getMessage}")
        e.printStackTrace()

      case e: java.text.ParseException =>
        println(s"Erreur de parsing (date ou autre) : ${e.getMessage}")
        e.printStackTrace()

      case e: IllegalArgumentException =>
        println(s"Argument invalide : ${e.getMessage}")
        e.printStackTrace()

      case e: Exception =>
        println(s"Erreur inattendue : ${e.getMessage}")
        e.printStackTrace()
    }
    spark.streams.awaitAnyTermination()
  }

  private def consumeKafkaTopic(spark: SparkSession, businessDF: DataFrame, usersDF: DataFrame): Unit = {

    val maybeKafkaDF = tryConnect(spark, attempt=1, retries=5, delaySeconds=5)

    maybeKafkaDF match {
      case Some(kafkaStreamDF) =>
          val messages = kafkaStreamDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
          
          val parsedMessages = messages.select(
            from_json(col("value"), REVIEW_SCHEMA).as("data")
          ).select("data.*")
          
          parsedMessages.writeStream
            .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

              updateReviewTable(spark, batchDF)
              processUsersStates(spark, usersDF)
              processBusinessState(spark, businessDF)

              processBusinessLocationState(spark)
              processReviewDistributionByUseful(spark)
              processRatingByOpenStatus(spark)
              processReviewEvolution(spark)
              processTopCategoriesPerRating(spark)
              saveNoteStarsDistribution(spark)
              println(s"Batch $batchId traité et statistiques insérées.")

            }
            .outputMode("append")
            .start()
            .awaitTermination()

      case None =>
        println("Le topic Kafka n’est pas disponible. Fermeture de l'application.")
        System.exit(1)
    }
  }
  
  @tailrec
  private def tryConnect(spark: SparkSession, attempt: Int, retries: Int, delaySeconds: Int): Option[DataFrame] = {
      println(s"Tentative $attempt/$retries de connexion au topic Kafka...")
      Try {
        spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
          .option("subscribe", REVIEW_TOPIC)
          .option("startingOffsets", "earliest")
          .load()
      } match {
        case Success(df) =>
          println(s"Connexion établie avec le topic Kafka '$REVIEW_TOPIC'")
          Some(df)
        case Failure(e) if attempt < retries =>
          println(s"Échec tentative $attempt : ${e.getMessage}")
          Thread.sleep(delaySeconds * 1000)
          tryConnect(spark, attempt + 1, retries, delaySeconds)
        case Failure(e) =>
          println(s"Échec après $retries tentatives. Abandon.")
          None
      }
  }
}

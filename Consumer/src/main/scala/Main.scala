import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import scala.util.{Try, Success, Failure}

import Config._
import DataSourceReader.loadOrCreateArtefactSafe
import UpdateDatabse.{updateUserTable, updateBusinessTable, updateReviewTable}
import StatsProcessor.{
  processTopFunBusiness,
  processTopUsefulUser,
  processMostFaithfulUsersPerBusiness,
  processTopRatedBusinessByCategory,
  processTopPopularBusinessByMonth,
  processTopPopularUsers,
  processApexPredatorUsers,
  processClosedBusinessRatingStats,
  processActivityEvolution,
  processEliteImpactOnRatings,
  processTopCategories
}

object Consumer {
  val dbOptions = Map(
    "url" -> DB_URL,
    "user" -> DB_USER,
    "password" -> DB_PASSWORD,
    "driver" -> DB_DRIVER
  )

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
      Seq("user_id", "name", "fans", "elite", "friends", "yelping_since"),
      "orc"
    )
    val userDF = usersDF.withColumn("yelping_since", to_timestamp(col("yelping_since"), "yyyy-MM-dd HH:mm:ss"))

    val businessDF = loadOrCreateArtefactSafe(
      spark,
      BUSINESS_JSON_PATH,
      BUSINESS_ARTEFACT_PATH,
      BUSINESS_SCHEMA,
      Seq("business_id", "name", "city", "state", "categories", "is_Open"),
      "orc"
    )

    consumeKafkaTopic(spark, businessDF, userDF)

    spark.streams.awaitAnyTermination()
  }

  def consumeKafkaTopic(spark: SparkSession, businessDF: DataFrame, usersDF: DataFrame): Unit = {

    val maybeKafkaDF = tryConnect(spark, attempt=1, retries=5, delaySeconds=5)

    maybeKafkaDF match {
      case Some(kafkaStreamDF) =>
          val messages = kafkaStreamDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
          
          val parsedMessages = messages.select(
            from_json(col("value"), REVIEW_SCHEMA).as("data")
          ).select("data.*")
          
          parsedMessages.writeStream
            .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
              
              val df_review_db = spark.read
                .format("jdbc")
                .options(dbOptions + ("dbtable" -> REVIEW_TABLE))
                .load()
                .select("review_id")

              val new_reviews = batchDF
                .join(df_review_db, Seq("review_id"), "left_anti").distinct()

              println("MAIN batchDF =", batchDF.count())
              println("MAIN review_db =", df_review_db.count())
              println("MAIN new_reviews =", new_reviews.count())

              updateReviewTable(new_reviews)
              updateUserTable(spark, usersDF)
              updateBusinessTable(spark, businessDF)

              println("MAIN Total Review = ", getAllFromTable(spark, REVIEW_TABLE).count())        
              println("MAINT Total Users = ", getAllFromTable(spark, USER_TABLE).count())        
              println("MAIN Total Business = ", getAllFromTable(spark, BUSINESS_TABLE).count())        

              processTopCategories(spark)
              // processTopFunBusiness(spark)
              // processTopUsefulUser(spark)
              // processMostFaithfulUsersPerBusiness(spark)
              // processTopRatedBusinessByCategory(spark)
              // processTopPopularBusinessByMonth(spark)
              // processTopPopularUsers(spark)
              // processApexPredatorUsers(spark)
              // processClosedBusinessRatingStats(spark)
              // processActivityEvolution(spark)
              // processEliteImpactOnRatings(spark)
              println(s"✅ Batch $batchId traité et statistiques insérées.")
            }
            .outputMode("append")
            .start()
            .awaitTermination()

      case None =>
        println("Le topic Kafka n’est pas disponible. Fermeture de l'application.")
        System.exit(1)
    }
  }
  
  def tryConnect(spark: SparkSession, attempt: Int, retries: Int, delaySeconds: Int): Option[DataFrame] = {
      println(s"⏳ Tentative $attempt/$retries de connexion au topic Kafka...")
      Try {
        spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
          .option("subscribe", REVIEW_TOPIC)
          .option("startingOffsets", "earliest")
          .load()
      } match {
        case Success(df) =>
          println(s"✅ Connexion établie avec le topic Kafka '$REVIEW_TOPIC'")
          Some(df)
        case Failure(e) if attempt < retries =>
          println(s"❌ Échec tentative $attempt : ${e.getMessage}")
          Thread.sleep(delaySeconds * 1000)
          tryConnect(spark, attempt + 1, retries, delaySeconds)
        case Failure(e) =>
          println(s"⛔ Échec après $retries tentatives. Abandon.")
          None
      }
    }

    def getAllFromTable(spark: SparkSession, table: String): DataFrame = {
        return spark.read
          .format("jdbc")
          .options(dbOptions + ("dbtable" -> table))
          .load()
    }

}
